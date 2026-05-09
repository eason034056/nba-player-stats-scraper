#!/usr/bin/env python3
"""
WNBA Player Game Logs Scraper
=============================
從 RealGM 爬取 WNBA 球員 2025 + 2026 兩個賽季的 game log。

設計重點（與 scrape_nba.py 的差異）：
- URL：球員列表用 /wnba/players/2026
- Season 格式：單一年 "2025"/"2026"，而非 NBA 的跨年 "2025-2026"
- 同一位球員一次抓多個 season（策略 A：每位球員內層切 season）
- 增量判斷以 (player, season) 為 key，已完成賽季抓過就跳過
"""

# ============================================================
# 步驟 1: 匯入套件（與 NBA 版相同）
# ============================================================
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd
import time
import logging
import os
import sys
from datetime import datetime

# ============================================================
# 步驟 2: 日誌（同 NBA 版，只改檔名）
# ============================================================
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scrape_wnba_log.txt', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# 步驟 3: 設定常數
# ============================================================
CSV_FILENAME = "wnba_player_game_logs.csv"

# 💡 要爬的賽季列表，未來只要在這裡 append 新賽季就行
SEASONS = ["2025", "2026"]

# 💡 「進行中」的賽季 — 只有它走增量邏輯，其他賽季一旦抓過就跳過
#    每年球季開始前更新這一行即可（例：2027 年改成 "2027"）
CURRENT_SEASON = "2026"

# WNBA 球員列表頁面（指定年份的在隊名單）
PLAYER_LIST_URL = "https://basketball.realgm.com/wnba/players/2026"

# 欄位結構與 NBA 完全相同（你的截圖確認過了）
COLUMNS = [
    "Player", "Season", "Date", "Team", "Opponent", "W/L", "Status",
    "Pos", "MIN", "PTS", "FGM", "FGA", "FG%", "3PM", "3PA", "3P%",
    "FTM", "FTA", "FT%", "ORB", "DRB", "REB", "AST", "STL", "BLK",
    "TOV", "PF", "FIC"
]


def load_existing_data():
    """
    載入現有 CSV，並回傳 (DataFrame, 每位球員每個賽季的最後日期)

    Returns:
        tuple:
        - df: pandas DataFrame
        - season_last_dates: dict，{(player, season): datetime}
                             ⚠️ 與 NBA 版差異：key 從 player 變成 (player, season) tuple
    """
    season_last_dates = {}

    if os.path.exists(CSV_FILENAME):
        try:
            df = pd.read_csv(CSV_FILENAME, encoding='utf-8-sig')
            # 把 Season 統一成字串，避免 pandas 有時把 "2026" 讀成 int
            df['Season'] = df['Season'].astype(str)
            logger.info(f"已載入現有資料：{len(df)} 筆記錄")

            # 💡 同時用 Player + Season 兩個欄位 groupby
            #    每組 = 同一球員、同一賽季的所有比賽
            for (player, season), group in df.groupby(['Player', 'Season']):
                latest_date = None
                for date_str in group['Date'].tolist():
                    try:
                        parsed_date = pd.to_datetime(date_str, format='%m/%d/%Y')
                        if latest_date is None or parsed_date > latest_date:
                            latest_date = parsed_date
                    except Exception:
                        continue

                if latest_date:
                    season_last_dates[(player, season)] = latest_date

            logger.info(f"找到 {len(season_last_dates)} 個 (球員, 賽季) 組合的歷史資料")
            return df, season_last_dates

        except Exception as e:
            logger.warning(f"無法讀取現有 CSV：{e}")
            return pd.DataFrame(columns=COLUMNS), {}
    else:
        logger.info("沒有找到現有 CSV，將進行完整爬取")
        return pd.DataFrame(columns=COLUMNS), {}


def setup_chrome_driver():
    """
    啟動 Chrome（headless + 反偵測）— 與 NBA 版完全相同
    """
    logger.info("正在設定 Chrome 瀏覽器...")
    options = webdriver.ChromeOptions()

    # 加速設定
    options.page_load_strategy = "eager"
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--blink-settings=imagesEnabled=false")

    # Headless / CI 穩定性
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--single-process")
    options.add_argument("--disable-ipc-flooding-protection")

    # 反偵測
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setBlockedURLs", {
            "urls": ["*.png", "*.jpg", "*.jpeg", "*.gif", "*.svg", "*.webp"]
        })
    except Exception:
        logger.info("無法設定圖片封鎖，繼續執行")

    driver.set_page_load_timeout(60)
    driver.implicitly_wait(2)
    driver.set_script_timeout(30)

    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        '''
    })

    wait = WebDriverWait(driver, 30)
    logger.info("Chrome 瀏覽器已啟動")
    return driver, wait


def get_player_links(driver, wait, max_retries=3):
    """
    取得所有 WNBA 球員的連結（含重試機制）

    ⚠️ 與 NBA 版差異：URL 改成 /wnba/players/2026
       /wnba/players/ 路徑必須帶年份，否則 RealGM 會 404 或重導
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"正在載入 WNBA 球員列表...（嘗試 {attempt + 1}/{max_retries}）")

            if attempt > 0:
                wait_time = 5 * attempt
                logger.info(f"等待 {wait_time} 秒後重試...")
                time.sleep(wait_time)

            driver.get(PLAYER_LIST_URL)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
            wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "table tbody tr")) > 0)

            player_elements = driver.find_elements(
                By.CSS_SELECTOR, "table tbody tr td:nth-child(2) a"
            )
            if not player_elements:
                raise Exception("未找到任何球員元素")

            players = []
            for elem in player_elements:
                try:
                    name = elem.text.strip()
                    href = elem.get_attribute("href")
                    if name and href:
                        players.append((name, href))
                except Exception:
                    continue

            if not players:
                raise Exception("無法提取球員資料")

            logger.info(f"成功找到 {len(players)} 位 WNBA 球員")
            return players

        except Exception as e:
            error_msg = str(e)[:100]
            logger.warning(f"載入球員列表失敗（嘗試 {attempt + 1}）：{error_msg}")
            if attempt == max_retries - 1:
                logger.error(f"載入球員列表失敗，已重試 {max_retries} 次")
                raise
            try:
                driver.refresh()
            except Exception:
                pass

    return []


def should_scrape_player_season(player_name, season, season_last_dates):
    """
    判斷某 (球員, 賽季) 組合是否需要爬取

    💡 核心邏輯：
    1. (player, season) 沒有歷史資料 → 抓
    2. season 不是當前賽季（已完結賽季）+ 已有資料 → 跳過（永遠不會有新資料）
    3. season 是當前賽季 + 今天已抓過 → 跳過
    4. 其他情況 → 抓增量

    Args:
        player_name: str
        season: str，賽季年份（如 "2025"）
        season_last_dates: dict，{(player, season): datetime}

    Returns:
        tuple: (should_scrape: bool, reason: str)
    """
    key = (player_name, season)
    today = datetime.now()

    # 沒有歷史資料 → 抓
    if key not in season_last_dates:
        return True, f"{season} 新資料"

    # 已完結賽季 + 已有資料 → 跳過
    if season != CURRENT_SEASON:
        return False, f"{season} 賽季已完整爬取"

    # 當前賽季 → 走增量邏輯
    last_date = season_last_dates[key]
    days_since_last = (today - last_date).days

    if days_since_last == 0:
        return False, f"{season} 今天已有資料"

    return True, f"{season} 距上次 {days_since_last} 天"


def get_player_name_from_page(driver):
    """從頁面 h2 取得球員姓名（與 NBA 版相同）"""
    try:
        h2_element = driver.find_element(By.CSS_SELECTOR, "div.half-column-left h2")
        player_name_raw = driver.execute_script("""
            var h2 = arguments[0];
            var text = '';
            for (var i = 0; i < h2.childNodes.length; i++) {
                if (h2.childNodes[i].nodeType === Node.TEXT_NODE) {
                    text += h2.childNodes[i].nodeValue;
                }
            }
            return text.trim();
        """, h2_element)
        return player_name_raw.strip()
    except Exception:
        return None


def select_dropdown_option(select_element, options_to_try, fallback_index=0):
    """嘗試選擇 dropdown 選項（同 NBA 版）"""
    for option_text in options_to_try:
        try:
            select_element.select_by_visible_text(option_text)
            return True
        except Exception:
            continue
    try:
        select_element.select_by_index(fallback_index)
        return True
    except Exception:
        return False


def select_option_if_needed(select_element, options_to_try, fallback_index=0):
    """目前選項已經是目標就不重選（避免不必要的 reload）"""
    try:
        current_text = select_element.first_selected_option.text.strip()
    except Exception:
        current_text = ""

    for option_text in options_to_try:
        if current_text == option_text:
            return False

    return select_dropdown_option(select_element, options_to_try, fallback_index)


def wait_for_table_refresh(driver, wait):
    """等待 dropdown 變更後表格重新載入（同 NBA 版）"""
    try:
        old_table = driver.find_element(By.CSS_SELECTOR, "table")
        try:
            wait.until(EC.staleness_of(old_table))
        except Exception:
            pass
    except Exception:
        pass

    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody")))


def parse_game_date(date_str):
    """解析 RealGM 的日期字串（如 "5/1/2026"）"""
    try:
        return pd.to_datetime(date_str, format='%m/%d/%Y')
    except Exception:
        return None


def read_table_rows(driver, player_name, season, last_date=None):
    """
    讀取目前頁面 table 的所有列，套用增量過濾

    💡 把 NBA 版的 row-parsing 邏輯獨立出來，因為 WNBA 一個球員會跑多次（每季一次）
    """
    new_games = []
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    first_date = None
    is_descending = None

    for row in rows:
        try:
            cells = row.find_elements(By.TAG_NAME, "td")
            cell_data = [cell.text.strip() for cell in cells]

            if not cell_data or len(cell_data) < 3:
                continue

            # 增量過濾：只在 last_date 存在時啟用
            game_date = parse_game_date(cell_data[0]) if last_date else None

            if last_date and game_date:
                if first_date is None:
                    first_date = game_date
                elif is_descending is None:
                    is_descending = first_date >= game_date

                if game_date <= last_date:
                    if is_descending:
                        break  # 表格降序，後面都更舊，可早退
                    continue

            # 補上 Player + Season 兩欄到最前面
            cell_data_with_info = [player_name, season] + cell_data
            new_games.append(cell_data_with_info)

        except Exception:
            continue

    return new_games


def scrape_player_seasons(driver, wait, player_name, game_log_url, season_last_dates):
    """
    對單一球員爬取所有目標賽季的 game log（策略 A：內層切 season）

    💡 流程：
    1. 開啟球員 game log 頁（只開一次）
    2. League 確保 = WNBA
    3. 對每個 SEASONS 中的賽季：
       a. 確認 dropdown 有這個 season（沒打過就跳過）
       b. 切換 season → 等表格刷新
       c. 確保 Games = All Games
       d. 讀取表格、套增量過濾
    4. 回傳所有賽季合併的 rows

    Returns:
        list: 所有新增的 game log rows
    """
    all_new_games = []
    short_wait = WebDriverWait(driver, 20)

    try:
        try:
            driver.get(game_log_url)
        except Exception as e:
            if "timeout" in str(e).lower():
                logger.warning(f"頁面載入超時，嘗試繼續...")
            else:
                raise

        short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))

        # 從頁面校正球員姓名（更準確）
        page_player_name = get_player_name_from_page(driver)
        if page_player_name:
            player_name = page_player_name

        # 第一步：確認 League = WNBA
        # ⚠️ 與 NBA 版差異：這裡選 "WNBA" 而非 "NBA"
        selects = driver.find_elements(By.TAG_NAME, "select")
        if len(selects) >= 1:
            league_select = Select(selects[0])
            if select_option_if_needed(league_select, ["WNBA"]):
                wait_for_table_refresh(driver, short_wait)

        # 第二步：對每個 season 執行
        for season in SEASONS:
            should_scrape, reason = should_scrape_player_season(
                player_name, season, season_last_dates
            )
            if not should_scrape:
                logger.info(f"  - {reason}，跳過")
                continue

            # 重新抓 selects（DOM 可能因前次切換而變動）
            selects = driver.find_elements(By.TAG_NAME, "select")
            if len(selects) < 2:
                logger.warning(f"  - 找不到 season dropdown，跳過 {season}")
                continue

            season_select = Select(selects[1])
            available = [opt.text.strip() for opt in season_select.options]

            # ⚠️ 重要：season dropdown 只列該球員打過的賽季
            #    如果 "2025" 不在裡面 → 該球員 2025 沒上場
            if season not in available:
                logger.info(f"  - {season} 該球員無資料")
                continue

            # 切換到目標 season
            if select_option_if_needed(season_select, [season]):
                wait_for_table_refresh(driver, short_wait)

            # 第三步：確保 Games = All Games（與 NBA 版邏輯一致）
            selects = driver.find_elements(By.TAG_NAME, "select")
            if len(selects) >= 3:
                games_select = Select(selects[2])
                available_games = [opt.text.strip() for opt in games_select.options]
                preferred = ["All Games", "Regular Season", "Playoffs", "Preseason"]
                target = next(
                    (opt for opt in preferred if opt in available_games),
                    available_games[0] if available_games else None,
                )
                if target:
                    try:
                        current = games_select.first_selected_option.text.strip()
                    except Exception:
                        current = ""
                    if current != target:
                        if select_dropdown_option(games_select, [target]):
                            wait_for_table_refresh(driver, short_wait)

            short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody")))

            # 第四步：讀表
            last_date = season_last_dates.get((player_name, season))
            season_rows = read_table_rows(driver, player_name, season, last_date)
            if season_rows:
                all_new_games.extend(season_rows)
                logger.info(f"  + {season}: {len(season_rows)} 場")
            else:
                logger.info(f"  + {season}: 無新比賽")

        return all_new_games

    except Exception as e:
        logger.warning(f"爬取 {player_name} 時發生錯誤：{str(e)[:100]}")
        return all_new_games  # 已有資料先回傳，不要整批丟掉


def scrape_all_players(driver, wait, players, season_last_dates):
    """
    對所有球員執行多賽季爬取（含瀏覽器重啟機制）
    結構與 NBA 版一致，差別在於把 scrape_player_games 換成 scrape_player_seasons
    """
    all_new_games = []
    stats = {
        'total': len(players),
        'scraped': 0,
        'skipped_all': 0,   # 兩個賽季都不需爬
        'new_games': 0,
        'errors': 0,
        'browser_restarts': 0
    }

    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5

    for idx, (player_name, link) in enumerate(players, start=1):
        # 提前判斷：如果所有 SEASONS 都不需爬，整個球員跳過（連頁面都不開，省最多時間）
        all_seasons_skip = all(
            not should_scrape_player_season(player_name, s, season_last_dates)[0]
            for s in SEASONS
        )
        if all_seasons_skip:
            stats['skipped_all'] += 1
            if idx % 50 == 0:
                logger.info(f"進度：{idx}/{len(players)} - 跳過多位無需更新的球員")
            continue

        game_log_url = link.replace("/Summary/", "/GameLogs/")
        logger.info(f"[{idx}/{len(players)}] {player_name}")

        max_retries = 3
        success = False

        for retry in range(max_retries):
            try:
                new_games = scrape_player_seasons(
                    driver, wait, player_name, game_log_url, season_last_dates
                )
                if new_games:
                    all_new_games.extend(new_games)
                    stats['new_games'] += len(new_games)
                stats['scraped'] += 1
                success = True
                consecutive_errors = 0
                break
            except Exception as e:
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 3
                    logger.warning(f"  ⟳ 重試 {retry + 1}/{max_retries}，等待 {wait_time} 秒...")
                    time.sleep(wait_time)
                else:
                    stats['errors'] += 1
                    consecutive_errors += 1
                    logger.warning(f"  ✗ 錯誤：{str(e)[:80]}")

        # 連續錯誤過多 → 重啟瀏覽器
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            logger.warning(f"連續 {consecutive_errors} 次錯誤，重啟瀏覽器...")
            try:
                driver.quit()
            except Exception:
                pass
            time.sleep(5)
            try:
                driver, wait = setup_chrome_driver()
                stats['browser_restarts'] += 1
                consecutive_errors = 0
                logger.info("瀏覽器已重啟")
            except Exception as e:
                logger.error(f"無法重啟瀏覽器：{e}")
                return all_new_games, stats, None, None

        time.sleep(0.8 if success else 2)

    return all_new_games, stats, driver, wait


def save_data(existing_df, new_games):
    """
    合併並儲存資料（dedup key 多加 Season 欄位以避免跨季衝突）
    """
    if not new_games:
        logger.info("沒有新資料需要儲存")
        return existing_df

    new_df = pd.DataFrame(new_games, columns=COLUMNS)
    logger.info(f"新增 {len(new_df)} 筆記錄")

    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    # ⚠️ 與 NBA 版差異：dedup key 加入 Season
    combined_df = combined_df.drop_duplicates(
        subset=['Player', 'Season', 'Date', 'Team', 'Opponent'],
        keep='last'
    )

    combined_df['_date_sort'] = pd.to_datetime(
        combined_df['Date'], format='%m/%d/%Y', errors='coerce'
    )
    # 排序：先按球員，再按賽季降序，再按日期降序
    combined_df = combined_df.sort_values(
        ['Player', 'Season', '_date_sort'],
        ascending=[True, False, False]
    )
    combined_df = combined_df.drop('_date_sort', axis=1)

    combined_df.to_csv(CSV_FILENAME, index=False, encoding='utf-8-sig')
    logger.info(f"資料已儲存至 {CSV_FILENAME}")
    logger.info(f"總共 {len(combined_df)} 筆記錄")

    return combined_df


def main():
    """主程式（流程與 NBA 版一致，只差呼叫的函數名）"""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"WNBA Game Log 爬蟲開始執行（增量模式）")
    logger.info(f"目標賽季：{SEASONS}（當前進行中：{CURRENT_SEASON}）")
    logger.info(f"開始時間：{start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    driver = None
    exit_code = 0

    try:
        existing_df, season_last_dates = load_existing_data()

        max_browser_retries = 3
        for browser_attempt in range(max_browser_retries):
            try:
                driver, wait = setup_chrome_driver()
                break
            except Exception as e:
                logger.warning(f"啟動瀏覽器失敗（嘗試 {browser_attempt + 1}/{max_browser_retries}）：{e}")
                if browser_attempt == max_browser_retries - 1:
                    raise
                time.sleep(5)

        players = get_player_links(driver, wait)
        if not players:
            logger.error("未找到任何球員！")
            return 1

        new_games, stats, driver, wait = scrape_all_players(
            driver, wait, players, season_last_dates
        )

        if new_games:
            save_data(existing_df, new_games)
        else:
            logger.info("沒有新資料需要儲存")

        logger.info("=" * 60)
        logger.info("爬蟲統計資訊：")
        logger.info("=" * 60)
        logger.info(f"總球員數：{stats['total']}")
        logger.info(f"已爬取：{stats['scraped']} 位球員")
        logger.info(f"跳過（兩季都無需更新）：{stats['skipped_all']} 位球員")
        logger.info(f"新增比賽：{stats['new_games']} 場")
        logger.info(f"錯誤：{stats['errors']} 次")

        if stats.get('browser_restarts', 0) > 0:
            logger.info(f"瀏覽器重啟：{stats['browser_restarts']} 次")

        total_attempted = stats['scraped'] + stats['errors']
        if total_attempted > 0 and stats['errors'] / total_attempted > 0.1:
            logger.warning(f"錯誤率超過 10%（{stats['errors']}/{total_attempted}）")
            exit_code = 1

    except Exception as e:
        import traceback
        logger.error(f"程式執行錯誤：{e}")
        logger.error(f"錯誤詳情：\n{traceback.format_exc()}")
        exit_code = 1

    finally:
        if driver:
            try:
                driver.quit()
                logger.info("瀏覽器已關閉")
            except Exception as e:
                logger.warning(f"關閉瀏覽器時發生錯誤：{e}")

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"執行時間：{duration}")
        logger.info("=" * 60)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
