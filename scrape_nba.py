#!/usr/bin/env python3
"""
NBA Player Game Logs Scraper
============================
這個腳本會從 RealGM 網站爬取 NBA 球員的比賽記錄，並儲存為 CSV 檔案。
專門設計用於 GitHub Actions Cron Job 自動執行。

功能說明：
- 自動爬取所有 NBA 球員的 Game Log 資料
- 選擇指定賽季（目前是 2025-2026）
- 儲存為 CSV 格式
- 完整的日誌記錄和錯誤處理
"""

# ============================================================
# 步驟 1: 匯入所需的套件
# ============================================================

# selenium 用於自動化網頁瀏覽器操作
from selenium import webdriver
# By 用於定位網頁元素的方式（如 CSS_SELECTOR, TAG_NAME 等）
from selenium.webdriver.common.by import By
# WebDriverWait 用於等待特定條件達成（顯式等待）
from selenium.webdriver.support.ui import WebDriverWait
# expected_conditions (EC) 提供常用的等待條件
from selenium.webdriver.support import expected_conditions as EC
# Select 用於操作下拉選單（<select> 元素）
from selenium.webdriver.support.ui import Select
# Service 用於設定 ChromeDriver 的服務
from selenium.webdriver.chrome.service import Service
# webdriver_manager 自動下載和管理 ChromeDriver 版本
# 這樣就不需要手動下載 ChromeDriver，會自動配對 Chrome 版本
from webdriver_manager.chrome import ChromeDriverManager

# pandas 用於資料處理和儲存成 CSV 格式
import pandas as pd
# time 用於控制程式執行速度（避免對伺服器造成負擔）
import time
# logging 用於記錄程式執行過程（比 print 更專業）
import logging
# os 用於處理檔案路徑和環境變數
import os
# sys 用於程式退出和系統相關操作
import sys
# datetime 用於處理日期時間
from datetime import datetime

# ============================================================
# 步驟 2: 設定日誌系統
# ============================================================
# logging.basicConfig() 設定日誌的基本配置
# format: 日誌格式，包含時間、等級、訊息
# level: 日誌等級，INFO 表示記錄一般資訊以上的訊息
# handlers: 日誌處理器，同時輸出到檔案和終端機
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',  # 格式：時間 - 等級 - 訊息
    level=logging.INFO,  # 等級：INFO（一般資訊）
    handlers=[
        logging.StreamHandler(sys.stdout),  # StreamHandler 輸出到終端機（stdout）
        logging.FileHandler('scrape_log.txt', encoding='utf-8')  # FileHandler 輸出到檔案
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# 步驟 3: CSV 檔案設定
# ============================================================
CSV_FILENAME = "nba_player_game_logs.csv"
SEASON = "2025-2026"

# CSV 欄位名稱（用於建立 DataFrame）
COLUMNS = [
    "Player", "Season", "Date", "Team", "Opponent", "W/L", "Status",
    "Pos", "MIN", "PTS", "FGM", "FGA", "FG%", "3PM", "3PA", "3P%",
    "FTM", "FTA", "FT%", "ORB", "DRB", "REB", "AST", "STL", "BLK",
    "TOV", "PF", "FIC"
]


def load_existing_data():
    """
    載入現有的 CSV 資料
    
    Returns:
        tuple: (df, player_last_dates)
        - df: pandas DataFrame，現有資料（如果沒有檔案則為空 DataFrame）
        - player_last_dates: dict，每位球員最後一場比賽的日期
          格式：{"LeBron James": datetime(2025, 1, 28), ...}
    
    這個函數會：
    1. 嘗試讀取現有的 CSV 檔案
    2. 解析每位球員最新的比賽日期
    3. 返回資料供後續增量爬取使用
    """
    player_last_dates = {}
    
    # os.path.exists() 檢查檔案是否存在
    if os.path.exists(CSV_FILENAME):
        try:
            # pd.read_csv() 讀取 CSV 檔案
            df = pd.read_csv(CSV_FILENAME, encoding='utf-8-sig')
            logger.info(f"已載入現有資料：{len(df)} 筆記錄")
            
            # 解析每位球員的最新比賽日期
            # groupby('Player') 按球員分組
            for player, group in df.groupby('Player'):
                # 取得該球員所有比賽日期
                dates = group['Date'].tolist()
                
                # 嘗試解析日期，找出最新的
                latest_date = None
                for date_str in dates:
                    try:
                        # pd.to_datetime() 將字串轉換為日期
                        # format='%m/%d/%Y' 指定日期格式（如 "1/25/2026"）
                        parsed_date = pd.to_datetime(date_str, format='%m/%d/%Y')
                        if latest_date is None or parsed_date > latest_date:
                            latest_date = parsed_date
                    except Exception:
                        continue
                
                if latest_date:
                    # 儲存每位球員的最新比賽日期
                    player_last_dates[player] = latest_date
            
            logger.info(f"找到 {len(player_last_dates)} 位球員的歷史資料")
            return df, player_last_dates
            
        except Exception as e:
            logger.warning(f"無法讀取現有 CSV 檔案：{e}")
            # 返回空的 DataFrame
            return pd.DataFrame(columns=COLUMNS), {}
    else:
        logger.info("沒有找到現有的 CSV 檔案，將進行完整爬取")
        return pd.DataFrame(columns=COLUMNS), {}


def setup_chrome_driver():
    """
    設定並啟動 Chrome 瀏覽器（Headless 模式）
    
    Returns:
        tuple: (driver, wait) - WebDriver 物件和 WebDriverWait 物件
        
    這個函數做了以下事情：
    1. 設定 Chrome 的各種選項（headless、反偵測等）
    2. 使用 webdriver_manager 自動管理 ChromeDriver
    3. 返回可用的 driver 和 wait 物件
    """
    logger.info("正在設定 Chrome 瀏覽器...")
    
    # webdriver.ChromeOptions() 創建一個 Chrome 瀏覽器的配置對象
    options = webdriver.ChromeOptions()
    
    # ============================================================
    # 【重要】Headless 模式設定 - GitHub Actions 必須啟用
    # ============================================================
    # add_argument("--headless") 讓瀏覽器在背景執行，不會開啟視窗
    # 這對於伺服器環境（如 GitHub Actions）是必要的，因為沒有 GUI
    options.add_argument("--headless")
    
    # add_argument("--no-sandbox") 關閉沙盒模式
    # 在 Docker 或 CI/CD 環境中，沙盒模式可能會造成權限問題
    # 沙盒是一種安全機制，限制瀏覽器對系統的存取
    options.add_argument("--no-sandbox")
    
    # add_argument("--disable-dev-shm-usage") 解決資源限制問題
    # /dev/shm 是 Linux 的共享記憶體，預設只有 64MB
    # 這個選項讓 Chrome 使用 /tmp 而不是 /dev/shm
    options.add_argument("--disable-dev-shm-usage")
    
    # add_argument("--disable-gpu") 禁用 GPU 加速
    # 在 headless 模式和伺服器環境中，GPU 通常不可用
    options.add_argument("--disable-gpu")
    
    # add_argument("--remote-debugging-port=9222") 設定遠端除錯端口
    # 這可以幫助解決某些 headless 模式的問題
    options.add_argument("--remote-debugging-port=9222")
    
    # ============================================================
    # 反偵測設定 - 讓瀏覽器看起來像真人操作
    # ============================================================
    
    # 設定視窗大小，模擬真實瀏覽器的解析度
    # 如果不設定，headless 模式的視窗可能是 800x600（看起來很可疑）
    options.add_argument("--window-size=1920,1080")
    
    # 設定 User-Agent，讓網站認為這是正常的 Chrome 瀏覽器
    # User-Agent 是瀏覽器向伺服器發送的身份識別字串
    # 預設的 headless User-Agent 會包含 "HeadlessChrome" 字樣
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    # 禁用 Blink 引擎的自動化控制特徵
    # Blink 是 Chrome 的渲染引擎
    # AutomationControlled 是一個會暴露 Selenium 的特徵
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # excludeSwitches 排除某些 Chrome 啟動參數
    # "enable-automation" 會在視窗顯示「Chrome 正由自動化測試軟體控制」
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    # useAutomationExtension 禁用自動化擴展
    # 這個擴展是 Selenium 用來控制瀏覽器的，但會被某些網站偵測
    options.add_experimental_option('useAutomationExtension', False)
    
    # ============================================================
    # 啟動 Chrome 瀏覽器
    # ============================================================
    # ChromeDriverManager().install() 自動下載適合的 ChromeDriver
    # 這比手動下載更方便，且會自動匹配 Chrome 版本
    # Service() 用於設定 ChromeDriver 的服務
    service = Service(ChromeDriverManager().install())
    
    # webdriver.Chrome() 使用上述配置啟動 Chrome 瀏覽器
    driver = webdriver.Chrome(service=service, options=options)
    
    # execute_cdp_cmd() 執行 Chrome DevTools Protocol 命令
    # 這裡用來修改 navigator.webdriver 屬性
    # 正常情況下，Selenium 會設定 navigator.webdriver = true
    # 這段 JavaScript 將它改為 undefined，就像真實瀏覽器一樣
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        '''
    })
    
    # WebDriverWait(driver, 45) 創建一個等待物件
    # 45 是最長等待秒數
    # 當需要等待網頁元素載入時使用
    wait = WebDriverWait(driver, 45)
    
    logger.info("Chrome 瀏覽器已啟動（Headless 模式 + 反偵測配置）")
    return driver, wait


def get_player_links(driver, wait):
    """
    取得所有 NBA 球員的連結
    
    Args:
        driver: Selenium WebDriver 物件
        wait: WebDriverWait 物件
        
    Returns:
        list: 所有球員頁面的 URL 列表
        
    這個函數做了以下事情：
    1. 訪問 RealGM 的球員列表頁面
    2. 等待頁面載入完成
    3. 提取所有球員的連結
    """
    logger.info("正在載入球員列表頁面...")
    
    # driver.get() 讓瀏覽器訪問指定的網址
    driver.get("https://basketball.realgm.com/nba/players")
    
    # wait.until() 等待直到指定條件達成
    # EC.presence_of_element_located() 檢查元素是否出現在 DOM 中
    # (By.CSS_SELECTOR, "table") 使用 CSS 選擇器尋找 <table> 元素
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
    
    # 取得球員連結和姓名
    player_elements = driver.find_elements(By.CSS_SELECTOR, "table tbody tr td:nth-child(2) a")
    
    # 同時取得球員姓名和連結
    # 這樣可以在之後快速判斷是否需要爬取
    players = []
    for elem in player_elements:
        name = elem.text.strip()
        url = elem.get_attribute("href")
        players.append((name, url))
    
    logger.info(f"找到 {len(players)} 位球員")
    return players


def should_scrape_player(player_name, player_last_dates):
    """
    判斷是否需要爬取該球員的資料
    
    Args:
        player_name: str，球員姓名
        player_last_dates: dict，每位球員最後一場比賽的日期
        
    Returns:
        tuple: (should_scrape, reason)
        - should_scrape: bool，是否需要爬取
        - reason: str，原因說明
    
    判斷邏輯：
    1. 如果是新球員（沒有歷史資料）→ 需要完整爬取
    2. 如果最後一場比賽是今天或昨天 → 可能不需要爬取
    3. 如果最後一場比賽超過 1 天 → 需要爬取新資料
    """
    today = datetime.now()
    
    # 如果是新球員，需要完整爬取
    if player_name not in player_last_dates:
        return True, "新球員"
    
    last_date = player_last_dates[player_name]
    days_since_last = (today - last_date).days
    
    # 如果最後一場比賽是今天 → 不需要爬取
    if days_since_last == 0:
        return False, "今天已有資料"
    
    # 如果超過 1 天沒有新資料 → 需要爬取
    # （因為 NBA 比賽通常每 1-2 天就有）
    return True, f"距上次 {days_since_last} 天"


def get_player_name_from_page(driver):
    """從頁面取得球員姓名"""
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
    """嘗試選擇下拉選單的選項"""
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


def parse_game_date(date_str):
    """
    解析比賽日期字串
    
    Args:
        date_str: str，日期字串（如 "1/25/2026"）
        
    Returns:
        datetime 或 None
    """
    try:
        return pd.to_datetime(date_str, format='%m/%d/%Y')
    except Exception:
        return None


def scrape_player_games(driver, wait, player_name, game_log_url, last_date=None):
    """
    爬取單一球員的比賽資料
    
    Args:
        driver: Selenium WebDriver
        wait: WebDriverWait
        player_name: str，球員姓名
        game_log_url: str，Game Log 頁面 URL
        last_date: datetime 或 None，該球員最後一場比賽的日期
                   如果提供，只會爬取比這個日期更新的比賽
    
    Returns:
        list: 新的比賽記錄列表
    
    【增量爬取的核心邏輯】
    當 last_date 不為 None 時，只爬取日期 > last_date 的比賽
    這樣可以大幅減少需要處理的資料量
    """
    new_games = []
    
    try:
        driver.get(game_log_url)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
        
        # 從頁面取得球員姓名（更準確）
        page_player_name = get_player_name_from_page(driver)
        if page_player_name:
            player_name = page_player_name
        
        # 設定下拉選單
        selects = driver.find_elements(By.TAG_NAME, "select")
        
        if len(selects) >= 3:
            # League
            league_select = Select(selects[0])
            select_dropdown_option(league_select, ["NBA"])
            time.sleep(1)  # 【優化】減少等待時間
            
            # Season
            season_select = Select(selects[1])
            season_variations = [SEASON, SEASON.replace("-20", "-")]
            select_dropdown_option(season_select, season_variations)
            time.sleep(1)
            
            # Games
            games_select = Select(selects[2])
            select_dropdown_option(games_select, ["All Games", "Regular Season"])
            time.sleep(1.5)
            
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody")))
            time.sleep(0.5)
        
        # 抓取表格資料
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        
        for row_idx in range(len(rows)):
            try:
                rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                if row_idx >= len(rows):
                    break
                
                row = rows[row_idx]
                cells = row.find_elements(By.TAG_NAME, "td")
                cell_data = [cell.text.strip() for cell in cells]
                
                if not cell_data or len(cell_data) < 3:
                    continue
                
                # 【增量爬取關鍵】檢查日期
                # cell_data[0] 是日期欄位
                game_date = parse_game_date(cell_data[0])
                
                if last_date and game_date:
                    # 如果這場比賽的日期 <= 最後記錄的日期，跳過
                    if game_date <= last_date:
                        continue
                
                # 加上球員姓名和賽季
                cell_data_with_info = [player_name, SEASON] + cell_data
                new_games.append(cell_data_with_info)
                
            except Exception:
                continue
        
        return new_games
        
    except Exception as e:
        logger.warning(f"爬取 {player_name} 時發生錯誤：{str(e)[:100]}")
        return []


def scrape_all_players(driver, wait, players, player_last_dates):
    """
    爬取所有球員的比賽資料（增量模式）
    
    Args:
        driver: Selenium WebDriver
        wait: WebDriverWait
        players: list，(player_name, url) 的 tuple 列表
        player_last_dates: dict，每位球員最後一場比賽的日期
        
    Returns:
        tuple: (all_new_games, stats)
        - all_new_games: list，所有新的比賽記錄
        - stats: dict，統計資訊
    """
    all_new_games = []
    stats = {
        'total': len(players),
        'scraped': 0,        # 有爬取的球員數
        'skipped': 0,        # 跳過的球員數
        'new_games': 0,      # 新增的比賽數
        'errors': 0          # 錯誤數
    }
    
    for idx, (player_name, link) in enumerate(players, start=1):
        # 判斷是否需要爬取
        should_scrape, reason = should_scrape_player(player_name, player_last_dates)
        
        if not should_scrape:
            # 【優化】跳過不需要爬取的球員
            if idx % 50 == 0:  # 每 50 位球員顯示一次進度
                logger.info(f"進度：{idx}/{len(players)} - 已跳過多位無需更新的球員")
            stats['skipped'] += 1
            continue
        
        # 取得該球員的最後比賽日期（如果有）
        last_date = player_last_dates.get(player_name)
        
        # 將 URL 轉換為 Game Log URL
        game_log_url = link.replace("/Summary/", "/GameLogs/")
        
        # 顯示進度
        logger.info(f"[{idx}/{len(players)}] {player_name} ({reason})")
        
        # 重試機制
        max_retries = 2  # 【優化】減少重試次數
        for retry in range(max_retries):
            try:
                new_games = scrape_player_games(
                    driver, wait, player_name, game_log_url, last_date
                )
                
                if new_games:
                    all_new_games.extend(new_games)
                    stats['new_games'] += len(new_games)
                    logger.info(f"  ✓ 新增 {len(new_games)} 場比賽")
                else:
                    logger.info(f"  ✓ 無新比賽")
                
                stats['scraped'] += 1
                break
                
            except Exception as e:
                if retry < max_retries - 1:
                    time.sleep(2)
                else:
                    stats['errors'] += 1
                    logger.warning(f"  ✗ 錯誤：{str(e)[:50]}")
        
        # 【優化】減少等待時間
        time.sleep(0.8)
    
    return all_new_games, stats


def save_data(existing_df, new_games):
    """
    合併並儲存資料
    
    Args:
        existing_df: pandas DataFrame，現有資料
        new_games: list，新的比賽記錄
        
    Returns:
        pandas DataFrame，合併後的資料
    
    這個函數會：
    1. 將新資料轉換為 DataFrame
    2. 與現有資料合併
    3. 移除重複記錄
    4. 按球員和日期排序
    5. 儲存為 CSV
    """
    if not new_games:
        logger.info("沒有新資料需要儲存")
        return existing_df
    
    # 建立新資料的 DataFrame
    new_df = pd.DataFrame(new_games, columns=COLUMNS)
    logger.info(f"新增 {len(new_df)} 筆記錄")
    
    # 合併現有資料和新資料
    # pd.concat() 將多個 DataFrame 合併
    # ignore_index=True 重新編排索引
    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    # 移除重複記錄
    # subset 指定用於判斷重複的欄位
    # keep='last' 保留最後出現的記錄（新資料）
    combined_df = combined_df.drop_duplicates(
        subset=['Player', 'Date', 'Team', 'Opponent'],
        keep='last'
    )
    
    # 按球員姓名和日期排序
    # 先將日期轉換為 datetime 格式以便正確排序
    combined_df['_date_sort'] = pd.to_datetime(
        combined_df['Date'], 
        format='%m/%d/%Y', 
        errors='coerce'
    )
    combined_df = combined_df.sort_values(
        ['Player', '_date_sort'], 
        ascending=[True, False]  # 球員升序，日期降序（最新的在前）
    )
    # 移除排序用的臨時欄位
    combined_df = combined_df.drop('_date_sort', axis=1)
    
    # 儲存為 CSV
    combined_df.to_csv(CSV_FILENAME, index=False, encoding='utf-8-sig')
    logger.info(f"資料已儲存至 {CSV_FILENAME}")
    logger.info(f"總共 {len(combined_df)} 筆記錄")
    
    return combined_df


def main():
    """主程式入口點"""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"NBA Game Log 爬蟲開始執行（增量模式）")
    logger.info(f"開始時間：{start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    driver = None
    exit_code = 0
    
    try:
        # 步驟 1: 載入現有資料
        existing_df, player_last_dates = load_existing_data()
        
        # 步驟 2: 啟動瀏覽器
        driver, wait = setup_chrome_driver()
        
        # 步驟 3: 取得球員列表
        players = get_player_links(driver, wait)
        
        if not players:
            logger.error("未找到任何球員！")
            return 1
        
        # 步驟 4: 爬取資料（增量模式）
        new_games, stats = scrape_all_players(driver, wait, players, player_last_dates)
        
        # 步驟 5: 儲存資料
        save_data(existing_df, new_games)
        
        # 顯示統計資訊
        logger.info("=" * 60)
        logger.info("爬蟲統計資訊：")
        logger.info("=" * 60)
        logger.info(f"總球員數：{stats['total']}")
        logger.info(f"已爬取：{stats['scraped']} 位球員")
        logger.info(f"已跳過：{stats['skipped']} 位球員（無需更新）")
        logger.info(f"新增比賽：{stats['new_games']} 場")
        logger.info(f"錯誤：{stats['errors']} 次")
        
        # 計算節省的時間
        if stats['skipped'] > 0:
            saved_time = stats['skipped'] * 7  # 每位球員約 7 秒
            logger.info(f"📈 增量模式節省約 {saved_time // 60} 分鐘")
        
    except Exception as e:
        logger.error(f"程式執行錯誤：{e}")
        exit_code = 1
        
    finally:
        if driver:
            driver.quit()
            logger.info("瀏覽器已關閉")
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"執行時間：{duration}")
        logger.info("=" * 60)
    
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
