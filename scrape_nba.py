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
# logging.getLogger() 取得 logger 物件，用於記錄日誌
logger = logging.getLogger(__name__)  # __name__ 是當前模組的名稱


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
    
    # driver.find_elements() 尋找所有符合條件的元素（複數）
    # "table tbody tr td:nth-child(2) a" 的意思是：
    #   - table: 表格元素
    #   - tbody: 表格主體
    #   - tr: 表格的每一列
    #   - td:nth-child(2): 每列的第二個儲存格（球員姓名所在欄位）
    #   - a: 該儲存格中的超連結
    player_links = driver.find_elements(By.CSS_SELECTOR, "table tbody tr td:nth-child(2) a")
    
    # 使用列表推導式提取所有連結的網址
    # link.get_attribute("href") 取得每個 <a> 標籤的 href 屬性（網址）
    links = [link.get_attribute("href") for link in player_links]
    
    logger.info(f"找到 {len(links)} 位球員的連結")
    return links


def get_player_name(driver):
    """
    從頁面取得球員姓名
    
    Args:
        driver: Selenium WebDriver 物件
        
    Returns:
        str: 球員姓名，如果取得失敗則返回 "Unknown"
        
    這個函數使用 JavaScript 來取得 h2 元素的純文字內容
    排除 span 標籤內的文字（如球衣號碼）
    """
    try:
        # 找到 h2 元素（包含球員姓名）
        h2_element = driver.find_element(By.CSS_SELECTOR, "div.half-column-left h2")
        
        # 使用 JavaScript 取得純文字內容
        # childNodes 是該元素的所有子節點
        # nodeType === Node.TEXT_NODE (值為 3) 檢查是否為純文字節點
        # nodeValue 取得該節點的文字值
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
        return "Unknown"


def select_dropdown_option(select_element, options_to_try, fallback_index=0):
    """
    嘗試選擇下拉選單的選項
    
    Args:
        select_element: Selenium Select 物件
        options_to_try: list, 要嘗試的選項文字列表
        fallback_index: int, 如果所有選項都找不到，使用的索引
        
    Returns:
        bool: 是否成功選擇選項
        
    這個函數會依序嘗試選擇列表中的選項
    如果都失敗，則使用 fallback_index 的選項
    """
    for option_text in options_to_try:
        try:
            # select_by_visible_text() 根據可見文字選擇選項
            select_element.select_by_visible_text(option_text)
            return True
        except Exception:
            continue
    
    # 如果所有選項都找不到，嘗試使用索引
    try:
        # select_by_index() 根據索引位置選擇（從 0 開始）
        select_element.select_by_index(fallback_index)
        return True
    except Exception:
        return False


def scrape_player_game_logs(driver, wait, links, season="2025-2026"):
    """
    爬取所有球員的 Game Log 資料
    
    Args:
        driver: Selenium WebDriver 物件
        wait: WebDriverWait 物件
        links: list, 球員頁面 URL 列表
        season: str, 要爬取的賽季（預設 "2025-2026"）
        
    Returns:
        tuple: (all_logs, success_count, failure_count, failure_reasons)
        - all_logs: list, 所有比賽記錄
        - success_count: int, 成功的球員數
        - failure_count: int, 失敗的球員數
        - failure_reasons: dict, 失敗原因統計
        
    這是主要的爬蟲函數，會遍歷所有球員並抓取他們的 Game Log
    """
    # all_logs 儲存所有球員的比賽記錄
    all_logs = []
    # failure_reasons 記錄各種錯誤類型的發生次數
    failure_reasons = {}
    # 成功和失敗的計數器
    success_count = 0
    failure_count = 0
    
    # enumerate(links, start=1) 同時取得索引和值，索引從 1 開始
    for idx, link in enumerate(links, start=1):
        # 將球員概要頁面的 URL 轉換成 Game Log 頁面的 URL
        # replace() 把網址中的 "Summary" 替換成 "GameLogs"
        game_log_url = link.replace("/Summary/", "/GameLogs/")
        
        # 重試機制：最多重試 3 次
        max_retries = 3
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                if retry_count > 0:
                    logger.info(f"  ⟳ 重試第 {retry_count} 次...")
                
                # 訪問球員的 Game Log 頁面
                driver.get(game_log_url)
                
                # 等待表格出現
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
                
                # 取得球員姓名
                player_name = get_player_name(driver)
                if player_name == "Unknown":
                    player_name = f"球員 {idx}"
                
                if retry_count == 0:
                    logger.info(f"正在抓取第 {idx}/{len(links)} 位球員：{player_name}")
                
                # 尋找頁面上所有的下拉選單
                selects = driver.find_elements(By.TAG_NAME, "select")
                
                if len(selects) >= 3:
                    # 第一個選擇器：League（聯盟）
                    league_select = Select(selects[0])
                    select_dropdown_option(league_select, ["NBA"])
                    time.sleep(1.5)
                    
                    # 第二個選擇器：Season（賽季）
                    season_select = Select(selects[1])
                    # 嘗試不同的賽季格式
                    season_variations = [season, season.replace("-20", "-")]  # "2025-2026" 和 "2025-26"
                    select_dropdown_option(season_select, season_variations)
                    time.sleep(1.5)
                    
                    # 第三個選擇器：Games（比賽類型）
                    games_select = Select(selects[2])
                    select_dropdown_option(games_select, ["All Games", "Regular Season"])
                    time.sleep(2)
                    
                    # 等待表格完全載入
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody")))
                    time.sleep(1)
                
                # 抓取表格資料
                rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                game_count = 0
                
                # 使用索引遍歷，避免 stale element 錯誤
                for row_idx in range(len(rows)):
                    try:
                        # 每次迭代都重新查找所有列
                        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                        if row_idx >= len(rows):
                            break
                        
                        row = rows[row_idx]
                        cells = row.find_elements(By.TAG_NAME, "td")
                        # 提取每個儲存格的文字
                        cell_data = [cell.text.strip() for cell in cells]
                        
                        if cell_data:
                            # 加上球員姓名和賽季資訊
                            cell_data_with_info = [player_name, season] + cell_data
                            all_logs.append(cell_data_with_info)
                            game_count += 1
                    except Exception:
                        continue
                
                logger.info(f"  ✓ 成功抓取 {game_count} 場比賽資料")
                success = True
                success_count += 1
                
                # 暫停 1 秒，避免對伺服器造成過大負擔
                time.sleep(1)
                
            except Exception as e:
                retry_count += 1
                
                if retry_count < max_retries:
                    wait_time = retry_count
                    time.sleep(wait_time)
                else:
                    # 分類錯誤類型
                    error_message = str(e)
                    if "stale element" in error_message.lower():
                        error_type = "Stale Element"
                    elif "timeout" in error_message.lower() or "timed out" in error_message.lower():
                        error_type = "Timeout"
                    elif "no such element" in error_message.lower():
                        error_type = "Element Not Found"
                    else:
                        error_type = "Other"
                    
                    failure_reasons[error_type] = failure_reasons.get(error_type, 0) + 1
                    short_error = error_message[:100] + "..." if len(error_message) > 100 else error_message
                    logger.warning(f"  ✗ 無法抓取球員 {idx} [{error_type}]：{short_error}")
                    failure_count += 1
    
    return all_logs, success_count, failure_count, failure_reasons


def save_to_csv(all_logs, filename="nba_player_game_logs.csv"):
    """
    將爬取的資料儲存為 CSV 檔案
    
    Args:
        all_logs: list, 所有比賽記錄
        filename: str, 輸出檔案名稱
        
    Returns:
        pd.DataFrame: 儲存的 DataFrame
        
    這個函數將資料轉換為 pandas DataFrame 並儲存為 CSV
    """
    # 定義欄位名稱
    # 這些欄位對應 RealGM Game Log 表格的標題
    columns = [
        "Player",      # 球員姓名
        "Season",      # 賽季
        "Date",        # 日期
        "Team",        # 球隊
        "Opponent",    # 對手
        "W/L",         # 勝負
        "Status",      # 狀態（先發/替補）
        "Pos",         # 位置
        "MIN",         # 上場時間
        "PTS",         # 得分
        "FGM",         # 投籃命中數
        "FGA",         # 投籃出手數
        "FG%",         # 投籃命中率
        "3PM",         # 三分命中數
        "3PA",         # 三分出手數
        "3P%",         # 三分命中率
        "FTM",         # 罰球命中數
        "FTA",         # 罰球出手數
        "FT%",         # 罰球命中率
        "ORB",         # 進攻籃板
        "DRB",         # 防守籃板
        "REB",         # 總籃板
        "AST",         # 助攻
        "STL",         # 抄截
        "BLK",         # 阻攻
        "TOV",         # 失誤
        "PF",          # 犯規
        "FIC"          # Floor Impact Counter（綜合表現指標）
    ]
    
    # pd.DataFrame() 將列表轉換成 DataFrame
    df = pd.DataFrame(all_logs, columns=columns)
    
    # df.to_csv() 儲存為 CSV 檔案
    # index=False 不儲存索引欄
    # encoding='utf-8-sig' 確保中文字元正確顯示
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    
    logger.info(f"資料已儲存至 {filename}")
    logger.info(f"總共有 {len(df)} 筆記錄，{len(df.columns)} 個欄位")
    
    return df


def main():
    """
    主程式入口點
    
    這個函數是程式的進入點，會：
    1. 啟動瀏覽器
    2. 取得球員列表
    3. 爬取所有球員的 Game Log
    4. 儲存為 CSV
    5. 關閉瀏覽器
    6. 回報執行結果
    
    Returns:
        int: 退出碼（0 表示成功，1 表示失敗）
    """
    # 記錄開始時間
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"NBA Game Log 爬蟲開始執行 - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    driver = None
    exit_code = 0
    
    try:
        # 步驟 1: 啟動瀏覽器
        driver, wait = setup_chrome_driver()
        
        # 步驟 2: 取得球員列表
        links = get_player_links(driver, wait)
        
        if not links:
            logger.error("未找到任何球員連結！")
            return 1
        
        # 步驟 3: 爬取所有球員的 Game Log
        all_logs, success_count, failure_count, failure_reasons = scrape_player_game_logs(
            driver, wait, links
        )
        
        # 步驟 4: 儲存為 CSV
        if all_logs:
            save_to_csv(all_logs)
        else:
            logger.warning("沒有抓取到任何資料！")
            exit_code = 1
        
        # 顯示統計資訊
        logger.info("=" * 60)
        logger.info("爬蟲統計資訊：")
        logger.info("=" * 60)
        total = success_count + failure_count
        logger.info(f"✓ 成功：{success_count}/{total} 位球員 ({success_count/total*100:.1f}%)")
        logger.info(f"✗ 失敗：{failure_count}/{total} 位球員 ({failure_count/total*100:.1f}%)")
        logger.info(f"📊 總共抓取了 {len(all_logs)} 筆 game log 資料")
        
        if failure_reasons:
            logger.info("\n失敗原因分析：")
            for reason, count in sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True):
                pct = count/failure_count*100 if failure_count > 0 else 0
                logger.info(f"  • {reason}: {count} 次 ({pct:.1f}%)")
        
        # 如果失敗率超過 20%，視為部分失敗
        if failure_count / total > 0.2:
            logger.warning("失敗率超過 20%，請檢查網站結構是否有變化")
            exit_code = 1
            
    except Exception as e:
        logger.error(f"程式執行錯誤：{e}")
        exit_code = 1
        
    finally:
        # 步驟 5: 關閉瀏覽器
        if driver:
            driver.quit()
            logger.info("瀏覽器已關閉")
        
        # 記錄執行時間
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"執行時間：{duration}")
        logger.info("=" * 60)
    
    return exit_code


# ============================================================
# 程式入口點
# ============================================================
# if __name__ == "__main__": 是 Python 的慣用寫法
# 當這個檔案被直接執行時（而不是被 import 時），才會執行這段程式碼
# 這樣可以讓這個檔案既可以作為模組被 import，也可以直接執行
if __name__ == "__main__":
    # sys.exit() 以指定的退出碼結束程式
    # 0 表示成功，非 0 表示失敗
    # GitHub Actions 會根據退出碼判斷任務是否成功
    sys.exit(main())
