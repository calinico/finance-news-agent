#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================
# FINANCE NEWS AGENT v5.0 — COMPLETO & STABILE
# ============================================
# Fix: deduplicazione, logging, indicatori tecnici reali,
#      date specifiche, nomi aziende, confidence score,
#      gestione errori, heartbeat, retry, watchdog

import feedparser
import requests
import os
import re
import html as html_lib
import sqlite3
import signal
import sys
import logging
import hashlib
import yaml
import threading
import time
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

try:
    import psutil
except ImportError:
    psutil = None

# ============================================
# CONFIGURAZIONE
# ============================================
CONFIG_PATH = Path("config.yaml")

DEFAULT_FINANCE_SOURCES = [
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://www.investing.com/rss/news_25.rss",
    "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
]

DEFAULT_GEOPOL_SOURCES = [
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.reutersagency.com/feed/?best-topics=world&post_type=best",
    "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
]


def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
    else:
        cfg = {}
    cfg.setdefault('telegram', {})
    cfg['telegram']['token'] = os.getenv('TELEGRAM_TOKEN', cfg['telegram'].get('token', ''))
    cfg['telegram']['chat_id'] = os.getenv('TELEGRAM_CHAT_ID', cfg['telegram'].get('chat_id', ''))
    cfg.setdefault('run_interval_hours', 4)
    cfg.setdefault('database', {}).setdefault('path', 'agent_data.db')
    cfg.setdefault('technical', {})
    cfg.setdefault('trading', {})
    cfg.setdefault('limits', {})
    cfg['limits'].setdefault('max_news_per_cycle', 10)
    cfg['limits'].setdefault('max_tickers_per_news', 3)
    cfg['limits'].setdefault('max_charts_per_cycle', 8)
    cfg.setdefault('sources', {}).setdefault('finance', [])
    cfg.setdefault('sources', {}).setdefault('geopol', [])
    if not cfg['sources']['finance']:
        cfg['sources']['finance'] = DEFAULT_FINANCE_SOURCES
    if not cfg['sources']['geopol']:
        cfg['sources']['geopol'] = DEFAULT_GEOPOL_SOURCES
    cfg.setdefault('heartbeat', {}).setdefault('enabled', True)
    cfg.setdefault('heartbeat', {}).setdefault('interval_runs', 6)
    cfg.setdefault('charts', {}).setdefault('dir', 'charts')
    cfg.setdefault('charts', {}).setdefault('enabled', True)
    cfg.setdefault('logging', {})
    return cfg


CFG = load_config()
TELEGRAM_TOKEN = CFG['telegram']['token']
CHAT_ID = CFG['telegram']['chat_id']
DB_PATH = CFG['database']['path']
RUN_INTERVAL_HOURS = CFG['run_interval_hours']
CHARTS_DIR = Path(CFG['charts']['dir'])
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

# ============================================
# LOGGING
# ============================================


def setup_logging() -> logging.Logger:
    log_cfg = CFG.get('logging', {})
    level = getattr(logging, log_cfg.get('level', 'INFO').upper(), logging.INFO)
    log_file = log_cfg.get('file', 'agent.log')
    max_bytes = log_cfg.get('max_bytes', 1048576)
    backup_count = log_cfg.get('backup_count', 5)
    logger = logging.getLogger('finance_agent')
    logger.setLevel(level)
    if not logger.handlers:
        fh = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
        fh.setLevel(level)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        fmt = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


logger = setup_logging()

# ============================================
# WATCHDOG
# ============================================


class Watchdog:
    def __init__(self, timeout_sec: int = 300):
        self.timeout = timeout_sec
        self._timer = None
        self._running = False
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            self._running = True
            self._reset_timer()
        logger.info("Watchdog avviato")

    def stop(self):
        with self._lock:
            self._running = False
            if self._timer:
                self._timer.cancel()
                self._timer = None
        logger.info("Watchdog fermato")

    def heartbeat(self):
        with self._lock:
            if self._running:
                self._reset_timer()

    def _reset_timer(self):
        if self._timer:
            self._timer.cancel()
        self._timer = threading.Timer(self.timeout, self._on_timeout)
        self._timer.daemon = True
        self._timer.start()

    def _on_timeout(self):
        global WATCHDOG_TRIGGERED
        WATCHDOG_TRIGGERED = True
        logger.warning("WATCHDOG TRIGGERED - ciclo bloccato!")


watchdog = Watchdog(timeout_sec=600)
WATCHDOG_TRIGGERED = False

# ============================================
# DATABASE
# ============================================


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def init_db(self):
        with self._connect() as conn:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS sent_news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title_hash TEXT UNIQUE NOT NULL,
                    title TEXT,
                    tickers TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS execution_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT,
                    news_count INTEGER DEFAULT 0,
                    charts_count INTEGER DEFAULT 0,
                    error_msg TEXT,
                    duration_sec REAL
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    company_name TEXT,
                    strategy TEXT,
                    entry_price REAL,
                    target_1 REAL,
                    target_2 REAL,
                    target_3 REAL,
                    stop_loss REAL,
                    confidence_score INTEGER,
                    position TEXT,
                    valid_until TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    hit_target INTEGER DEFAULT 0,
                    hit_stop INTEGER DEFAULT 0
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS heartbeat_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT,
                    memory_mb REAL,
                    uptime_hours REAL
                )
            """)
            conn.commit()
            logger.info("Database inizializzato")

    def is_news_sent(self, title: str) -> bool:
        h = hashlib.md5(title.lower().strip().encode()).hexdigest()
        with self._connect() as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM sent_news WHERE title_hash = ?", (h,))
            return c.fetchone() is not None

    def mark_news_sent(self, title: str, tickers: List[str]):
        h = hashlib.md5(title.lower().strip().encode()).hexdigest()
        with self._connect() as conn:
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO sent_news (title_hash, title, tickers) VALUES (?, ?, ?)",
                      (h, title, ','.join(tickers)))
            conn.commit()

    def get_last_execution(self) -> Optional[Tuple]:
        with self._connect() as conn:
            c = conn.cursor()
            c.execute("SELECT run_at, status, news_count, charts_count, error_msg FROM execution_log ORDER BY id DESC LIMIT 1")
            return c.fetchone()

    def log_execution(self, status: str, news_count: int = 0, charts_count: int = 0, error_msg: str = "", duration_sec: float = 0.0):
        with self._connect() as conn:
            c = conn.cursor()
            c.execute("INSERT INTO execution_log (status, news_count, charts_count, error_msg, duration_sec) VALUES (?, ?, ?, ?, ?)",
                      (status, news_count, charts_count, error_msg, duration_sec))
            conn.commit()

    def save_prediction(self, ticker: str, company_name: str, strategy: str, entry: float,
                        target_1: float, target_2: float, target_3: float, stop_loss: float,
                        confidence: int, position: str, valid_until: str):
        with self._connect() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO predictions (ticker, company_name, strategy, entry_price, target_1, target_2, target_3, stop_loss, confidence_score, position, valid_until)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ticker, company_name, strategy, entry, target_1, target_2, target_3, stop_loss, confidence, position, valid_until))
            conn.commit()

    def cleanup_old_news(self, days: int = 30):
        with self._connect() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM sent_news WHERE sent_at < datetime('now', '-{} days')".format(days))
            deleted = c.rowcount
            conn.commit()
            if deleted > 0:
                logger.info(f"Pulizia DB: {deleted} notizie vecchie eliminate")

    def log_heartbeat(self, status: str, memory_mb: float, uptime_hours: float):
        with self._connect() as conn:
            c = conn.cursor()
            c.execute("INSERT INTO heartbeat_log (status, memory_mb, uptime_hours) VALUES (?, ?, ?)",
                      (status, memory_mb, uptime_hours))
            conn.commit()


db = Database(DB_PATH)

# ============================================
# NOMI AZIENDE
# ============================================
COMPANY_NAMES = {
    "AAPL": "Apple Inc.", "MSFT": "Microsoft Corp.", "GOOGL": "Alphabet Inc.", "GOOG": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.", "META": "Meta Platforms Inc.", "NVDA": "NVIDIA Corp.", "TSLA": "Tesla Inc.",
    "JPM": "JPMorgan Chase & Co.", "BAC": "Bank of America Corp.", "WFC": "Wells Fargo & Co.",
    "C": "Citigroup Inc.", "GS": "Goldman Sachs Group", "MS": "Morgan Stanley", "V": "Visa Inc.",
    "MA": "Mastercard Inc.", "JNJ": "Johnson & Johnson", "PFE": "Pfizer Inc.", "MRK": "Merck & Co.",
    "ABBV": "AbbVie Inc.", "LLY": "Eli Lilly & Co.", "NVO": "Novo Nordisk A/S", "UNH": "UnitedHealth Group",
    "XOM": "Exxon Mobil Corp.", "CVX": "Chevron Corp.", "COP": "ConocoPhillips", "OXY": "Occidental Petroleum",
    "SPY": "SPDR S&P 500 ETF Trust", "QQQ": "Invesco QQQ Trust", "DIA": "SPDR Dow Jones Industrial Average",
    "XLF": "Financial Select Sector SPDR", "XLK": "Technology Select Sector SPDR",
    "XLE": "Energy Select Sector SPDR", "TLT": "iShares 20+ Year Treasury Bond",
    "GLD": "SPDR Gold Shares", "USO": "United States Oil Fund", "IWM": "iShares Russell 2000 ETF",
    "VTI": "Vanguard Total Stock Market ETF", "BABA": "Alibaba Group Holding", "TCEHY": "Tencent Holdings Ltd.",
    "TSM": "Taiwan Semiconductor Mfg.", "AMD": "Advanced Micro Devices", "INTC": "Intel Corp.",
    "QCOM": "Qualcomm Inc.", "CRM": "Salesforce Inc.", "NOW": "ServiceNow Inc.", "SNOW": "Snowflake Inc.",
    "PLTR": "Palantir Technologies", "COIN": "Coinbase Global Inc.", "MSTR": "MicroStrategy Inc.",
    "RIOT": "Riot Platforms Inc.", "MARA": "Marathon Digital Holdings", "NFLX": "Netflix Inc.",
    "DIS": "Walt Disney Co.", "WBD": "Warner Bros. Discovery", "PARA": "Paramount Global",
    "BA": "Boeing Co.", "LMT": "Lockheed Martin Corp.", "RTX": "RTX Corp.", "GE": "GE Aerospace",
    "CAT": "Caterpillar Inc.", "DE": "Deere & Co.", "F": "Ford Motor Co.", "GM": "General Motors Co.",
    "NKE": "Nike Inc.", "MCD": "McDonald's Corp.", "SBUX": "Starbucks Corp.", "KO": "Coca-Cola Co.",
    "PEP": "PepsiCo Inc.", "WMT": "Walmart Inc.", "TGT": "Target Corp.", "COST": "Costco Wholesale Corp.",
    "HD": "Home Depot Inc.", "LOW": "Lowe's Companies Inc.", "T": "AT&T Inc.", "VZ": "Verizon Communications",
    "TMUS": "T-Mobile US Inc.", "NEE": "NextEra Energy Inc.", "DUK": "Duke Energy Corp.",
    "SO": "Southern Co.", "ENPH": "Enphase Energy Inc.", "SEDG": "SolarEdge Technologies",
    "FSLR": "First Solar Inc.", "RKLB": "Rocket Lab USA Inc.", "SPCE": "Virgin Galactic Holdings",
    "ASTS": "AST SpaceMobile Inc.", "NUE": "Nucor Corp.", "STLD": "Steel Dynamics Inc.",
    "FCX": "Freeport-McMoRan Inc.", "VALE": "Vale S.A.", "RIO": "Rio Tinto Group", "BHP": "BHP Group Ltd.",
    "SLV": "iShares Silver Trust", "BITO": "ProShares Bitcoin Strategy ETF",
    "BITW": "Bitwise Bitcoin Strategy ETF", "GBTC": "Grayscale Bitcoin Trust",
    "EWG": "iShares MSCI Germany ETF", "EWQ": "iShares MSCI France ETF",
    "EWI": "iShares MSCI Italy ETF", "EWP": "iShares MSCI Spain ETF",
    "EWU": "iShares MSCI UK ETF", "VGK": "Vanguard FTSE Europe ETF",
    "FXI": "iShares China Large-Cap ETF", "MCHI": "iShares MSCI China ETF",
    "EWJ": "iShares MSCI Japan ETF", "INDA": "iShares MSCI India ETF",
    "EWT": "iShares MSCI Taiwan ETF", "EWY": "iShares MSCI South Korea ETF",
    "EWA": "iShares MSCI Australia ETF", "EWW": "iShares MSCI Mexico ETF",
    "EWZ": "iShares MSCI Brazil ETF", "ARGT": "Global X MSCI Argentina ETF",
    "KSA": "iShares MSCI Saudi Arabia ETF", "UAE": "iShares MSCI UAE ETF",
    "ISRA": "VanEck Israel ETF", "EIS": "iShares MSCI Israel ETF",
    "RSX": "VanEck Russia ETF", "VIXY": "ProShares VIX Short-Term Futures",
    "SQQQ": "ProShares Short QQQ", "IEF": "iShares 7-10 Year Treasury Bond",
    "IAU": "iShares Gold Trust", "PHYS": "Sprott Physical Gold Trust",
    "GOLD": "Barrick Gold Corp.", "UNG": "United States Natural Gas Fund",
    "BOIL": "ProShares Ultra Bloomberg Natural Gas", "WEAT": "Teucrium Wheat Fund",
    "CORN": "Teucrium Corn Fund", "SOYB": "Teucrium Soybean Fund",
    "PDBC": "Invesco Commodity Index Tracking", "VNQ": "Vanguard Real Estate ETF",
    "SPG": "Simon Property Group", "O": "Realty Income Corp.",
    "PLD": "Prologis Inc.", "AMT": "American Tower Corp.", "DHI": "D.R. Horton Inc.",
    "LEN": "Lennar Corp.", "PHM": "PulteGroup Inc.", "NVR": "NVR Inc.", "KBH": "KB Home",
    "HII": "Huntington Ingalls Industries", "KTOS": "Kratos Defense & Security Solutions",
    "BWXT": "BWX Technologies Inc.", "CRWD": "CrowdStrike Holdings Inc.",
    "PANW": "Palo Alto Networks Inc.", "FTNT": "Fortinet Inc.", "ZS": "Zscaler Inc.",
    "OKTA": "Okta Inc.", "NET": "Cloudflare Inc.",
    "DDOG": "Datadog Inc.", "MDB": "MongoDB Inc.", "VEEV": "Veeva Systems Inc.",
    "DOCU": "DocuSign Inc.", "SHOP": "Shopify Inc.", "ETSY": "Etsy Inc.",
    "EBAY": "eBay Inc.", "W": "Wayfair Inc.", "RIVN": "Rivian Automotive Inc.",
    "LCID": "Lucid Group Inc.", "NIO": "NIO Inc.", "XPEV": "XPeng Inc.",
    "LI": "Li Auto Inc.", "QS": "QuantumScape Corp.", "MP": "MP Materials Corp.",
    "ALB": "Albemarle Corp.", "SQM": "Sociedad Quimica y Minera", "LTHM": "Livent Corp.",
    "ENI": "Eni S.p.A.", "UCG": "UniCredit S.p.A.", "ISP": "Intesa Sanpaolo S.p.A.",
    "LUX": "Luxottica Group", "TOT": "TotalEnergies SE", "OR": "L'Oreal S.A.",
    "SAN": "Sanofi S.A.", "AIR": "Airbus SE", "SAP": "SAP SE", "SIE": "Siemens AG",
    "BMWYY": "Bayerische Motoren Werke AG", "VWAGY": "Volkswagen AG",
    "MBGYY": "Mercedes-Benz Group AG", "TM": "Toyota Motor Corp.",
    "HMC": "Honda Motor Co.", "STLA": "Stellantis N.V.", "HYMTF": "Hyundai Motor Co.",
    "RACE": "Ferrari N.V.", "HSBC": "HSBC Holdings plc", "BP": "BP p.l.c.",
    "SHEL": "Shell plc", "AZN": "AstraZeneca plc", "UL": "Unilever PLC",
    "GSK": "GSK plc", "SNY": "Sanofi S.A.", "BTI": "British American Tobacco",
    "INFY": "Infosys Ltd.", "TCS": "Tata Consultancy Services", "WIT": "Wipro Ltd.",
    "HDB": "HDFC Bank Ltd.", "SKM": "SK Telecom Co.", "KB": "KB Financial Group",
    "KEP": "Korea Electric Power Corp.", "POSCO": "POSCO Holdings Inc.",
    "LPL": "LG Display Co.", "WPL": "Woodside Energy Group", "NAB": "National Australia Bank",
    "WBC": "Westpac Banking Corp.", "ANZ": "ANZ Group Holdings", "TEVA": "Teva Pharmaceutical",
    "ICL": "ICL Group Ltd.", "CHKP": "Check Point Software Technologies",
    "CYBR": "CyberArk Software Ltd.", "PBR": "Petroleo Brasileiro S.A.",
    "ITUB": "Itau Unibanco Holding", "BBD": "Banco Bradesco S.A.",
    "FMX": "Fomento Economico Mexicano", "AMX": "America Movil S.A.B.",
    "CEMEX": "Cemex S.A.B. de C.V.", "GMEXIC": "Grupo Mexico S.A.B.",
    "GGAL": "Grupo Financiero Galicia", "YPF": "YPF S.A.", "PAM": "Pampa Energia S.A.",
    "TEO": "Telecom Argentina S.A.", "NEM": "Newmont Corp.", "AEM": "Agnico Eagle Mines",
    "KGC": "Kinross Gold Corp.", "WPM": "Wheaton Precious Metals Corp.",
    "RGLD": "Royal Gold Inc.", "FNV": "Franco-Nevada Corp.", "PAAS": "Pan American Silver Corp.",
    "HL": "Hecla Mining Co.", "CDE": "Coeur Mining Inc.", "EXK": "Endeavour Silver Corp.",
    "MAG": "MAG Silver Corp.", "SCCO": "Southern Copper Corp.", "TECK": "Teck Resources Ltd.",
    "GLNCY": "Glencore plc", "ANTO": "Antofagasta plc", "CLF": "Cleveland-Cliffs Inc.",
    "TX": "Ternium S.A.", "DBC": "Invesco DB Commodity Tracking",
    "GSG": "iShares S&P GSCI Commodity-Indexed Trust", "COMT": "iShares GSCI Commodity Dynamic Roll Strategy",
    "USCI": "United States Commodity Index Fund", "GCC": "WisdomTree Continuous Commodity Index",
    "ITA": "iShares U.S. Aerospace & Defense ETF",
    "LUNR": "Intuitive Machines Inc.", "HOOD": "Robinhood Markets Inc.",
    "ETHE": "Grayscale Ethereum Trust", "HIVE": "HIVE Blockchain Technologies Ltd.",
    "HUT": "Hut 8 Mining Corp.", "BITF": "Bitfarms Ltd.", "SQ": "Block Inc.",
    "PYPL": "PayPal Holdings Inc.", "IBM": "International Business Machines",
    "ORCL": "Oracle Corp.", "ADBE": "Adobe Inc.", "INTU": "Intuit Inc.",
    "UBER": "Uber Technologies Inc.", "LYFT": "Lyft Inc.", "ABNB": "Airbnb Inc.",
    "ZM": "Zoom Video Communications", "ROKU": "Roku Inc.", "FUBO": "fuboTV Inc.",
    "AMC": "AMC Entertainment Holdings", "CNK": "Cinemark Holdings Inc.",
    "TJX": "TJX Companies Inc.", "ROST": "Ross Stores Inc.", "BURL": "Burlington Stores Inc.",
    "BBY": "Best Buy Co. Inc.", "DPZ": "Domino's Pizza Inc.", "YUM": "Yum! Brands Inc.",
    "AEP": "American Electric Power", "EXC": "Exelon Corp.", "SRE": "Sempra Energy",
    "ED": "Consolidated Edison Inc.", "XLU": "Utilities Select Sector SPDR",
    "SMCI": "Super Micro Computer Inc.", "DELL": "Dell Technologies Inc.",
    "HPE": "Hewlett Packard Enterprise", "ANET": "Arista Networks Inc.",
    "AVGO": "Broadcom Inc.", "LITE": "Lumentum Holdings Inc.", "QRVO": "Qorvo Inc.",
    "SWKS": "Skyworks Solutions Inc.", "CRUS": "Cirrus Logic Inc.", "STM": "STMicroelectronics N.V.",
    "MRVL": "Marvell Technology Inc.", "AMAT": "Applied Materials Inc.",
    "LRCX": "Lam Research Corp.", "KLAC": "KLA Corp.", "TER": "Teradyne Inc.",
    "ON": "ON Semiconductor Corp.", "NXPI": "NXP Semiconductors N.V.",
    "TXN": "Texas Instruments Inc.", "ADI": "Analog Devices Inc.", "MCHP": "Microchip Technology Inc.",
    "MPWR": "Monolithic Power Systems Inc.", "TDC": "Teradata Corp.", "TRMB": "Trimble Inc.",
    "SNAP": "Snap Inc.", "PINS": "Pinterest Inc.", "MTCH": "Match Group Inc.",
    "RKT": "Rocket Companies Inc.", "UWMC": "UWM Holdings Corp.", "LDI": "loanDepot Inc.",
    "PFSI": "PennyMac Financial Services", "COOP": "Mr. Cooper Group Inc.",
    "DFS": "Discover Financial Services", "COF": "Capital One Financial Corp.",
    "SYF": "Synchrony Financial", "ALLY": "Ally Financial Inc.",
    "PNC": "PNC Financial Services Group", "USB": "U.S. Bancorp", "TFC": "Truist Financial Corp.",
    "RF": "Regions Financial Corp.", "KRE": "SPDR S&P Regional Banking ETF",
    "KBE": "SPDR S&P Bank ETF", "IYF": "iShares U.S. Financials ETF",
    "VFH": "Vanguard Financials ETF", "VGT": "Vanguard Information Technology ETF",
    "SMH": "VanEck Semiconductor ETF", "SOXX": "iShares Semiconductor ETF",
    "IGV": "iShares Expanded Tech-Software Sector ETF", "VHT": "Vanguard Health Care ETF",
    "XBI": "SPDR S&P Biotech ETF", "IBB": "iShares Biotechnology ETF",
    "XLV": "Health Care Select Sector SPDR", "IHI": "iShares U.S. Medical Devices ETF",
    "DRIV": "Global X Autonomous & Electric Vehicles ETF", "IDRV": "iShares Self-Driving EV and Tech ETF",
    "LIT": "Global X Lithium & Battery Tech ETF", "BATT": "Amplify Lithium & Battery Technology ETF",
    "CARZ": "First Trust NASDAQ Global Auto ETF", "WGMI": "Valkyrie Bitcoin Miners ETF",
    "BKCH": "Global X Blockchain ETF", "SCHH": "Schwab U.S. REIT ETF",
    "USRT": "iShares Core U.S. REIT ETF", "REET": "iShares Global REIT ETF",
    "FREL": "Fidelity MSCI Real Estate Index ETF", "VEU": "Vanguard FTSE All-World ex-US ETF",
    "EZU": "iShares MSCI Eurozone ETF", "FEZ": "SPDR EURO STOXX 50 ETF",
    "BLK": "BlackRock Inc.", "AXP": "American Express Co.", "SCHW": "Charles Schwab Corp.",
    "BK": "Bank of New York Mellon", "STT": "State Street Corp.", "ICE": "Intercontinental Exchange",
    "CME": "CME Group Inc.", "NDAQ": "Nasdaq Inc.", "MCO": "Moody's Corp.",
    "SPGI": "S&P Global Inc.", "AON": "Aon plc", "MMC": "Marsh & McLennan Companies",
    "MET": "MetLife Inc.", "PRU": "Prudential Financial Inc.", "AFL": "Aflac Inc.",
    "ALL": "Allstate Corp.", "TRV": "Travelers Companies Inc.", "CB": "Chubb Ltd.",
    "PGR": "Progressive Corp.", "MMM": "3M Co.", "HON": "Honeywell International Inc.",
    "SNPS": "Synopsys Inc.", "CDNS": "Cadence Design Systems", "ANSS": "ANSYS Inc.",
    "ADSK": "Autodesk Inc.", "WDAY": "Workday Inc.", "TWLO": "Twilio Inc.",
    "REGN": "Regeneron Pharmaceuticals", "VRTX": "Vertex Pharmaceuticals", "ALNY": "Alnylam Pharmaceuticals",
    "SRPT": "Sarepta Therapeutics", "BMRN": "BioMarin Pharmaceutical", "IONS": "Ionis Pharmaceuticals",
    "EXEL": "Exelixis Inc.", "AMGN": "Amgen Inc.", "GILD": "Gilead Sciences", "BIIB": "Biogen Inc.",
    "MRNA": "Moderna Inc.", "BNTX": "BioNTech SE", "NVAX": "Novavax Inc.",
}


def get_company_name(ticker: str) -> str:
    if ticker in COMPANY_NAMES:
        return COMPANY_NAMES[ticker]
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
        resp = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        data = resp.json()
        meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
        name = meta.get("shortName", meta.get("longName", meta.get("symbol", ticker)))
        if name and name != ticker:
            COMPANY_NAMES[ticker] = name
            return name
    except Exception as e:
        logger.debug(f"Fallback nome per {ticker} fallito: {e}")
    return ticker

# ============================================
# DATABASE ASSET, KEYWORD, SECTOR
# ============================================
COUNTRY_ASSETS = {
    "united states": ["SPY", "QQQ", "DIA", "XLF", "TLT", "GLD", "USO"],
    "usa": ["SPY", "QQQ", "DIA", "XLF", "TLT", "GLD", "USO"],
    "fed": ["SPY", "QQQ", "XLF", "TLT", "KRE", "JPM", "BAC", "BLK"],
    "powell": ["SPY", "QQQ", "XLF", "TLT", "KRE"],
    "europe": ["VGK", "EZU", "EWG", "EWQ", "EWI", "FEZ"],
    "ecb": ["VGK", "EZU", "EWG", "EWQ", "SAN"],
    "lagarde": ["VGK", "EZU", "EWG", "EWQ"],
    "germany": ["EWG", "VGK", "EZU", "SAP", "SIE", "BMWYY", "VWAGY"],
    "france": ["EWQ", "VGK", "EZU", "TOT", "OR", "SAN", "AIR"],
    "italy": ["EWI", "VGK", "EZU", "ENI", "UCG", "ISP", "LUX"],
    "spain": ["EWP", "VGK", "EZU", "SAN"],
    "uk": ["EWU", "VGK", "EZU", "HSBC", "BP", "SHEL", "AZN", "UL"],
    "britain": ["EWU", "VGK", "HSBC", "BP", "SHEL"],
    "boe": ["EWU", "VGK", "HSBC", "BP"],
    "bailey": ["EWU", "VGK", "HSBC", "BP"],
    "china": ["FXI", "MCHI", "BABA", "TCEHY"],
    "taiwan": ["EWT", "FXI", "TSM"],
    "japan": ["EWJ", "TM", "HMC"],
    "india": ["INDA", "INFY", "TCS", "WIT", "HDB"],
    "south korea": ["EWY", "SKM", "KB", "KEP", "POSCO", "LPL"],
    "australia": ["EWA", "BHP", "RIO", "WPL", "NAB", "WBC", "ANZ"],
    "israel": ["ISRA", "EIS", "TEVA", "ICL", "CHKP", "CYBR"],
    "iran": ["USO", "XLE", "CVX", "XOM"],
    "saudi arabia": ["KSA", "USO", "XLE", "CVX", "XOM"],
    "uae": ["UAE", "USO", "XLE"],
    "russia": ["RSX", "USO", "GLD", "UNG", "WEAT", "CORN", "SOYB"],
    "ukraine": ["USO", "UNG", "WEAT", "CORN", "SOYB", "GLD", "RSX"],
    "brazil": ["EWZ", "PBR", "VALE", "ITUB", "BBD"],
    "mexico": ["EWW", "FMX", "AMX", "CEMEX", "GMEXIC"],
    "argentina": ["ARGT", "GGAL", "YPF", "PAM", "TEO"],
    "gold": ["GLD", "IAU", "PHYS", "GOLD", "NEM", "AEM", "KGC"],
    "oil": ["USO", "XLE", "XOM", "CVX", "COP", "OXY"],
    "natural gas": ["UNG", "BOIL"],
    "wheat": ["WEAT", "CORN", "SOYB"],
    "corn": ["CORN", "WEAT", "SOYB"],
    "bitcoin": ["MSTR", "COIN", "HOOD", "BITO", "BITW", "GBTC", "RIOT", "MARA"],
    "ethereum": ["COIN", "HOOD", "BITW", "ETHE", "RIOT", "MARA", "HIVE"],
}

KEYWORD_TICKERS = {
    "apple": ["AAPL", "AVGO", "LITE", "QRVO", "SWKS"],
    "iphone": ["AAPL", "CRUS", "STM"],
    "microsoft": ["MSFT", "QLYS", "VEEV", "DOCU"],
    "google": ["GOOGL", "GOOG", "TDC", "TRMB"],
    "meta": ["META", "SNAP", "PINS", "MTCH"],
    "nvidia": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "SWKS"],
    "ai": ["NVDA", "AMD", "PLTR", "SNOW", "MDB", "DDOG", "NET"],
    "artificial intelligence": ["NVDA", "AMD", "PLTR", "SNOW", "MDB"],
    "chip": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "SWKS", "QRVO", "MPWR"],
    "semiconductor": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "AMAT", "LRCX"],
    "cloud": ["MSFT", "AMZN", "GOOGL", "CRM", "NOW", "SNOW", "DDOG", "MDB"],
    "cybersecurity": ["CRWD", "PANW", "FTNT", "ZS", "OKTA", "CYBR", "NET"],
    "data center": ["NVDA", "AMD", "INTC", "SMCI", "DELL", "HPE", "ANET"],
    "bank": ["JPM", "BAC", "WFC", "C", "GS", "MS", "PNC", "USB", "TFC", "RF"],
    "banca": ["JPM", "BAC", "WFC", "C", "GS", "MS"],
    "credit": ["JPM", "BAC", "WFC", "C", "DFS", "COF", "SYF", "ALLY"],
    "mortgage": ["RKT", "UWMC", "LDI", "PFSI", "COOP"],
    "oil": ["XOM", "CVX", "COP", "OXY"],
    "petrolio": ["XOM", "CVX", "COP", "OXY"],
    "gas": ["XOM", "CVX", "COP"],
    "energy": ["XOM", "CVX", "COP", "XLE", "OXY"],
    "renewable": ["ENPH", "SEDG", "FSLR"],
    "solar": ["ENPH", "SEDG", "FSLR"],
    "wind": ["GE", "NEE"],
    "pharma": ["JNJ", "PFE", "MRK", "ABBV", "LLY", "NVO", "AZN", "GILD", "BIIB"],
    "drug": ["JNJ", "PFE", "MRK", "ABBV", "LLY", "NVO", "AZN", "GILD", "VRTX"],
    "vaccine": ["PFE", "MRNA", "BNTX", "NVAX", "GSK", "SNY", "JNJ"],
    "biotech": ["AMGN", "GILD", "BIIB", "VRTX", "REGN", "ALNY", "SRPT", "BMRN"],
    "fda": ["JNJ", "PFE", "MRK", "ABBV", "VRTX", "REGN", "ALNY"],
    "clinical trial": ["BIIB", "VRTX", "REGN", "ALNY", "SRPT", "BMRN", "IONS", "EXEL"],
    "tesla": ["TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI"],
    "ev": ["TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI", "QS", "MP"],
    "electric vehicle": ["TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI", "QS", "MP"],
    "automaker": ["F", "GM", "STLA", "TM", "HMC", "HYMTF", "VWAGY", "BMWYY"],
    "car": ["F", "GM", "STLA", "TM", "HMC", "VWAGY", "BMWYY", "RACE"],
    "battery": ["TSLA", "QS", "MP", "ALB", "SQM", "LTHM"],
    "bitcoin": ["MSTR", "COIN", "HOOD", "BITO", "BITW", "GBTC", "RIOT", "MARA"],
    "ethereum": ["COIN", "HOOD", "BITW", "ETHE", "RIOT", "MARA", "HIVE", "HUT"],
    "crypto": ["MSTR", "COIN", "HOOD", "BITO", "RIOT", "MARA", "HIVE", "HUT", "BITF"],
    "blockchain": ["IBM", "COIN", "MSTR", "RIOT", "MARA", "SQ", "PYPL"],
    "real estate": ["VNQ", "SPG", "O", "AMT", "PLD"],
    "housing": ["DHI", "LEN", "PHM", "NVR", "KBH"],
    "construction": ["DHI", "LEN", "PHM", "CAT", "DE"],
    "gold": ["GLD", "IAU", "PHYS", "GOLD", "NEM", "AEM", "KGC", "WPM", "RGLD", "FNV"],
    "silver": ["SLV", "PAAS", "HL", "CDE", "EXK", "MAG"],
    "copper": ["FCX", "SCCO", "TECK", "VALE", "RIO", "BHP", "GLNCY", "ANTO"],
    "commodity": ["PDBC", "USCI", "GCC", "DBC", "GSG", "COMT"],
    "steel": ["NUE", "STLD", "VALE", "RIO", "BHP", "CLF", "TX"],
    "amazon": ["AMZN", "SHOP", "ETSY", "EBAY", "W"],
    "retail": ["WMT", "TGT", "COST", "HD", "LOW", "BBY", "TJX", "ROST", "BURL"],
    "consumer": ["KO", "PEP", "WMT", "COST", "MCD", "SBUX", "DPZ", "YUM"],
    "defense": ["LMT", "RTX", "BA", "HII", "KTOS", "BWXT"],
    "aerospace": ["BA", "AIR", "GE", "HON", "RTX", "LMT"],
    "infrastructure": ["CAT", "DE"],
    "telecom": ["T", "VZ", "TMUS"],
    "streaming": ["NFLX", "DIS", "WBD", "PARA", "ROKU", "FUBO", "AMC", "CNK"],
    "ozempic": ["NVO", "LLY", "PFE", "MRK", "ABBV"],
    "wegovy": ["NVO", "LLY"],
    "weight loss": ["NVO", "LLY", "PFE", "MRK"],
    "diabetes": ["NVO", "LLY", "PFE", "MRK", "JNJ"],
    "spacex": ["RKLB", "ASTS", "SPCE", "LUNR"],
    "space": ["RKLB", "ASTS", "SPCE", "LUNR", "BA", "LMT"],
    "paramount": ["PARA", "WBD", "DIS", "NFLX"],
    "warner": ["WBD", "PARA", "DIS", "NFLX"],
    "mercedes": ["MBGYY", "VWAGY", "BMWYY", "TM", "HMC", "STLA", "F", "GM"],
    "electricity": ["XLU", "NEE", "DUK", "SO", "AEP", "EXC", "SRE", "ED"],
    "utility": ["XLU", "NEE", "DUK", "SO", "AEP", "EXC", "SRE", "ED"],
}

SECTOR_ETFS = {
    "Tech": ["XLK", "VGT", "SMH", "SOXX", "IGV"],
    "Banche/Finanza": ["XLF", "VFH", "KRE", "KBE", "IYF"],
    "Energia": ["XLE", "OIH", "XOP"],
    "Farmaceutica/Biotech": ["XBI", "IBB", "VHT", "XLV", "IHI"],
    "Auto/Elettrici": ["DRIV", "IDRV", "LIT", "BATT", "CARZ"],
    "Crypto": ["BITO", "BITW", "WGMI", "BKCH"],
    "Immobiliare": ["VNQ", "SCHH", "USRT", "REET", "FREL"],
    "Materie Prime": ["PDBC", "USCI", "GCC", "GSG", "COMT"],
    "Indici Globali": ["SPY", "QQQ", "IWM", "DIA", "VTI", "VEU"],
    "Geopolitica/Safe Haven": ["GLD", "IAU", "TLT", "IEF", "VIXY", "SQQQ"],
    "Utility/Energia": ["XLU", "NEE", "DUK", "SO", "AEP"],
    "Media/Entertainment": ["PARA", "WBD", "DIS", "NFLX"],
    "Space/Aerospace": ["ITA", "BA", "LMT", "RTX", "RKLB"],
}

SECTOR_KEYWORDS = {
    "Tech": ["apple", "microsoft", "google", "meta", "nvidia", "ai", "artificial intelligence", "chip", "semiconductor", "cloud", "cybersecurity", "data center", "software", "hardware"],
    "Banche/Finanza": ["bank", "banca", "fed", "ecb", "interest rate", "tasso", "banche", "credit", "loan", "mortgage", "central bank", "financial"],
    "Energia": ["oil", "petrolio", "gas", "energy", "renewable", "solar", "wind", "opec", "electricity", "utility", "power"],
    "Farmaceutica/Biotech": ["pharma", "drug", "vaccine", "biotech", "fda", "clinical trial", "medicine", "healthcare", "ozempic", "wegovy", "weight loss", "diabetes"],
    "Auto/Elettrici": ["tesla", "ev", "electric vehicle", "automaker", "car", "battery", "mercedes", "auto"],
    "Crypto": ["bitcoin", "ethereum", "crypto", "blockchain"],
    "Immobiliare": ["real estate", "housing", "property", "mortgage", "construction"],
    "Materie Prime": ["gold", "oro", "silver", "copper", "commodity", "steel"],
    "Indici Globali": ["sp500", "nasdaq", "dow", "ftse", "dax", "nikkei"],
    "Geopolitica/Safe Haven": ["war", "conflict", "sanctions", "tension", "missile", "attack", "invasion", "peace", "treaty", "diplomatic"],
    "Utility/Energia": ["electricity", "utility", "power", "grid"],
    "Media/Entertainment": ["paramount", "warner", "streaming", "media", "movie", "film", "tv", "content"],
    "Space/Aerospace": ["spacex", "space", "rocket", "satellite", "launch", "nasa"],
}


def classify_sectors(title: str, summary: str = "") -> List[str]:
    text = (title + " " + summary).lower()
    affected = []
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            affected.append(sector)
    return affected if affected else ["Indici Globali"]


def find_countries(title: str, summary: str = "") -> List[Tuple[str, List[str]]]:
    text = (title + " " + summary).lower()
    found = []
    for country, assets in COUNTRY_ASSETS.items():
        if country in text:
            found.append((country, assets))
    return found


def find_tickers_from_news(title: str, summary: str = "") -> Tuple[List[str], List[str], List[Tuple[str, List[str]]]]:
    text = (title + " " + summary).lower()
    found_tickers = set()
    matched_keywords = []
    for keyword, tickers in KEYWORD_TICKERS.items():
        if keyword in text:
            matched_keywords.append(keyword)
            for ticker in tickers[:3]:
                found_tickers.add(ticker)
    countries = find_countries(title, summary)
    for country, assets in countries:
        for asset in assets[:3]:
            found_tickers.add(asset)
    if not found_tickers:
        sectors = classify_sectors(title, summary)
        for sector in sectors:
            if sector in SECTOR_ETFS:
                for etf in SECTOR_ETFS[sector][:2]:
                    found_tickers.add(etf)
    return list(found_tickers)[:8], matched_keywords, countries

# ============================================
# FETCH CON RETRY
# ============================================


def fetch_with_retry(url: str, timeout: int = 15, max_retries: int = 3, **kwargs) -> requests.Response:
    last_err = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}, **kwargs)
            if resp.status_code == 200:
                return resp
            logger.warning(f"HTTP {resp.status_code} per {url} (tentativo {attempt + 1})")
        except Exception as e:
            last_err = e
            logger.warning(f"Errore fetch {url}: {e} (tentativo {attempt + 1})")
        time.sleep(2 ** attempt)
    raise Exception(f"Fetch fallito dopo {max_retries} tentativi: {url} ({last_err})")

# ============================================
# DATI STORICI YAHOO FINANCE
# ============================================


def get_stock_data(ticker: str, days: int = 60) -> Optional[Dict]:
    try:
        end = int(datetime.now().timestamp())
        start = int((datetime.now() - timedelta(days=days + 10)).timestamp())
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start}&period2={end}&interval=1d"
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        data = resp.json()
        if not data.get("chart", {}).get("result"):
            return None
        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        quotes = result["indicators"]["quote"][0]
        closes = quotes.get("close", [])
        opens = quotes.get("open", [])
        highs = quotes.get("high", [])
        lows = quotes.get("low", [])
        volumes = quotes.get("volume", [])
        prices, dates, vols, ohlc = [], [], [], []
        for i, (ts, close) in enumerate(zip(timestamps, closes)):
            if close is not None and opens[i] is not None and highs[i] is not None and lows[i] is not None:
                prices.append(close)
                dates.append(datetime.fromtimestamp(ts))
                vols.append(volumes[i] if volumes[i] else 0)
                ohlc.append({"open": opens[i], "high": highs[i], "low": lows[i], "close": close})
        if len(prices) < 5:
            return None
        change = ((prices[-1] - prices[0]) / prices[0]) * 100
        return {
            "ticker": ticker,
            "prices": prices,
            "dates": dates,
            "current": prices[-1],
            "change": change,
            "high": max(prices),
            "low": min(prices),
            "avg": sum(prices) / len(prices),
            "volumes": vols,
            "ohlc": ohlc,
            "company": get_company_name(ticker)
        }
    except Exception as e:
        logger.error(f"Errore dati {ticker}: {e}")
        return None

# ============================================
# INDICATORI TECNICI
# ============================================


def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
    return round(rsi, 1)


def calculate_sma(prices: List[float], period: int) -> Optional[float]:
    if len(prices) < period:
        return None
    return round(float(np.mean(prices[-period:])), 2)


def calculate_bollinger(prices: List[float], period: int = 20, std_dev: int = 2) -> Optional[Dict]:
    if len(prices) < period:
        return None
    sma = np.mean(prices[-period:])
    std = np.std(prices[-period:])
    return {
        "upper": round(float(sma + std_dev * std), 2),
        "middle": round(float(sma), 2),
        "lower": round(float(sma - std_dev * std), 2),
        "bandwidth": round(float((std_dev * std * 2) / sma * 100), 2) if sma else 0.0
    }


def calculate_macd(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Optional[Dict]:
    if len(prices) < slow + signal:
        return None

    def ema(data, period):
        multiplier = 2 / (period + 1)
        ema_values = [np.mean(data[:period])]
        for price in data[period:]:
            ema_values.append((price - ema_values[-1]) * multiplier + ema_values[-1])
        return ema_values

    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    macd_line = [f - s for f, s in zip(ema_fast[-len(ema_slow):], ema_slow)]
    signal_line = ema(macd_line, signal)
    histogram = [m - s for m, s in zip(macd_line[-len(signal_line):], signal_line)]
    return {
        "macd": round(float(macd_line[-1]), 3),
        "signal": round(float(signal_line[-1]), 3),
        "histogram": round(float(histogram[-1]), 3),
        "trend": "BULLISH" if macd_line[-1] > signal_line[-1] else "BEARISH"
    }


def calculate_atr(ohlc: List[Dict], period: int = 14) -> Optional[float]:
    if len(ohlc) < period + 1:
        return None
    tr_values = []
    for i in range(1, len(ohlc)):
        tr1 = ohlc[i]["high"] - ohlc[i]["low"]
        tr2 = abs(ohlc[i]["high"] - ohlc[i - 1]["close"])
        tr3 = abs(ohlc[i]["low"] - ohlc[i - 1]["close"])
        tr_values.append(max(tr1, tr2, tr3))
    return round(float(np.mean(tr_values[-period:])), 2)


def calculate_stochastic(prices: List[float], highs: List[float], lows: List[float],
                          period: int = 14, smooth_k: int = 3, smooth_d: int = 3) -> Optional[Dict]:
    if len(prices) < period + smooth_k + smooth_d:
        return None
    k_values = []
    for i in range(period - 1, len(prices)):
        period_low = min(lows[i - period + 1:i + 1])
        period_high = max(highs[i - period + 1:i + 1])
        if period_high - period_low == 0:
            k_values.append(50.0)
        else:
            k_values.append(100 * (prices[i] - period_low) / (period_high - period_low))
    smoothed_k = [np.mean(k_values[max(0, i - smooth_k + 1):i + 1]) for i in range(len(k_values))]
    smoothed_d = [np.mean(smoothed_k[max(0, i - smooth_d + 1):i + 1]) for i in range(len(smoothed_k))]
    k = round(float(smoothed_k[-1]), 1)
    d = round(float(smoothed_d[-1]), 1)
    if k > 80 and d > 80:
        signal_text = "OVERBOUGHT"
    elif k < 20 and d < 20:
        signal_text = "OVERSOLD"
    elif k > d:
        signal_text = "BULLISH"
    else:
        signal_text = "BEARISH"
    return {"k": k, "d": d, "signal": signal_text}


def analyze_volume_trend(volumes: List[int]) -> str:
    if len(volumes) < 5:
        return "N/D"
    recent_avg = np.mean(volumes[-3:])
    older_avg = np.mean(volumes[-6:-3]) if len(volumes) >= 6 else np.mean(volumes[:3])
    if older_avg == 0:
        return "N/D"
    if recent_avg > older_avg * 1.2:
        return "CRESCENTE"
    elif recent_avg < older_avg * 0.8:
        return "DECRESCENTE"
    return "STABILE"

# ============================================
# CALCOLO LIVELLI TRADING + CONFIDENCE SCORE
# ============================================


def calculate_trading_levels(data: Dict) -> Optional[Dict]:
    if not data:
        return None
    prices = data["prices"]
    current = data["current"]
    ohlc = data.get("ohlc", [])
    volumes = data.get("volumes", [])
    company = data.get("company", data["ticker"])
    tc = CFG.get('technical', {})
    tr = CFG.get('trading', {})

    rsi = calculate_rsi(prices, tc.get('rsi_period', 14))
    sma20 = calculate_sma(prices, tc.get('sma_short', 20))
    sma50 = calculate_sma(prices, tc.get('sma_long', 50))
    bb = calculate_bollinger(prices, tc.get('bb_period', 20), tc.get('bb_std', 2))
    macd = calculate_macd(prices, tc.get('macd_fast', 12), tc.get('macd_slow', 26), tc.get('macd_signal', 9))
    atr = calculate_atr(ohlc, tc.get('atr_period', 14))
    highs = [o["high"] for o in ohlc]
    lows = [o["low"] for o in ohlc]
    stoch = calculate_stochastic(prices, highs, lows, tc.get('stoch_period', 14)) if ohlc else None
    vol_trend = analyze_volume_trend(volumes)

    # --- Sistema di scoring multi-indicatore ---
    bullish_signals = 0
    bearish_signals = 0
    total_signals = 0

    if rsi is not None:
        total_signals += 1
        if rsi < 30:
            bullish_signals += 1
        elif rsi > 70:
            bearish_signals += 1

    if sma20 is not None and sma50 is not None:
        total_signals += 1
        if current > sma20 > sma50:
            bullish_signals += 1
        elif current < sma20 < sma50:
            bearish_signals += 1

    if macd is not None:
        total_signals += 1
        if macd["trend"] == "BULLISH" and macd["histogram"] > 0:
            bullish_signals += 1
        elif macd["trend"] == "BEARISH" and macd["histogram"] < 0:
            bearish_signals += 1

    if bb is not None:
        total_signals += 1
        if current <= bb["lower"]:
            bullish_signals += 1
        elif current >= bb["upper"]:
            bearish_signals += 1

    if stoch is not None:
        total_signals += 1
        if stoch["signal"] == "OVERSOLD":
            bullish_signals += 1
        elif stoch["signal"] == "OVERBOUGHT":
            bearish_signals += 1

    if total_signals == 0:
        return None

    if bullish_signals > bearish_signals:
        position = "LONG"
        strategy_signal = bullish_signals
    elif bearish_signals > bullish_signals:
        position = "SHORT"
        strategy_signal = bearish_signals
    else:
        position = "NEUTRAL"
        strategy_signal = 0

    confidence = int(round((strategy_signal / total_signals) * 100)) if total_signals else 0
    if vol_trend == "CRESCENTE" and position != "NEUTRAL":
        confidence = min(100, confidence + 10)
    elif vol_trend == "DECRESCENTE" and position != "NEUTRAL":
        confidence = max(0, confidence - 5)

    atr_val = atr if atr else round(current * 0.02, 2)
    sl_mult = tr.get('stop_loss_atr_mult', 1.5)
    t1_mult = tr.get('target1_atr_mult', 1.0)
    t2_mult = tr.get('target2_atr_mult', 2.0)
    t3_mult = tr.get('target3_atr_mult', 3.0)

    if position == "SHORT":
        stop_loss = round(current + atr_val * sl_mult, 2)
        target_1 = round(current - atr_val * t1_mult, 2)
        target_2 = round(current - atr_val * t2_mult, 2)
        target_3 = round(current - atr_val * t3_mult, 2)
    else:
        # LONG e NEUTRAL usano la stessa impostazione direzionale rialzista di default
        stop_loss = round(current - atr_val * sl_mult, 2)
        target_1 = round(current + atr_val * t1_mult, 2)
        target_2 = round(current + atr_val * t2_mult, 2)
        target_3 = round(current + atr_val * t3_mult, 2)

    valid_days = tr.get('validity_days', 5)
    valid_until = (datetime.now() + timedelta(days=valid_days)).strftime("%d/%m/%Y")
    strategy = f"{position} - Multi-indicatore ({strategy_signal}/{total_signals} segnali concordi)"

    return {
        "ticker": data["ticker"],
        "company": company,
        "current": current,
        "position": position,
        "confidence": confidence,
        "strategy": strategy,
        "entry": current,
        "target_1": target_1,
        "target_2": target_2,
        "target_3": target_3,
        "stop_loss": stop_loss,
        "valid_until": valid_until,
        "rsi": rsi,
        "sma20": sma20,
        "sma50": sma50,
        "bb": bb,
        "macd": macd,
        "atr": atr_val,
        "stoch": stoch,
        "vol_trend": vol_trend,
    }

# ============================================
# GENERAZIONE GRAFICI
# ============================================


def generate_chart(data: Dict, levels: Optional[Dict]) -> Optional[str]:
    if not CFG.get('charts', {}).get('enabled', True):
        return None
    try:
        prices = data["prices"]
        dates = data["dates"]
        ticker = data["ticker"]
        company = data.get("company", ticker)

        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(10, 7), gridspec_kw={"height_ratios": [3, 1]}, sharex=True
        )

        ax1.plot(dates, prices, color="#1f77b4", linewidth=1.6, label="Prezzo")

        sma20_period = CFG.get('technical', {}).get('sma_short', 20)
        sma50_period = CFG.get('technical', {}).get('sma_long', 50)
        if len(prices) >= sma20_period:
            sma20_series = [np.mean(prices[max(0, i - sma20_period + 1):i + 1]) for i in range(len(prices))]
            ax1.plot(dates, sma20_series, color="#ff7f0e", linewidth=1.0, label=f"SMA{sma20_period}")
        if len(prices) >= sma50_period:
            sma50_series = [np.mean(prices[max(0, i - sma50_period + 1):i + 1]) for i in range(len(prices))]
            ax1.plot(dates, sma50_series, color="#2ca02c", linewidth=1.0, label=f"SMA{sma50_period}")

        if levels:
            ax1.axhline(levels["target_1"], color="green", linestyle="--", linewidth=0.9, alpha=0.7, label="Target 1")
            ax1.axhline(levels["target_2"], color="green", linestyle=":", linewidth=0.9, alpha=0.5, label="Target 2")
            ax1.axhline(levels["stop_loss"], color="red", linestyle="--", linewidth=0.9, alpha=0.7, label="Stop Loss")
            if levels.get("bb"):
                ax1.axhline(levels["bb"]["upper"], color="gray", linestyle=":", linewidth=0.7, alpha=0.5)
                ax1.axhline(levels["bb"]["lower"], color="gray", linestyle=":", linewidth=0.7, alpha=0.5)

        title_conf = f" | Confidence {levels['confidence']}% ({levels['position']})" if levels else ""
        ax1.set_title(f"{company} ({ticker}){title_conf}", fontsize=12, fontweight="bold")
        ax1.set_ylabel("Prezzo (USD)")
        ax1.legend(loc="upper left", fontsize=8)
        ax1.grid(alpha=0.3)

        volumes = data.get("volumes", [])
        if volumes:
            ax2.bar(dates, volumes, color="#7f7f7f", alpha=0.6)
        ax2.set_ylabel("Volume")
        ax2.grid(alpha=0.3)

        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
        fig.autofmt_xdate()
        fig.tight_layout()

        filename = CHARTS_DIR / f"{ticker}_{int(time.time())}.png"
        fig.savefig(filename, dpi=110)
        plt.close(fig)
        return str(filename)
    except Exception as e:
        logger.error(f"Errore generazione grafico per {data.get('ticker')}: {e}")
        return None

# ============================================
# RSS / NEWS
# ============================================


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_rss_entries(url: str, category: str) -> List[Dict]:
    entries = []
    try:
        resp = fetch_with_retry(url, timeout=15, max_retries=2)
        parsed = feedparser.parse(resp.content)
        for entry in parsed.entries:
            title = clean_html(entry.get("title", ""))
            summary = clean_html(entry.get("summary", entry.get("description", "")))
            link = entry.get("link", "")
            if not title:
                continue
            entries.append({
                "title": title,
                "summary": summary,
                "link": link,
                "source": url,
                "category": category,
            })
    except Exception as e:
        logger.warning(f"Impossibile leggere il feed {url}: {e}")
    return entries


def gather_news() -> List[Dict]:
    all_entries = []
    for url in CFG['sources'].get('finance', []):
        all_entries.extend(fetch_rss_entries(url, "finance"))
        watchdog.heartbeat()
    for url in CFG['sources'].get('geopol', []):
        all_entries.extend(fetch_rss_entries(url, "geopol"))
        watchdog.heartbeat()
    logger.info(f"Raccolte {len(all_entries)} notizie totali dai feed RSS")
    return all_entries

# ============================================
# TELEGRAM
# ============================================


def escape_html(text: str) -> str:
    return html_lib.escape(text or "", quote=False)


def send_telegram_message(text: str, max_retries: int = 3) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.warning("Token o Chat ID Telegram non configurati: messaggio non inviato")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text[:4096], "parse_mode": "HTML", "disable_web_page_preview": True}
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, timeout=15)
            if resp.status_code == 200:
                return True
            logger.warning(f"Telegram HTTP {resp.status_code}: {resp.text[:200]} (tentativo {attempt + 1})")
        except Exception as e:
            logger.warning(f"Errore invio Telegram: {e} (tentativo {attempt + 1})")
        time.sleep(2 ** attempt)
    return False


def send_telegram_photo(photo_path: str, caption: str = "", max_retries: int = 3) -> bool:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    for attempt in range(max_retries):
        try:
            with open(photo_path, "rb") as photo_file:
                files = {"photo": photo_file}
                data = {"chat_id": CHAT_ID, "caption": caption[:1024], "parse_mode": "HTML"}
                resp = requests.post(url, data=data, files=files, timeout=30)
            if resp.status_code == 200:
                return True
            logger.warning(f"Telegram photo HTTP {resp.status_code} (tentativo {attempt + 1})")
        except Exception as e:
            logger.warning(f"Errore invio foto Telegram: {e} (tentativo {attempt + 1})")
        time.sleep(2 ** attempt)
    return False

# ============================================
# COSTRUZIONE MESSAGGIO
# ============================================


def build_message(entry: Dict, tickers_levels: List[Dict], sectors: List[str],
                   countries: List[Tuple[str, List[str]]]) -> str:
    cat_label = "📊 FINANZA" if entry["category"] == "finance" else "🌍 GEOPOLITICA"
    now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    lines = [f"<b>{cat_label}</b> — {now_str}", "", f"<b>{escape_html(entry['title'])}</b>"]
    if entry.get("summary"):
        summary_short = entry["summary"][:300]
        lines.append(escape_html(summary_short) + ("…" if len(entry["summary"]) > 300 else ""))
    if entry.get("link"):
        lines.append(f'<a href="{escape_html(entry["link"])}">Fonte completa</a>')

    if sectors:
        lines.append("")
        lines.append(f"<b>Settori coinvolti:</b> {escape_html(', '.join(sectors))}")
    if countries:
        country_names = ", ".join(c[0].title() for c in countries)
        lines.append(f"<b>Paesi/Aree citati:</b> {escape_html(country_names)}")

    if tickers_levels:
        lines.append("")
        lines.append("<b>📈 Analisi tecnica ticker collegati:</b>")
        for lv in tickers_levels:
            emoji = "🟢" if lv["position"] == "LONG" else ("🔴" if lv["position"] == "SHORT" else "⚪")
            lines.append(
                f"\n{emoji} <b>{escape_html(lv['ticker'])}</b> — {escape_html(lv['company'])}\n"
                f"Prezzo attuale: {lv['current']:.2f} | Posizione: {lv['position']} | Confidence: {lv['confidence']}%\n"
                f"Entry: {lv['entry']:.2f} | T1: {lv['target_1']:.2f} | T2: {lv['target_2']:.2f} | T3: {lv['target_3']:.2f} | SL: {lv['stop_loss']:.2f}\n"
                f"RSI: {lv['rsi']} | Volume: {lv['vol_trend']} | Valido fino al {lv['valid_until']}"
            )
    return "\n".join(lines)

# ============================================
# ELABORAZIONE SINGOLA NOTIZIA
# ============================================


def process_news_item(entry: Dict, charts_budget: List[int]) -> Optional[Dict]:
    tickers, keywords, countries = find_tickers_from_news(entry["title"], entry["summary"])
    sectors = classify_sectors(entry["title"], entry["summary"])
    max_tickers = CFG['limits'].get('max_tickers_per_news', 3)

    tickers_levels = []
    chart_paths = []
    for ticker in tickers[:max_tickers]:
        stock_data = get_stock_data(ticker)
        watchdog.heartbeat()
        if not stock_data:
            continue
        levels = calculate_trading_levels(stock_data)
        if not levels:
            continue
        tickers_levels.append(levels)
        if charts_budget[0] > 0:
            chart_path = generate_chart(stock_data, levels)
            if chart_path:
                chart_paths.append((ticker, chart_path))
                charts_budget[0] -= 1

    if not tickers_levels and not sectors:
        return None

    message = build_message(entry, tickers_levels, sectors, countries)
    return {
        "message": message,
        "tickers": [lv["ticker"] for lv in tickers_levels],
        "levels": tickers_levels,
        "charts": chart_paths,
        "title": entry["title"],
    }

# ============================================
# CICLO PRINCIPALE
# ============================================


def run_cycle():
    start_time = time.time()
    watchdog.start()
    news_sent = 0
    charts_sent = 0
    status = "OK"
    error_msg = ""
    try:
        all_entries = gather_news()
        max_news = CFG['limits'].get('max_news_per_cycle', 10)
        charts_budget = [CFG['limits'].get('max_charts_per_cycle', 8)]

        for entry in all_entries:
            if news_sent >= max_news:
                logger.info("Limite massimo di notizie per ciclo raggiunto")
                break
            if db.is_news_sent(entry["title"]):
                continue

            watchdog.heartbeat()
            try:
                result = process_news_item(entry, charts_budget)
            except Exception as e:
                logger.error(f"Errore elaborazione notizia '{entry['title'][:60]}': {e}")
                continue

            if not result:
                db.mark_news_sent(entry["title"], [])
                continue

            sent_ok = send_telegram_message(result["message"])
            if sent_ok:
                for ticker, chart_path in result["charts"]:
                    send_telegram_photo(chart_path, caption=f"Grafico tecnico {ticker}")
                    charts_sent += 1
                for lv in result["levels"]:
                    db.save_prediction(
                        lv["ticker"], lv["company"], lv["strategy"], lv["entry"],
                        lv["target_1"], lv["target_2"], lv["target_3"], lv["stop_loss"],
                        lv["confidence"], lv["position"], lv["valid_until"]
                    )
                db.mark_news_sent(entry["title"], result["tickers"])
                news_sent += 1
                logger.info(f"Notizia inviata: {result['title'][:70]}")
            else:
                logger.warning(f"Invio Telegram fallito per: {entry['title'][:70]}")

        db.cleanup_old_news(days=30)

    except Exception as e:
        status = "ERROR"
        error_msg = str(e)
        logger.error(f"Errore durante il ciclo di esecuzione: {e}", exc_info=True)
    finally:
        watchdog.stop()
        duration = round(time.time() - start_time, 2)
        db.log_execution(status=status, news_count=news_sent, charts_count=charts_sent,
                          error_msg=error_msg, duration_sec=duration)
        logger.info(f"Ciclo completato in {duration}s | Notizie inviate: {news_sent} | Grafici: {charts_sent} | Stato: {status}")


def get_memory_usage_mb() -> float:
    if psutil:
        try:
            process = psutil.Process(os.getpid())
            return round(process.memory_info().rss / (1024 * 1024), 2)
        except Exception:
            return 0.0
    return 0.0


def send_heartbeat(start_time: float, run_count: int):
    if not CFG.get('heartbeat', {}).get('enabled', True):
        return
    interval_runs = CFG['heartbeat'].get('interval_runs', 6)
    if run_count == 0 or run_count % interval_runs != 0:
        return
    mem_mb = get_memory_usage_mb()
    uptime_hours = round((time.time() - start_time) / 3600, 2)
    db.log_heartbeat("ALIVE", mem_mb, uptime_hours)
    logger.info(f"💓 Heartbeat | Uptime: {uptime_hours}h | Memoria: {mem_mb}MB")
    send_telegram_message(
        f"💓 <b>Agent attivo</b>\nUptime: {uptime_hours}h\nMemoria: {mem_mb}MB\nCicli completati: {run_count}"
    )

# ============================================
# GESTIONE ARRESTO E CICLO DI VITA
# ============================================

_stop_event = threading.Event()


def _handle_shutdown(signum, frame):
    logger.info(f"Segnale {signum} ricevuto: arresto in corso...")
    _stop_event.set()


def main():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info("=" * 50)
    logger.info("FINANCE NEWS AGENT v5.0 — avvio")
    logger.info(f"Intervallo esecuzione: {RUN_INTERVAL_HOURS}h")
    logger.info("=" * 50)

    start_time = time.time()
    run_count = 0

    while not _stop_event.is_set():
        try:
            run_cycle()
        except Exception as e:
            logger.error(f"Errore imprevisto nel ciclo principale: {e}", exc_info=True)
            db.log_execution(status="CRASH", error_msg=str(e))

        run_count += 1
        send_heartbeat(start_time, run_count)

        global WATCHDOG_TRIGGERED
        if WATCHDOG_TRIGGERED:
            logger.warning("Riavvio forzato dopo trigger del watchdog")
            WATCHDOG_TRIGGERED = False

        sleep_seconds = RUN_INTERVAL_HOURS * 3600
        logger.info(f"In attesa del prossimo ciclo tra {RUN_INTERVAL_HOURS}h...")
        elapsed = 0
        while elapsed < sleep_seconds and not _stop_event.is_set():
            time.sleep(min(30, sleep_seconds - elapsed))
            elapsed += 30

    logger.info("Agent arrestato correttamente.")


if __name__ == "__main__":
    main()
