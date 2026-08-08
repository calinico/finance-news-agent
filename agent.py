#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================
# FINANCE NEWS AGENT v6.1 — OTTIMIZZATO & PARALLELO
# ============================================
# Ottimizzazioni:
# - ThreadPoolExecutor per download dati parallelo
# - Cache dati Yahoo (evita richieste duplicate)
# - Batch processing notizie
# - Async fetch per feed RSS
# - Connection pooling per requests
# - Pre-fetch dati per ticker comuni

import feedparser
import requests
import os
import json
import re
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
from typing import List, Dict, Tuple, Optional, Set, Any
from functools import wraps, lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

__version__ = "6.1.0"

# ============================================
# SESSIONE HTTP CON CONNECTION POOLING
# ============================================
SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

# ============================================
# CACHE DATI YAHOO (evita richieste duplicate nello stesso ciclo)
# ============================================
class DataCache:
    """Cache thread-safe per dati Yahoo Finance."""
    def __init__(self, ttl_seconds: int = 3600):
        self._cache = {}
        self._timestamps = {}
        self._lock = threading.Lock()
        self.ttl = ttl_seconds

    def get(self, ticker: str) -> Optional[Dict]:
        with self._lock:
            if ticker in self._cache:
                if time.time() - self._timestamps[ticker] < self.ttl:
                    return self._cache[ticker]
                else:
                    del self._cache[ticker]
                    del self._timestamps[ticker]
            return None

    def set(self, ticker: str, data: Dict):
        with self._lock:
            self._cache[ticker] = data
            self._timestamps[ticker] = time.time()

    def clear(self):
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()


data_cache = DataCache(ttl_seconds=3600)

# ============================================
# CONFIGURAZIONE
# ============================================
CONFIG_PATH = Path("config.yaml")

DEFAULT_CONFIG = {
    'telegram': {
        'token': '',
        'chat_id': '',
        'enabled': True,
        'notification_sound': True,  # Suono per notifiche importanti
        'silent_mode': False,  # Se True, nessuna notifica push (solo badge)
        'priority_keywords': ['fed', 'powell', 'war', 'invasion', 'crash', 'rally']
    },
    'run_interval_hours': 4,
    'database': {
        'path': 'agent_data.db',
        'backup_enabled': True,
        'backup_retention_days': 7
    },
    'technical': {
        'rsi_period': 14,
        'sma_short': 20,
        'sma_long': 50,
        'bb_period': 20,
        'bb_std': 2,
        'macd_fast': 12,
        'macd_slow': 26,
        'macd_signal': 9,
        'atr_period': 14,
        'stoch_period': 14,
        'stoch_smooth': 3
    },
    'trading': {
        'stop_loss_long_pct': 0.03,
        'stop_loss_short_pct': 0.05,
        'target_1_pct': 0.03,
        'target_2_pct': 0.05,
        'target_3_pct': 0.10,
        'valid_days': 7,
        'min_confidence': 50,
        'max_positions_per_cycle': 10
    },
    'limits': {
        'max_finance_news': 5,
        'max_geopol_news': 5,
        'max_news_per_source': 2,
        'max_tickers_per_news': 4,
        'max_charts_per_cycle': 10,
        'min_confidence_to_send': 45,
        'max_workers': 5,  # Thread paralleli per download
        'batch_size': 3  # Processa ticker in batch
    },
    'sources': {
        'finance': [
            'https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC,^IXIC,^DJI&region=US&lang=en-US',
            'https://www.marketwatch.com/rss/topstories',
            'https://feeds.a.dj.com/rss/RSSMarketsMain.xml',
            'https://www.investing.com/rss/news.rss',
            'https://seekingalpha.com/feed.xml'
        ],
        'geopol': [
            'https://feeds.bbci.co.uk/news/world/rss.xml',
            'https://rss.cnn.com/rss/edition_world.rss',
            'https://feeds.reuters.com/reuters/hotstories',
            'https://feeds.france24.com/en/news',
            'https://www.aljazeera.com/xml/rss/all.xml'
        ]
    },
    'logging': {
        'level': 'INFO',
        'file': 'agent.log',
        'max_bytes': 10485760,
        'backup_count': 10,
        'json_format': False
    },
    'heartbeat': {
        'enabled': True,
        'interval_runs': 6,
        'max_memory_mb': 512,
        'max_cpu_percent': 80.0
    },
    'network': {
        'timeout': 15,
        'max_retries': 3,
        'retry_delay': 2,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'rate_limit_delay': 0.5  # Ridotto per parallelismo
    },
    'watchdog': {
        'enabled': True,
        'timeout_seconds': 300,
        'check_interval': 30
    },
    'performance': {
        'parallel_download': True,
        'pre_fetch_common_tickers': True,
        'chart_dpi': 100,  # Ridotto per velocità
        'skip_low_confidence_charts': True  # Non genera chart se confidence bassa
    }
}


def load_config() -> dict:
    cfg = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                user_cfg = yaml.safe_load(f) or {}
            def deep_merge(base, override):
                for key, value in override.items():
                    if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                        deep_merge(base[key], value)
                    else:
                        base[key] = value
                return base
            cfg = deep_merge(cfg, user_cfg)
        except Exception as e:
            print(f"WARNING: Errore caricamento config.yaml: {e}")

    cfg['telegram']['token'] = os.getenv('TELEGRAM_TOKEN', cfg['telegram']['token'])
    cfg['telegram']['chat_id'] = os.getenv('TELEGRAM_CHAT_ID', cfg['telegram']['chat_id'])
    cfg['telegram']['enabled'] = os.getenv('TELEGRAM_ENABLED', str(cfg['telegram']['enabled'])).lower() == 'true'

    return cfg


CFG = load_config()
TELEGRAM_TOKEN = CFG['telegram']['token']
CHAT_ID = CFG['telegram']['chat_id']
TELEGRAM_ENABLED = CFG['telegram']['enabled']
DB_PATH = CFG['database']['path']
RUN_INTERVAL_HOURS = CFG['run_interval_hours']
NETWORK_CFG = CFG['network']
PERF_CFG = CFG.get('performance', {})

# ============================================
# LOGGING
# ============================================
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName
        }
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_data, ensure_ascii=False)


def setup_logging() -> logging.Logger:
    log_cfg = CFG.get('logging', {})
    level = getattr(logging, log_cfg.get('level', 'INFO').upper(), logging.INFO)
    log_file = log_cfg.get('file', 'agent.log')
    max_bytes = log_cfg.get('max_bytes', 10485760)
    backup_count = log_cfg.get('backup_count', 10)
    use_json = log_cfg.get('json_format', False)

    logger = logging.getLogger('finance_agent')
    logger.setLevel(level)
    logger.handlers = []

    fh = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
    fh.setLevel(level)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)

    if use_json:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%d/%m/%Y %H:%M:%S'
        )

    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = setup_logging()

# ============================================
# RATE LIMITER
# ============================================
class RateLimiter:
    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.last_call = 0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self.last_call
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_call = time.time()


rate_limiter = RateLimiter(NETWORK_CFG.get('rate_limit_delay', 0.5))

# ============================================
# RETRY DECORATOR
# ============================================
def retry_on_failure(max_retries=None, delay=None, backoff=2.0):
    max_retries = max_retries or NETWORK_CFG.get('max_retries', 3)
    delay = delay or NETWORK_CFG.get('retry_delay', 2)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    wait_time = delay * (backoff ** attempt)
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
            raise last_exception
        return wrapper
    return decorator


# ============================================
# FETCH CON RETRY (usa sessione con pool)
# ============================================
@retry_on_failure()
def fetch_with_retry(url: str, timeout: int = None, **kwargs) -> requests.Response:
    timeout = timeout or NETWORK_CFG.get('timeout', 15)
    headers = kwargs.pop('headers', {})
    headers.setdefault('User-Agent', NETWORK_CFG.get('user_agent', 'Mozilla/5.0'))

    rate_limiter.wait()
    resp = SESSION.get(url, timeout=timeout, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp


# ============================================
# WATCHDOG
# ============================================
class Watchdog:
    def __init__(self, timeout_seconds: int = 300, check_interval: int = 30):
        self.timeout = timeout_seconds
        self.check_interval = check_interval
        self._last_heartbeat = time.time()
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
        self.triggered = False

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="Watchdog")
        self._thread.start()
        logger.info(f"Watchdog avviato (timeout: {self.timeout}s)")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def heartbeat(self):
        with self._lock:
            self._last_heartbeat = time.time()
            self.triggered = False

    def _run(self):
        while self._running:
            time.sleep(self.check_interval)
            with self._lock:
                if time.time() - self._last_heartbeat > self.timeout:
                    logger.error("WATCHDOG TRIGGERED!")
                    self.triggered = True


watchdog = Watchdog(
    timeout_seconds=CFG['watchdog'].get('timeout_seconds', 300),
    check_interval=CFG['watchdog'].get('check_interval', 30)
)

# ============================================
# DATABASE
# ============================================
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
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
                    confidence_reason TEXT,
                    position TEXT,
                    valid_until TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    hit_target INTEGER DEFAULT 0,
                    hit_stop INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'ACTIVE'
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS heartbeat_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT,
                    memory_mb REAL,
                    uptime_hours REAL,
                    cycle_count INTEGER DEFAULT 0
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_sent_hash ON sent_news(title_hash)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_pred_ticker ON predictions(ticker)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_pred_status ON predictions(status)")
            conn.commit()
            logger.info("Database inizializzato")

    def is_news_sent(self, title: str) -> bool:
        h = hashlib.md5(title.lower().strip().encode()).hexdigest()
        with self._lock:
            with self._connect() as conn:
                c = conn.cursor()
                c.execute("SELECT 1 FROM sent_news WHERE title_hash = ?", (h,))
                return c.fetchone() is not None

    def mark_news_sent(self, title: str, tickers: List[str]):
        h = hashlib.md5(title.lower().strip().encode()).hexdigest()
        with self._lock:
            with self._connect() as conn:
                c = conn.cursor()
                c.execute(
                    "INSERT OR IGNORE INTO sent_news (title_hash, title, tickers) VALUES (?, ?, ?)",
                    (h, title, ','.join(tickers))
                )
                conn.commit()

    def cleanup_old_news(self, days: int = 30):
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        with self._lock:
            with self._connect() as conn:
                c = conn.cursor()
                c.execute("DELETE FROM sent_news WHERE sent_at < ?", (cutoff,))
                deleted = c.rowcount
                conn.commit()
                if deleted > 0:
                    logger.info(f"Pulizia DB: rimosse {deleted} notizie vecchie")

    def log_execution(self, status: str, news_count: int = 0, charts_count: int = 0, 
                      error_msg: str = "", duration_sec: float = 0.0):
        with self._lock:
            with self._connect() as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO execution_log (status, news_count, charts_count, error_msg, duration_sec)
                    VALUES (?, ?, ?, ?, ?)
                """, (status, news_count, charts_count, error_msg, duration_sec))
                conn.commit()

    def save_prediction(self, ticker: str, company_name: str, strategy: str, entry: float,
                       target_1: float, target_2: float, target_3: float, stop_loss: float,
                       confidence: int, confidence_reason: str, position: str, valid_until: str):
        with self._lock:
            with self._connect() as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO predictions 
                    (ticker, company_name, strategy, entry_price, target_1, target_2, target_3, 
                     stop_loss, confidence_score, confidence_reason, position, valid_until)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (ticker, company_name, strategy, entry, target_1, target_2, target_3,
                      stop_loss, confidence, confidence_reason, position, valid_until))
                conn.commit()

    def log_heartbeat(self, status: str, memory_mb: float, uptime_hours: float, cycle_count: int = 0):
        with self._lock:
            with self._connect() as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO heartbeat_log (status, memory_mb, uptime_hours, cycle_count)
                    VALUES (?, ?, ?, ?)
                """, (status, memory_mb, uptime_hours, cycle_count))
                conn.commit()

    def get_stats(self) -> Dict:
        with self._lock:
            with self._connect() as conn:
                c = conn.cursor()
                stats = {}
                c.execute("SELECT COUNT(*) FROM sent_news")
                stats['total_news'] = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM predictions WHERE status = 'ACTIVE'")
                stats['active_predictions'] = c.fetchone()[0]
                return stats


db = Database(DB_PATH)


# ============================================
# NOMI AZIENDE (esteso)
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
    "OKTA": "Okta Inc.", "S": "SentinelOne Inc.", "NET": "Cloudflare Inc.",
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
    "TX": "Ternium S.A.", "DJP": "iPath Bloomberg Commodity Index", "DBC": "Invesco DB Commodity Tracking",
    "GSG": "iShares S&P GSCI Commodity-Indexed Trust", "COMT": "iShares GSCI Commodity Dynamic Roll Strategy",
    "USCI": "United States Commodity Index Fund", "GCC": "WisdomTree Continuous Commodity Index",
    "ITA": "iShares U.S. Aerospace & Defense ETF", "VORB": "Virgin Orbit Holdings Inc.",
    "MNTS": "Momentus Inc.", "LUNR": "Intuitive Machines Inc.", "HOOD": "Robinhood Markets Inc.",
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
    "SPGI": "S&P Global Inc.", "AON": "Aon plc", "AJG": "Arthur J. Gallagher & Co.",
    "MMC": "Marsh & McLennan Companies", "MET": "MetLife Inc.", "PRU": "Prudential Financial Inc.",
    "AFL": "Aflac Inc.", "PFG": "Principal Financial Group", "LNC": "Lincoln National Corp.",
    "RJF": "Raymond James Financial", "L": "Loews Corp.", "ALL": "Allstate Corp.",
    "TRV": "Travelers Companies Inc.", "CB": "Chubb Ltd.", "PGR": "Progressive Corp.",
    "CINF": "Cincinnati Financial Corp.", "WRB": "W.R. Berkley Corp.", "AFG": "American Financial Group",
    "Y": "Alleghany Corp.", "RE": "Everest Re Group Ltd.", "RNR": "RenaissanceRe Holdings",
    "AXS": "Axis Capital Holdings", "ACGL": "Arch Capital Group Ltd.", "MKL": "Markel Corp.",
    "BRK-B": "Berkshire Hathaway Inc.", "BRK-A": "Berkshire Hathaway Inc.",
    "ORI": "Old Republic International", "FAF": "First American Financial",
    "FNF": "Fidelity National Financial", "RDN": "Radian Group Inc.", "MTG": "MGIC Investment Corp.",
    "ESNT": "Essent Group Ltd.", "NMIH": "NMI Holdings Inc.",
    "OPEN": "Opendoor Technologies Inc.", "Z": "Zillow Group Inc.", "ZG": "Zillow Group Inc.",
    "EXPI": "eXp World Holdings Inc.", "COMP": "Compass Inc.", "RDFN": "Redfin Corp.",
    "LESL": "Leslie's Inc.", "POOL": "Pool Corp.", "SITE": "SiteOne Landscape Supply",
    "TSCO": "Tractor Supply Co.", "FND": "Floor & Decor Holdings", "LL": "LL Flooring Holdings",
    "SHW": "Sherwin-Williams Co.", "PPG": "PPG Industries Inc.", "AXTA": "Axalta Coating Systems",
    "RPM": "RPM International Inc.", "MAS": "Masco Corp.", "FBHS": "Fortune Brands Home & Security",
    "JELD": "JELD-WEN Holding Inc.", "DOOR": "Masonite International Corp.",
    "APOG": "Apogee Enterprises Inc.", "PGTI": "PGT Innovations Inc.",
    "OC": "Owens Corning", "LPX": "Louisiana-Pacific Corp.", "WY": "Weyerhaeuser Co.",
    "RFP": "Resolute Forest Products", "UFS": "Domtar Corp.", "PKG": "Packaging Corp. of America",
    "IP": "International Paper Co.", "WRK": "WestRock Co.", "SON": "Sonoco Products Co.",
    "GEF": "Greif Inc.", "SLGN": "Silgan Holdings Inc.", "BERY": "Berry Global Group Inc.",
    "AMCR": "Amcor plc", "BALL": "Ball Corp.", "CCK": "Crown Holdings Inc.",
    "OI": "O-I Glass Inc.", "SEE": "Sealed Air Corp.", "AVY": "Avery Dennison Corp.",
    "MMM": "3M Co.", "HON": "Honeywell International Inc.", "TDG": "TransDigm Group Inc.",
    "HEI": "HEICO Corp.", "CW": "Curtiss-Wright Corp.", "AJRD": "Aerojet Rocketdyne Holdings",
    "NPK": "National Presto Industries", "ATRO": "Astronics Corp.", "KAMN": "Kaman Corp.",
    "ESL": "Esterline Technologies Corp.", "COL": "Rockwell Collins Inc.",
    "UTX": "United Technologies Corp.", "TXT": "Textron Inc.", "ERJ": "Embraer S.A.",
    "SAFRF": "Safran S.A.", "ROP": "Roper Technologies Inc.", "GWW": "W.W. Grainger Inc.",
    "FAST": "Fastenal Co.", "MSM": "MSC Industrial Direct Co.", "DKS": "Dick's Sporting Goods Inc.",
    "ASO": "Academy Sports and Outdoors", "BGFV": "Big 5 Sporting Goods Corp.",
    "HIBB": "Hibbett Inc.", "FL": "Foot Locker Inc.", "SCVL": "Shoe Carnival Inc.",
    "BKE": "The Buckle Inc.", "ANF": "Abercrombie & Fitch Co.", "AEO": "American Eagle Outfitters",
    "URBN": "Urban Outfitters Inc.", "GPS": "Gap Inc.", "JWN": "Nordstrom Inc.",
    "M": "Macy's Inc.", "KSS": "Kohl's Corp.", "JCP": "J.C. Penney Co.",
    "SHLDQ": "Sears Holdings Corp.", "BONT": "The Bon-Ton Stores Inc.",
    "DEST": "Destination Maternity Corp.", "CACH": "Cache Inc.",
    "PSUN": "Pacific Sunwear of California", "ZUMZ": "Zumiez Inc.", "TLYS": "Tilly's Inc.",
    "VFC": "VF Corp.", "COLM": "Columbia Sportswear Co.", "DECK": "Deckers Outdoor Corp.",
    "SKX": "Skechers U.S.A. Inc.", "CROX": "Crocs Inc.", "SHOO": "Steven Madden Ltd.",
    "RCKY": "Rocky Brands Inc.", "WEYS": "Weyco Group Inc.", "RGS": "Regis Corp.",
    "EL": "The Estee Lauder Companies", "COTY": "Coty Inc.", "ELF": "e.l.f. Beauty Inc.",
    "REV": "Revlon Inc.", "IPAR": "Inter Parfums Inc.", "LR": "L'Oreal S.A.",
    "KHC": "Kraft Heinz Co.", "GIS": "General Mills Inc.", "CPB": "Campbell Soup Co.",
    "CAG": "Conagra Brands Inc.", "SJM": "J.M. Smucker Co.", "HSY": "The Hershey Co.",
    "MDLZ": "Mondelez International Inc.", "K": "Kellogg Co.", "POST": "Post Holdings Inc.",
    "BGS": "B&G Foods Inc.", "FLO": "Flowers Foods Inc.", "LANC": "Lancaster Colony Corp.",
    "TWNK": "Hostess Brands Inc.", "BIMI": "BIMI International Medical Inc.",
    "THS": "TreeHouse Foods Inc.", "HAIN": "Hain Celestial Group Inc.",
    "UNFI": "United Natural Foods Inc.", "SPTN": "SpartanNash Co.", "ANDE": "The Andersons Inc.",
    "ADM": "Archer-Daniels-Midland Co.", "INGR": "Ingredion Inc.", "BG": "Bunge Ltd.",
    "AGRO": "Adecoagro S.A.", "TSN": "Tyson Foods Inc.", "HRL": "Hormel Foods Corp.",
    "PPC": "Pilgrim's Pride Corp.", "SAFM": "Sanderson Farms Inc.", "SEB": "Seaboard Corp.",
    "CALM": "Cal-Maine Foods Inc.", "PETS": "PetMed Express Inc.", "FRPT": "Freshpet Inc.",
    "CHWY": "Chewy Inc.", "WOOF": "Petco Health and Wellness Co.", "ZTS": "Zoetis Inc.",
    "IDXX": "IDEXX Laboratories Inc.", "MASI": "Masimo Corp.", "RMD": "ResMed Inc.",
    "VAR": "Varian Medical Systems", "EW": "Edwards Lifesciences Corp.", "ABT": "Abbott Laboratories",
    "MDT": "Medtronic plc", "SYK": "Stryker Corp.", "ZBH": "Zimmer Biomet Holdings",
    "BSX": "Boston Scientific Corp.", "DXCM": "Dexcom Inc.", "PODD": "Insulet Corp.",
    "TNDM": "Tandem Diabetes Care Inc.", "ALGN": "Align Technology Inc.",
    "COO": "The Cooper Companies Inc.", "BAX": "Baxter International Inc.",
    "FMS": "Fresenius Medical Care AG", "DVA": "DaVita Inc.", "UHS": "Universal Health Services",
    "CYH": "Community Health Systems", "LPNT": "LifePoint Health Inc.",
    "HCA": "HCA Healthcare Inc.", "THC": "Tenet Healthcare Corp.", "SEM": "Select Medical Holdings",
    "ENSG": "The Ensign Group Inc.", "USPH": "U.S. Physical Therapy Inc.",
    "AMN": "AMN Healthcare Services", "CCRN": "Cross Country Healthcare Inc.",
    "HSII": "Heidrick & Struggles International", "KFY": "Korn Ferry",
    "MAN": "ManpowerGroup Inc.", "RHI": "Robert Half International", "ASGN": "ASGN Inc.",
    "KFRC": "Kforce Inc.", "TBI": "TrueBlue Inc.", "CDK": "CDK Global Inc.",
    "ADP": "Automatic Data Processing", "PAYX": "Paychex Inc.", "PCTY": "Paylocity Holding Corp.",
    "PAYC": "Paycom Software Inc.", "WDAY": "Workday Inc.", "ULTI": "The Ultimate Software Group",
    "CSOD": "Cornerstone OnDemand Inc.", "TLEO": "Taleo Corp.", "SABA": "Saba Software Inc.",
    "KRON": "Kronos Worldwide Inc.", "TWLO": "Twilio Inc.", "FSLY": "Fastly Inc.",
    "ESTC": "Elastic N.V.", "SPLK": "Splunk Inc.", "SUMO": "Sumo Logic Inc.",
    "QLYS": "Qualys Inc.", "TENB": "Tenable Holdings Inc.", "RPD": "Rapid7 Inc.",
    "VRNS": "Varonis Systems Inc.", "NLOK": "NortonLifeLock Inc.", "PFPT": "Proofpoint Inc.",
    "MIME": "Mimecast Ltd.", "FEYE": "FireEye Inc.", "ATEN": "A10 Networks Inc.",
    "RDWR": "Radware Ltd.", "ALLT": "Allot Ltd.", "FFIV": "F5 Inc.",
    "NTCT": "NetScout Systems Inc.", "ARLO": "Arlo Technologies Inc.", "CALX": "Calix Inc.",
    "DZSI": "DZS Inc.", "ADTN": "ADTRAN Holdings Inc.", "CIEN": "Ciena Corp.",
    "INFN": "Infinera Corp.", "IIVI": "II-VI Inc.", "COHR": "Coherent Corp.",
    "NEO": "NeoPhotonics Corp.", "AAOI": "Applied Optoelectronics Inc.", "NPTN": "NeoPhotonics Corp.",
    "OCLR": "Oclaro Inc.", "FNSR": "Finisar Corp.", "VIAV": "Viavi Solutions Inc.",
    "EXFO": "EXFO Inc.", "KEYS": "Keysight Technologies Inc.", "AMKR": "Amkor Technology Inc.",
    "KLIC": "Kulicke & Soffa Industries", "COHU": "Cohu Inc.", "XPER": "Xperi Holding Corp.",
    "FORM": "FormFactor Inc.", "PDFS": "PDF Solutions Inc.", "SNPS": "Synopsys Inc.",
    "CDNS": "Cadence Design Systems", "ANSS": "ANSYS Inc.", "PTC": "PTC Inc.",
    "ADSK": "Autodesk Inc.", "DSGX": "The Descartes Systems Group", "MANH": "Manhattan Associates Inc.",
    "BL": "BlackLine Inc.", "MODN": "Model N Inc.", "PRO": "Pros Holdings Inc.",
    "GUID": "Guidewire Software Inc.", "INST": "Instructure Holdings Inc.", "TWOU": "2U Inc.",
    "CHGG": "Chegg Inc.", "LRN": "Stride Inc.", "LOPE": "Grand Canyon Education Inc.",
    "APEI": "American Public Education Inc.", "STRA": "Strategic Education Inc.",
    "CECO": "Career Education Corp.", "UTI": "Universal Technical Institute",
    "EDMC": "Education Management Corp.", "DV": "DoubleVerify Holdings Inc.",
    "MGNI": "Magnite Inc.", "TBLA": "Taboola.com Ltd.", "PERI": "Perion Network Ltd.",
    "QUOT": "Quotient Technology Inc.", "FLNT": "Fluent Inc.", "CARS": "Cars.com Inc.",
    "TRIP": "TripAdvisor Inc.", "EXPE": "Expedia Group Inc.", "BKNG": "Booking Holdings Inc.",
    "TCOM": "Trip.com Group Ltd.", "MMYT": "MakeMyTrip Ltd.", "DESP": "Despegar.com Corp.",
    "WEB": "Web.com Group Inc.", "GDDY": "GoDaddy Inc.", "WIX": "Wix.com Ltd.",
    "SQSP": "Squarespace Inc.", "BIGC": "BigCommerce Holdings", "VTEX": "VTEX",
    "LSPD": "Lightspeed Commerce Inc.", "TOST": "Toast Inc.", "OLO": "Olo Inc.",
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
        resp = fetch_with_retry(url, timeout=5)
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
# KEYWORDS E SECTOR MAPPING
# ============================================
COUNTRY_ASSETS = {
    "united states": ["SPY", "QQQ", "DIA", "XLF", "TLT", "GLD", "USO"],
    "usa": ["SPY", "QQQ", "DIA", "XLF", "TLT", "GLD", "USO"],
    "fed": ["SPY", "QQQ", "XLF", "TLT", "KRE", "JPM", "BAC", "BLK"],
    "powell": ["SPY", "QQQ", "XLF", "TLT", "KRE"],
    "europe": ["VGK", "EZU", "EWG", "EWQ", "EWI", "FEZ"],
    "ecb": ["VGK", "EZU", "EWG", "EWQ", "DB", "UBS", "SAN"],
    "lagarde": ["VGK", "EZU", "EWG", "EWQ"],
    "germany": ["EWG", "VGK", "EZU", "SAP", "SIE", "BMWYY", "VWAGY"],
    "france": ["EWQ", "VGK", "EZU", "TOT", "OR", "SAN", "AIR"],
    "italy": ["EWI", "VGK", "EZU", "ENI", "UCG", "ISP", "LUX"],
    "spain": ["EWP", "VGK", "EZU", "SAN", "BBVA", "ITX", "TEF"],
    "uk": ["EWU", "VGK", "EZU", "HSBC", "BP", "SHEL", "AZN", "UL"],
    "britain": ["EWU", "VGK", "HSBC", "BP", "SHEL", "AZN"],
    "boe": ["EWU", "VGK", "HSBC", "BP", "SHEL"],
    "bailey": ["EWU", "VGK", "HSBC", "BP"],
    "china": ["FXI", "MCHI", "KWEB", "ASHR", "BABA", "TCEHY", "JD", "PDD"],
    "taiwan": ["EWT", "FXI", "TSM", "UMC", "ASML"],
    "japan": ["EWJ", "DXJ", "HEWJ", "TM", "HMC", "SONY", "NTDOY", "SNE"],
    "india": ["INDA", "EPI", "MINDX", "INFY", "TCS", "WIT", "HDB"],
    "south korea": ["EWY", "SKM", "KB", "KEP", "POSCO", "LPL"],
    "australia": ["EWA", "BHP", "RIO", "WPL", "NAB", "WBC", "ANZ"],
    "israel": ["ISRA", "EIS", "TEVA", "ICL", "CHKP", "CYBR"],
    "iran": ["USO", "UCO", "BNO", "OIL", "XLE", "CVX", "XOM"],
    "saudi arabia": ["KSA", "USO", "XLE", "CVX", "XOM"],
    "uae": ["UAE", "USO", "XLE"],
    "qatar": ["QAT", "USO", "XLE"],
    "russia": ["RSX", "ERUS", "USO", "GLD", "UNG", "WEAT", "CORN", "SOYB"],
    "ukraine": ["USO", "UNG", "WEAT", "CORN", "SOYB", "GLD", "RSX"],
    "brazil": ["EWZ", "BRZU", "PBR", "VALE", "ITUB", "BBD"],
    "mexico": ["EWW", "FMX", "AMX", "CEMEX", "GMEXIC"],
    "argentina": ["ARGT", "GGAL", "YPF", "PAM", "TEO"],
    "gold": ["GLD", "IAU", "PHYS", "GOLD", "NEM", "AEM", "KGC"],
    "oil": ["USO", "UCO", "BNO", "XLE", "XOM", "CVX", "COP", "OXY"],
    "natural gas": ["UNG", "BOIL", "KOLD", "KBR", "SWN", "EQT"],
    "wheat": ["WEAT", "CORN", "SOYB", "DBA", "TEUC", "ADM"],
    "corn": ["CORN", "WEAT", "SOYB", "DBA", "ADM", "INGR"],
    "bitcoin": ["MSTR", "COIN", "HOOD", "BITO", "BITW", "GBTC", "RIOT", "MARA"],
    "ethereum": ["COIN", "HOOD", "BITW", "ETHE", "RIOT", "MARA", "HIVE"],
}

KEYWORD_TICKERS = {
    "apple": ["AAPL", "AVGO", "LITE", "QRVO", "SWKS"],
    "iphone": ["AAPL", "LGL", "CRUS", "STM"],
    "microsoft": ["MSFT", "QLYS", "VEEV", "DOCU"],
    "google": ["GOOGL", "GOOG", "TDC", "TRMB"],
    "meta": ["META", "SNAP", "PINS", "MTCH"],
    "nvidia": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "SWKS"],
    "ai": ["NVDA", "AMD", "PLTR", "AI", "SNOW", "MDB", "DDOG", "NET"],
    "artificial intelligence": ["NVDA", "AMD", "PLTR", "AI", "SNOW", "MDB"],
    "chip": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "SWKS", "QRVO", "MPWR"],
    "semiconductor": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "AMAT", "LRCX"],
    "cloud": ["MSFT", "AMZN", "GOOGL", "CRM", "NOW", "SNOW", "DDOG", "MDB"],
    "cybersecurity": ["CRWD", "PANW", "FTNT", "ZS", "OKTA", "CYBR", "S", "NET"],
    "data center": ["NVDA", "AMD", "INTC", "SMCI", "DELL", "HPE", "ANET"],
    "bank": ["JPM", "BAC", "WFC", "C", "GS", "MS", "PNC", "USB", "TFC", "RF"],
    "banca": ["JPM", "BAC", "WFC", "C", "GS", "MS"],
    "credit": ["JPM", "BAC", "WFC", "C", "DFS", "COF", "SYF", "ALLY"],
    "mortgage": ["RKT", "UWMC", "LDI", "PFSI", "COOP"],
    "oil": ["XOM", "CVX", "COP", "EOG", "MPC", "VLO", "PSX", "MRO", "OXY", "DVN"],
    "petrolio": ["XOM", "CVX", "COP", "EOG", "MPC", "VLO", "OXY"],
    "gas": ["XOM", "CVX", "COP", "EOG", "MRO", "DVN", "EQT", "RRC", "SWN"],
    "energy": ["XOM", "CVX", "COP", "EOG", "XLE", "OXY", "DVN", "MRO", "FANG"],
    "renewable": ["ENPH", "SEDG", "FSLR", "RUN", "NOVA", "SPWR", "CSIQ", "JKS"],
    "solar": ["ENPH", "SEDG", "FSLR", "RUN", "NOVA", "SPWR", "CSIQ", "JKS"],
    "wind": ["GE", "VWDRY", "NPI", "BEP", "NEE", "ORA", "TPIC"],
    "pharma": ["JNJ", "PFE", "MRK", "ABBV", "BMY", "LLY", "NVO", "AZN", "GILD", "BIIB"],
    "drug": ["JNJ", "PFE", "MRK", "ABBV", "BMY", "LLY", "NVO", "AZN", "GILD", "VRTX"],
    "vaccine": ["PFE", "MRNA", "BNTX", "NVAX", "GSK", "SNY", "JNJ"],
    "biotech": ["AMGN", "GILD", "BIIB", "VRTX", "REGN", "ALNY", "SRPT", "BMRN"],
    "fda": ["JNJ", "PFE", "MRK", "ABBV", "BMY", "LLY", "VRTX", "REGN", "ALNY"],
    "clinical trial": ["BIIB", "VRTX", "REGN", "ALNY", "SRPT", "BMRN", "IONS", "EXEL"],
    "tesla": ["TSLA", "RIVN", "LCID", "FSR", "NIO", "XPEV", "LI", "BYDDF"],
    "ev": ["TSLA", "RIVN", "LCID", "FSR", "NIO", "XPEV", "LI", "QS", "MP"],
    "electric vehicle": ["TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI", "QS", "MP"],
    "automaker": ["F", "GM", "STLA", "TM", "HMC", "HYMTF", "VWAGY", "BMWYY"],
    "car": ["F", "GM", "STLA", "TM", "HMC", "VWAGY", "BMWYY", "RACE"],
    "battery": ["TSLA", "QS", "MP", "ALB", "SQM", "LTHM", "PLL", "LAC"],
    "bitcoin": ["MSTR", "COIN", "HOOD", "BITO", "BITW", "GBTC", "RIOT", "MARA"],
    "ethereum": ["COIN", "HOOD", "BITW", "ETHE", "RIOT", "MARA", "HIVE", "HUT"],
    "crypto": ["MSTR", "COIN", "HOOD", "BITO", "RIOT", "MARA", "HIVE", "HUT", "BITF"],
    "blockchain": ["IBM", "COIN", "MSTR", "RIOT", "MARA", "SQ", "PYPL"],
    "real estate": ["VNQ", "SPG", "O", "AMT", "PLD", "WPC", "NNN", "STAG"],
    "housing": ["DHI", "LEN", "PHM", "TOL", "NVR", "KBH", "MTH", "TMHC", "TPH"],
    "construction": ["DHI", "LEN", "PHM", "TOL", "CAT", "DE", "URI"],
    "gold": ["GLD", "IAU", "PHYS", "GOLD", "NEM", "AEM", "KGC", "WPM", "RGLD", "FNV"],
    "silver": ["SLV", "PAAS", "HL", "CDE", "EXK", "MAG", "FSM", "SVM"],
    "copper": ["FCX", "SCCO", "TECK", "VALE", "RIO", "BHP", "GLNCY", "ANTO"],
    "commodity": ["PDBC", "USCI", "GCC", "DJP", "DBC", "GSG", "COMT"],
    "steel": ["NUE", "STLD", "MT", "VALE", "RIO", "BHP", "CLF", "TX"],
    "amazon": ["AMZN", "SHOP", "ETSY", "EBAY", "W", "OSTK", "CVNA"],
    "retail": ["WMT", "TGT", "COST", "HD", "LOW", "BBY", "TJX", "ROST", "BURL"],
    "consumer": ["PG", "KO", "PEP", "WMT", "COST", "MCD", "SBUX", "DPZ", "YUM"],
    "defense": ["LMT", "NOC", "RTX", "GD", "BA", "HII", "KTOS", "BWXT"],
    "aerospace": ["BA", "AIR", "SAFRF", "GE", "HON", "RTX", "LMT", "NOC"],
    "infrastructure": ["CAT", "DE", "URI", "PCAR", "VMI", "MLI", "TREX", "AWP"],
    "telecom": ["T", "VZ", "TMUS", "CMCSA", "CHTR", "LUMN", "FYBR", "CNSL"],
    "streaming": ["NFLX", "DIS", "WBD", "PARA", "ROKU", "FUBO", "AMC", "CNK"],
    "ozempic": ["NVO", "LLY", "PFE", "MRK", "ABBV", "BMY"],
    "wegovy": ["NVO", "LLY"],
    "weight loss": ["NVO", "LLY", "PFE", "MRK"],
    "diabetes": ["NVO", "LLY", "PFE", "MRK", "JNJ"],
    "spacex": ["RKLB", "ASTS", "SPCE", "LUNR", "VORB", "MNTS"],
    "space": ["RKLB", "ASTS", "SPCE", "LUNR", "VORB", "BA", "LMT"],
    "paramount": ["PARA", "WBD", "DIS", "NFLX", "CMCSA", "FOX"],
    "warner": ["WBD", "PARA", "DIS", "NFLX", "CMCSA", "FOX"],
    "mercedes": ["MBGYY", "VWAGY", "BMWYY", "TM", "HMC", "STLA", "F", "GM"],
    "vat": ["SPY", "QQQ", "DIA", "XLU", "NEE", "DUK", "SO", "AEP"],
    "electricity": ["XLU", "NEE", "DUK", "SO", "AEP", "EXC", "SRE", "ED"],
    "utility": ["XLU", "NEE", "DUK", "SO", "AEP", "EXC", "SRE", "ED"],
}

SECTOR_ETFS = {
    "Tech": ["XLK", "VGT", "SMH", "SOXX", "IGV"],
    "Banche/Finanza": ["XLF", "VFH", "KRE", "KBE", "IYF"],
    "Energia": ["XLE", "VDE", "FENY", "OIH", "XOP"],
    "Farmaceutica/Biotech": ["XBI", "IBB", "VHT", "XLV", "IHI"],
    "Auto/Elettrici": ["DRIV", "IDRV", "LIT", "BATT", "CARZ"],
    "Crypto": ["BITO", "BITW", "WGMI", "BKCH"],
    "Immobiliare": ["VNQ", "SCHH", "USRT", "REET", "FREL"],
    "Materie Prime": ["PDBC", "USCI", "GCC", "GSG", "COMT"],
    "Indici Globali": ["SPY", "QQQ", "IWM", "DIA", "VTI", "VEU"],
    "Geopolitica/Safe Haven": ["GLD", "IAU", "TLT", "IEF", "VIXY", "SQQQ"],
    "Utility/Energia": ["XLU", "NEE", "DUK", "SO", "AEP"],
    "Media/Entertainment": ["XLC", "PARA", "WBD", "DIS", "NFLX"],
    "Space/Aerospace": ["ITA", "BA", "LMT", "NOC", "RTX", "RKLB"],
}

SECTOR_KEYWORDS = {
    "Tech": ["apple", "microsoft", "google", "meta", "nvidia", "ai", "artificial intelligence", "chip", "semiconductor", "cloud", "cybersecurity", "data center", "software", "hardware"],
    "Banche/Finanza": ["bank", "banca", "fed", "ecb", "interest rate", "tasso", "banche", "credit", "loan", "mortgage", "central bank", "financial"],
    "Energia": ["oil", "petrolio", "gas", "energy", "renewable", "solar", "wind", "opec", "electricity", "utility", "vat", "power"],
    "Farmaceutica/Biotech": ["pharma", "drug", "vaccine", "biotech", "fda", "clinical trial", "medicine", "healthcare", "ozempic", "wegovy", "weight loss", "diabetes"],
    "Auto/Elettrici": ["tesla", "ev", "electric vehicle", "automaker", "car", "battery", "mercedes", "auto"],
    "Crypto": ["bitcoin", "ethereum", "crypto", "blockchain"],
    "Immobiliare": ["real estate", "housing", "property", "mortgage", "construction"],
    "Materie Prime": ["gold", "oro", "silver", "copper", "commodity", "steel"],
    "Indici Globali": ["sp500", "nasdaq", "dow", "ftse", "dax", "nikkei"],
    "Geopolitica/Safe Haven": ["war", "conflict", "sanctions", "tension", "missile", "attack", "invasion", "peace", "treaty", "diplomatic"],
    "Utility/Energia": ["electricity", "utility", "vat", "power", "grid"],
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
# DATI STORICI YAHOO FINANCE — CON CACHE
# ============================================
def get_stock_data(ticker: str, days: int = 30) -> Optional[Dict]:
    """Scarica dati storici da Yahoo Finance con cache."""
    # Controlla cache prima
    cached = data_cache.get(ticker)
    if cached is not None:
        logger.debug(f"Cache hit per {ticker}")
        return cached

    try:
        end = int(datetime.now().timestamp())
        start = int((datetime.now() - timedelta(days=days + 5)).timestamp())
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start}&period2={end}&interval=1d"

        resp = fetch_with_retry(url, timeout=10)
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

        prices = []
        dates = []
        vols = []
        ohlc = []

        for i, (ts, close) in enumerate(zip(timestamps, closes)):
            if close is not None and opens[i] is not None and highs[i] is not None and lows[i] is not None:
                prices.append(close)
                dates.append(datetime.fromtimestamp(ts).strftime("%d/%m"))
                vols.append(volumes[i] if volumes[i] else 0)
                ohlc.append({"open": opens[i], "high": highs[i], "low": lows[i], "close": close})

        if len(prices) < 5:
            return None

        change = ((prices[-1] - prices[0]) / prices[0]) * 100

        result_data = {
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

        # Salva in cache
        data_cache.set(ticker, result_data)
        return result_data

    except Exception as e:
        logger.error(f"Errore dati {ticker}: {e}")
        return None


def get_stock_data_batch(tickers: List[str], days: int = 30, max_workers: int = 5) -> Dict[str, Optional[Dict]]:
    """Scarica dati per molteplici ticker in parallelo usando ThreadPool.

    Questa e la funzione CHIAVE per l'ottimizzazione della velocita.
    """
    results = {}

    # Filtra ticker gia in cache
    tickers_to_fetch = []
    for t in tickers:
        cached = data_cache.get(t)
        if cached is not None:
            results[t] = cached
        else:
            tickers_to_fetch.append(t)

    if not tickers_to_fetch:
        return results

    logger.info(f"Download parallelo per {len(tickers_to_fetch)} ticker con {max_workers} workers...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(get_stock_data, t, days): t for t in tickers_to_fetch}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                data = future.result()
                results[ticker] = data
            except Exception as e:
                logger.error(f"Errore download parallelo per {ticker}: {e}")
                results[ticker] = None

    elapsed = time.time() - start_time
    logger.info(f"Download batch completato in {elapsed:.1f}s")
    return results


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
        return 100.0
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
    return round(np.mean(prices[-period:]), 2)


def calculate_bollinger(prices: List[float], period: int = 20, std_dev: int = 2) -> Optional[Dict]:
    if len(prices) < period:
        return None
    sma = np.mean(prices[-period:])
    std = np.std(prices[-period:])
    return {
        "upper": round(sma + std_dev * std, 2),
        "middle": round(sma, 2),
        "lower": round(sma - std_dev * std, 2),
        "bandwidth": round((std_dev * std * 2) / sma * 100, 2) if sma else 0
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
        "macd": round(macd_line[-1], 3),
        "signal": round(signal_line[-1], 3),
        "histogram": round(histogram[-1], 3),
        "trend": "BULLISH" if macd_line[-1] > signal_line[-1] else "BEARISH"
    }


def calculate_atr(ohlc: List[Dict], period: int = 14) -> Optional[float]:
    if len(ohlc) < period + 1:
        return None
    tr_values = []
    for i in range(1, len(ohlc)):
        tr1 = ohlc[i]["high"] - ohlc[i]["low"]
        tr2 = abs(ohlc[i]["high"] - ohlc[i-1]["close"])
        tr3 = abs(ohlc[i]["low"] - ohlc[i-1]["close"])
        tr_values.append(max(tr1, tr2, tr3))
    return round(np.mean(tr_values[-period:]), 2)


def calculate_stochastic(prices: List[float], highs: List[float], lows: List[float], 
                         period: int = 14, smooth_k: int = 3, smooth_d: int = 3) -> Optional[Dict]:
    """Calcola Stochastic Oscillator."""
    if len(prices) < period + smooth_k + smooth_d:
        return None

    k_values = []
    for i in range(period - 1, len(prices)):
        period_high = max(highs[i - period + 1:i + 1])
        period_low = min(lows[i - period + 1:i + 1])
        if period_high == period_low:
            k_values.append(50.0)
        else:
            k_values.append(((prices[i] - period_low) / (period_high - period_low)) * 100)

    smoothed_k = [np.mean(k_values[max(0, i - smooth_k + 1):i + 1]) for i in range(len(k_values))]
    d_values = [np.mean(smoothed_k[max(0, i - smooth_d + 1):i + 1]) for i in range(len(smoothed_k))]

    current_k = round(smoothed_k[-1], 1)
    current_d = round(d_values[-1], 1)

    if current_k < 20 and current_d < 20 and current_k > current_d:
        signal_type = "BUY"
    elif current_k > 80 and current_d > 80 and current_k < current_d:
        signal_type = "SELL"
    else:
        signal_type = "NEUTRAL"

    return {"k": current_k, "d": current_d, "signal": signal_type, "oversold": current_k < 20, "overbought": current_k > 80}


def analyze_volume_trend(volumes: List[int]) -> str:
    if len(volumes) < 5:
        return "N/D"
    recent_avg = np.mean(volumes[-3:])
    older_avg = np.mean(volumes[-6:-3]) if len(volumes) >= 6 else np.mean(volumes[:3])
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
    vol_trend = analyze_volume_trend(volumes)

    highs = [c["high"] for c in ohlc] if ohlc else prices
    lows = [c["low"] for c in ohlc] if ohlc else prices
    stoch = calculate_stochastic(prices, highs, lows, tc.get('stoch_period', 14), tc.get('stoch_smooth', 3))

    trend_signals = []
    if sma20 and sma50:
        trend_signals.append("bullish_ma" if sma20 > sma50 else "bearish_ma")
    if macd:
        trend_signals.append(macd["trend"].lower())
    if rsi is not None:
        if rsi > 60:
            trend_signals.append("rsi_bullish")
        elif rsi < 40:
            trend_signals.append("rsi_bearish")

    bullish_count = sum(1 for s in trend_signals if "bullish" in s)
    bearish_count = sum(1 for s in trend_signals if "bearish" in s)

    if bullish_count > bearish_count:
        position = "LONG"
        stop_pct = tr.get('stop_loss_long_pct', 0.03)
        entry = current
        if sma20 and current > sma20:
            entry = round(sma20 * 1.005, 2)
    elif bearish_count > bullish_count:
        position = "SHORT"
        stop_pct = tr.get('stop_loss_short_pct', 0.05)
        entry = current
        if sma20 and current < sma20:
            entry = round(sma20 * 0.995, 2)
    else:
        position = "NEUTRAL"
        stop_pct = tr.get('stop_loss_long_pct', 0.03)
        entry = current

    if atr:
        atr_stop = round(current - (atr * 2), 2) if position == "LONG" else round(current + (atr * 2), 2)
        pct_stop = round(current * stop_pct, 2)
        stop_loss = max(atr_stop, current - pct_stop) if position == "LONG" else min(atr_stop, current + pct_stop)
    else:
        stop_loss = round(current * (1 - stop_pct), 2) if position == "LONG" else round(current * (1 + stop_pct), 2)

    risk = abs(entry - stop_loss)
    t1_pct = tr.get('target_1_pct', 0.03)
    t2_pct = tr.get('target_2_pct', 0.05)
    t3_pct = tr.get('target_3_pct', 0.10)

    target_1 = round(entry * (1 + t1_pct), 2) if position == "LONG" else round(entry * (1 - t1_pct), 2)
    target_2 = round(entry * (1 + t2_pct), 2) if position == "LONG" else round(entry * (1 - t2_pct), 2)
    target_3 = round(entry * (1 + t3_pct), 2) if position == "LONG" else round(entry * (1 - t3_pct), 2)

    risk_reward_1 = round(abs(target_1 - entry) / risk, 2) if risk > 0 else 0
    risk_reward_2 = round(abs(target_2 - entry) / risk, 2) if risk > 0 else 0
    risk_reward_3 = round(abs(target_3 - entry) / risk, 2) if risk > 0 else 0

    # CONFIDENCE SCORE
    confidence = 50
    reasons = []

    if position == "LONG" and bullish_count >= 2:
        confidence += 15
        reasons.append(f"Trend rialzista confermato ({bullish_count}/{len(trend_signals)} segnali)")
    elif position == "SHORT" and bearish_count >= 2:
        confidence += 15
        reasons.append(f"Trend ribassista confermato ({bearish_count}/{len(trend_signals)} segnali)")
    else:
        confidence -= 10
        reasons.append("Trend non chiaro o in conflitto")

    if rsi is not None:
        if position == "LONG" and 40 < rsi < 70:
            confidence += 10
            reasons.append(f"RSI a {rsi} - momentum favorevole")
        elif position == "SHORT" and 30 < rsi < 60:
            confidence += 10
            reasons.append(f"RSI a {rsi} - momentum favorevole")
        elif (position == "LONG" and rsi > 75) or (position == "SHORT" and rsi < 25):
            confidence -= 15
            reasons.append(f"RSI estremo ({rsi}) - possibile inversione")

    if bb:
        if position == "LONG" and current < bb["lower"] * 1.02:
            confidence += 10
            reasons.append("Prezzo vicino banda inferiore Bollinger")
        elif position == "SHORT" and current > bb["upper"] * 0.98:
            confidence += 10
            reasons.append("Prezzo vicino banda superiore Bollinger")

    if vol_trend == "CRESCENTE":
        confidence += 10
        reasons.append("Volume in crescita")
    elif vol_trend == "DECRESCENTE":
        confidence -= 5
        reasons.append("Volume in calo")

    if macd and abs(macd["histogram"]) > 0.5:
        confidence += 5
        reasons.append("MACD con momentum significativo")

    if risk_reward_1 >= 1.5:
        confidence += 10
        reasons.append(f"R/R favorevole: {risk_reward_1}:1")
    elif risk_reward_1 < 1.0:
        confidence -= 10
        reasons.append(f"R/R sfavorevole: {risk_reward_1}:1")

    if stoch:
        if position == "LONG" and stoch["k"] < 30:
            confidence += 5
            reasons.append("Stochastic in zona ipervenduto")
        elif position == "SHORT" and stoch["k"] > 70:
            confidence += 5
            reasons.append("Stochastic in zona ipercomprato")

    confidence = max(0, min(100, confidence))

    valid_days = tr.get('valid_days', 7)
    hold_days = valid_days + 7 if confidence >= 80 else valid_days if confidence >= 60 else max(3, valid_days - 2)
    valid_until = (datetime.now() + timedelta(days=hold_days)).strftime("%d/%m/%Y")

    if confidence >= 80:
        entry_safety = "ALTA - Entrata consigliata con sizing standard"
    elif confidence >= 65:
        entry_safety = "MEDIA-BUONA - Entrata possibile con sizing ridotto (50-70%)"
    elif confidence >= 50:
        entry_safety = "MEDIA - Attendere conferma o entrare con sizing minimo (25-30%)"
    else:
        entry_safety = "BASSA - Evitare entrata o usare solo paper trading"

    return {
        "ticker": data["ticker"],
        "company": company,
        "position": position,
        "entry": round(entry, 2),
        "target_1": target_1,
        "target_2": target_2,
        "target_3": target_3,
        "stop_loss": round(stop_loss, 2),
        "risk_reward_1": risk_reward_1,
        "risk_reward_2": risk_reward_2,
        "risk_reward_3": risk_reward_3,
        "confidence": confidence,
        "confidence_reasons": reasons,
        "entry_safety": entry_safety,
        "valid_until": valid_until,
        "hold_days": hold_days,
        "indicators": {
            "rsi": rsi,
            "sma20": sma20,
            "sma50": sma50,
            "bb": bb,
            "macd": macd,
            "atr": atr,
            "volume_trend": vol_trend,
            "stochastic": stoch
        }
    }


# ============================================
# GRAFICI AVANZATI — OTTIMIZZATI
# ============================================
def create_advanced_chart(data: Dict, levels: Dict, output_path: str = None) -> str:
    ticker = data["ticker"]
    company = data.get("company", ticker)
    prices = data["prices"]
    dates = data["dates"]
    volumes = data.get("volumes", [])

    if output_path is None:
        output_path = f"chart_{ticker}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"

    dpi = PERF_CFG.get('chart_dpi', 100)
    fig, axes = plt.subplots(3, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1, 1]})
    fig.suptitle(f"{ticker} - {company}\nAnalisi del {datetime.now().strftime('%d/%m/%Y %H:%M')}", 
                 fontsize=12, fontweight='bold')

    x_pos = range(len(prices))

    # Prezzo + Livelli
    ax1 = axes[0]
    ax1.plot(x_pos, prices, label=f"{ticker}", color='#1f77b4', linewidth=1.2)

    sma20_vals = [np.mean(prices[max(0, i-20):i]) for i in range(1, len(prices)+1)]
    sma50_vals = [np.mean(prices[max(0, i-50):i]) for i in range(1, len(prices)+1)]
    ax1.plot(x_pos, sma20_vals, '--', color='orange', alpha=0.6, label='SMA 20')
    ax1.plot(x_pos, sma50_vals, '--', color='purple', alpha=0.6, label='SMA 50')

    if levels:
        entry = levels["entry"]
        stop = levels["stop_loss"]
        t1, t2, t3 = levels["target_1"], levels["target_2"], levels["target_3"]
        ax1.axhline(y=entry, color='blue', linestyle='-', alpha=0.7, label=f'Entry: ${entry}')
        ax1.axhline(y=stop, color='red', linestyle='-', alpha=0.7, label=f'Stop: ${stop}')
        ax1.axhline(y=t1, color='green', linestyle=':', alpha=0.6, label=f'T1: ${t1}')
        ax1.axhline(y=t2, color='green', linestyle='--', alpha=0.6, label=f'T2: ${t2}')
        ax1.axhline(y=t3, color='green', linestyle='-', alpha=0.6, label=f'T3: ${t3}')

    ax1.set_ylabel("Prezzo ($)")
    ax1.legend(loc='upper left', fontsize=6)
    ax1.grid(True, alpha=0.3)
    step = max(1, len(dates) // 6)
    ax1.set_xticks(x_pos[::step])
    ax1.set_xticklabels(dates[::step], rotation=45, ha='right', fontsize=6)

    # Volume
    ax2 = axes[1]
    colors_vol = ['green' if prices[i] >= prices[i-1] else 'red' for i in range(1, len(prices))]
    colors_vol = ['gray'] + colors_vol
    ax2.bar(x_pos, volumes, color=colors_vol, alpha=0.5, width=0.8)
    ax2.set_ylabel("Volume")
    ax2.set_xticks(x_pos[::step])
    ax2.set_xticklabels(dates[::step], rotation=45, ha='right', fontsize=6)
    ax2.grid(True, alpha=0.3)

    # Info
    ax3 = axes[2]
    ax3.axis('off')
    if levels:
        info_lines = [
            f"{ticker} - {company}",
            f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')} | Valida fino: {levels['valid_until']}",
            f"Pos: {levels['position']} | Entry: ${levels['entry']} | Stop: ${levels['stop_loss']}",
            f"T1: ${levels['target_1']} | T2: ${levels['target_2']} | T3: ${levels['target_3']}",
            f"Confidence: {levels['confidence']}/100 | {levels.get('entry_safety', '')}",
        ]
        y_start = 0.9
        for line in info_lines:
            ax3.text(0.02, y_start, line, transform=ax3.transAxes, fontsize=8, 
                    verticalalignment='top', fontfamily='monospace')
            y_start -= 0.18

    plt.tight_layout()
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight', facecolor='white')
    plt.close()
    logger.info(f"Grafico salvato: {output_path}")
    return output_path


# ============================================
# TELEGRAM — CON GESTIONE NOTIFICHE
# ============================================
def send_telegram_message(text: str, chat_id: str = None, max_retries: int = 3, 
                          disable_notification: bool = False) -> bool:
    """Invia messaggio Telegram. 

    disable_notification=True = nessun suono/vibrazione (solo badge)
    disable_notification=False = notifica normale con suono
    """
    if not TELEGRAM_ENABLED:
        logger.info(f"[TELEGRAM DISABLED] {text[:100]}...")
        return False

    chat_id = chat_id or CHAT_ID
    if not TELEGRAM_TOKEN or not chat_id:
        logger.warning("Telegram non configurato")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
        "disable_notification": disable_notification
    }

    for attempt in range(max_retries):
        try:
            resp = SESSION.post(url, json=payload, timeout=15)
            if resp.status_code == 200:
                return True
            logger.warning(f"Telegram HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.warning(f"Telegram errore (tentativo {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    return False


def send_telegram_photo(photo_path: str, caption: str = "", chat_id: str = None,
                        disable_notification: bool = False) -> bool:
    """Invia foto Telegram."""
    if not TELEGRAM_ENABLED:
        logger.info(f"[TELEGRAM DISABLED] Photo: {photo_path}")
        return False

    chat_id = chat_id or CHAT_ID
    if not TELEGRAM_TOKEN or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    for attempt in range(3):
        try:
            with open(photo_path, 'rb') as f:
                files = {'photo': f}
                data = {
                    'chat_id': chat_id, 
                    'caption': caption, 
                    'parse_mode': 'Markdown',
                    'disable_notification': str(disable_notification).lower()
                }
                resp = SESSION.post(url, files=files, data=data, timeout=30)
                if resp.status_code == 200:
                    return True
                logger.warning(f"Telegram photo HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"Telegram photo errore: {e}")
            time.sleep(2 ** attempt)
    return False


def is_priority_news(title: str) -> bool:
    """Determina se una notizia e prioritaria (suono notifica)."""
    priority_keywords = CFG['telegram'].get('priority_keywords', ['fed', 'powell', 'war', 'crash', 'rally'])
    title_lower = title.lower()
    return any(kw in title_lower for kw in priority_keywords)


# ============================================
# FETCH NEWS
# ============================================
def fetch_news_feed(url: str, max_items: int = 3) -> List[Dict]:
    try:
        resp = fetch_with_retry(url, timeout=15)
        feed = feedparser.parse(resp.content)
        news = []
        for entry in feed.entries[:max_items]:
            title = entry.get('title', '').strip()
            summary = entry.get('summary', '')[:300]
            published = entry.get('published', '')
            link = entry.get('link', '')
            if title and not db.is_news_sent(title):
                news.append({
                    'title': title,
                    'summary': summary,
                    'published': published,
                    'link': link,
                    'source': url
                })
        return news
    except Exception as e:
        logger.error(f"Errore fetch feed {url}: {e}")
        return []


def process_news_item(item: Dict) -> Optional[Dict]:
    title = item['title']
    summary = item.get('summary', '')
    tickers, keywords, countries = find_tickers_from_news(title, summary)
    if not tickers:
        return None
    sectors = classify_sectors(title, summary)
    return {
        'title': title,
        'summary': summary,
        'published': item.get('published', ''),
        'link': item.get('link', ''),
        'tickers': tickers[:CFG.get('limits', {}).get('max_tickers_per_news', 4)],
        'keywords': keywords,
        'countries': [c[0] for c in countries],
        'sectors': sectors
    }


# ============================================
# FORMATTAZIONE MESSAGGIO
# ============================================
def format_prediction_message(pred: Dict, news_title: str = "") -> str:
    company = pred.get("company", pred["ticker"])
    lines = [f"**SIGNAL: {pred['ticker']} - {company}**", ""]
    if news_title:
        lines.extend([f"**Notizia trigger:** {news_title}", ""])
    lines.extend([
        f"**Setup:** {pred['position']}",
        f"**Entry:** ${pred['entry']}",
        f"**Stop Loss:** ${pred['stop_loss']}",
        f"**Target 1:** ${pred['target_1']} (R/R: {pred['risk_reward_1']}:1)",
        f"**Target 2:** ${pred['target_2']} (R/R: {pred['risk_reward_2']}:1)",
        f"**Target 3:** ${pred['target_3']} (R/R: {pred['risk_reward_3']}:1)",
        "",
        f"**Sicurezza Entrata:** {pred['entry_safety']}",
        f"**Confidence Score:** {pred['confidence']}/100",
        "",
        f"**Data analisi:** {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        f"**Valido fino al:** {pred['valid_until']}",
        f"**Durata posizione:** {pred['hold_days']} giorni",
        "",
        "**Motivazione Confidence:**"
    ])
    for reason in pred.get('confidence_reasons', []):
        lines.append(f"  {reason}")
    lines.append("")
    lines.append("**Indicatori:**")
    ind = pred.get('indicators', {})
    if ind.get('rsi'):
        lines.append(f"  - RSI(14): {ind['rsi']}")
    if ind.get('sma20'):
        lines.append(f"  - SMA20: ${ind['sma20']}")
    if ind.get('sma50'):
        lines.append(f"  - SMA50: ${ind['sma50']}")
    if ind.get('macd'):
        lines.append(f"  - MACD: {ind['macd']['trend']} (hist: {ind['macd']['histogram']})")
    if ind.get('atr'):
        lines.append(f"  - ATR(14): ${ind['atr']}")
    if ind.get('stochastic'):
        st = ind['stochastic']
        lines.append(f"  - Stoch(14,3): K={st['k']} D={st['d']} [{st['signal']}]")
    if ind.get('volume_trend'):
        lines.append(f"  - Volume: {ind['volume_trend']}")
    return "\n".join([l for l in lines if l])


# ============================================
# HEARTBEAT
# ============================================
def send_heartbeat(cycle_count: int = 0):
    try:
        import psutil
        process = psutil.Process()
        mem_mb = process.memory_info().rss / 1024 / 1024
        uptime_hours = (time.time() - START_TIME) / 3600
    except ImportError:
        mem_mb = 0
        uptime_hours = (time.time() - START_TIME) / 3600
    db.log_heartbeat("OK", mem_mb, uptime_hours, cycle_count)
    watchdog.heartbeat()


# ============================================
# MAIN LOOP — OTTIMIZZATO CON PARALLELISMO
# ============================================
START_TIME = time.time()


def run_cycle():
    """Esegue un ciclo completo di analisi con download parallelo."""
    start_ts = time.time()
    logger.info("=" * 50)
    logger.info("INIZIO CICLO ANALISI")
    logger.info("=" * 50)

    news_count = 0
    charts_count = 0
    error_msg = ""

    try:
        # Cleanup DB
        db.cleanup_old_news(30)
        data_cache.clear()  # Pulisci cache all'inizio di ogni ciclo

        # Fetch news
        finance_sources = CFG.get('sources', {}).get('finance', [])
        geopol_sources = CFG.get('sources', {}).get('geopol', [])
        limits = CFG.get('limits', {})

        all_news = []
        for url in finance_sources[:limits.get('max_finance_news', 5)]:
            news = fetch_news_feed(url, limits.get('max_news_per_source', 2))
            all_news.extend(news)
        for url in geopol_sources[:limits.get('max_geopol_news', 5)]:
            news = fetch_news_feed(url, limits.get('max_news_per_source', 2))
            all_news.extend(news)

        logger.info(f"Notizie fresche trovate: {len(all_news)}")

        # Processa notizie
        processed = []
        for item in all_news:
            result = process_news_item(item)
            if result:
                processed.append(result)
                db.mark_news_sent(item['title'], result['tickers'])

        # Raccogli TUTTI i ticker unici da tutte le notizie
        all_tickers = []
        ticker_to_news = {}  # Mappa ticker -> notizia
        for news in processed:
            for ticker in news['tickers']:
                if ticker not in ticker_to_news:
                    all_tickers.append(ticker)
                    ticker_to_news[ticker] = news

        logger.info(f"Ticker unici da analizzare: {len(all_tickers)}")

        # DOWNLOAD PARALLELO di TUTTI i dati in una sola volta!
        max_workers = limits.get('max_workers', 5)
        batch_data = get_stock_data_batch(all_tickers, max_workers=max_workers)

        # Analisi e invio (sequenziale per Telegram, ma dati gia pronti)
        max_charts = limits.get('max_charts_per_cycle', 10)
        min_confidence = limits.get('min_confidence_to_send', 45)
        skip_low_conf_charts = PERF_CFG.get('skip_low_confidence_charts', True)

        for ticker, data in batch_data.items():
            if charts_count >= max_charts:
                logger.info(f"Raggiunto limite di {max_charts} chart per ciclo")
                break

            if not data:
                continue

            levels = calculate_trading_levels(data)
            if not levels:
                continue

            # Salva sempre nel DB
            db.save_prediction(
                ticker=ticker,
                company_name=levels['company'],
                strategy=levels['position'],
                entry=levels['entry'],
                target_1=levels['target_1'],
                target_2=levels['target_2'],
                target_3=levels['target_3'],
                stop_loss=levels['stop_loss'],
                confidence=levels['confidence'],
                confidence_reason=" | ".join(levels['confidence_reasons']),
                position=levels['position'],
                valid_until=levels['valid_until'],
                hold_days=levels['hold_days']
            )

            # Invia solo se confidence sufficiente
            if levels['confidence'] >= min_confidence:
                news = ticker_to_news.get(ticker, {})
                news_title = news.get('title', '')

                # Determina se notifica prioritaria (con suono)
                priority = is_priority_news(news_title)
                silent = CFG['telegram'].get('silent_mode', False) and not priority

                # Genera chart solo se necessario (o se confidence alta)
                if not skip_low_conf_charts or levels['confidence'] >= 65:
                    chart_path = create_advanced_chart(data, levels)
                    msg = format_prediction_message(levels, news_title)

                    send_telegram_message(msg, disable_notification=silent)
                    send_telegram_photo(chart_path, caption=f"{ticker} - {levels['company']}", 
                                       disable_notification=silent)

                    charts_count += 1

                    try:
                        os.remove(chart_path)
                    except Exception:
                        pass
                else:
                    # Invia solo messaggio testuale senza chart
                    msg = format_prediction_message(levels, news_title)
                    send_telegram_message(msg, disable_notification=silent)
                    charts_count += 1
            else:
                logger.info(f"Confidence {levels['confidence']} per {ticker} sotto soglia {min_confidence}")

            news_count += 1

        duration = time.time() - start_ts
        db.log_execution("SUCCESS", news_count, charts_count, "", duration)
        logger.info(f"Ciclo completato: {news_count} notizie, {charts_count} chart in {duration:.1f}s")

    except Exception as e:
        error_msg = str(e)
        duration = time.time() - start_ts
        db.log_execution("ERROR", news_count, charts_count, error_msg, duration)
        logger.exception(f"Errore ciclo: {e}")
        send_telegram_message(f"**ERRORE CICLO**\n\n`{error_msg[:500]}`")

    return news_count, charts_count


def main():
    logger.info("=" * 50)
    logger.info(f"FINANCE NEWS AGENT v{__version__} AVVIATO (OTTIMIZZATO)")
    logger.info("=" * 50)

    if CFG['watchdog'].get('enabled', True):
        watchdog.start()

    # Pre-fetch ticker comuni se abilitato
    if PERF_CFG.get('pre_fetch_common_tickers', True):
        common_tickers = ['SPY', 'QQQ', 'AAPL', 'MSFT', 'NVDA', 'TSLA', 'GLD', 'TLT']
        logger.info(f"Pre-fetch dati per {len(common_tickers)} ticker comuni...")
        get_stock_data_batch(common_tickers, max_workers=5)

    send_telegram_message(
        f"**Finance News Agent v{__version__} Avviato**\n\n"
        f"**Data:** {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
        f"**Intervallo:** {RUN_INTERVAL_HOURS}h\n"
        f"**Modalita:** PARALLELA (ottimizzata)\n"
        f"**DB:** {DB_PATH}"
    )

    cycle_count = 0
    hb_cfg = CFG.get('heartbeat', {})
    hb_interval = hb_cfg.get('interval_runs', 6)

    while True:
        if watchdog.triggered:
            logger.warning("Watchdog triggered! Riavvio ciclo...")
            watchdog.triggered = False

        try:
            run_cycle()
            cycle_count += 1
            send_heartbeat(cycle_count)

            if cycle_count % hb_interval == 0 and hb_cfg.get('enabled', True):
                uptime = (time.time() - START_TIME) / 3600
                stats = db.get_stats()
                send_telegram_message(
                    f"**Heartbeat**\n\n"
                    f"Cicli: {cycle_count}\n"
                    f"Uptime: {uptime:.1f}h\n"
                    f"Notizie: {stats.get('total_news', 0)}\n"
                    f"Predizioni attive: {stats.get('active_predictions', 0)}"
                )

            if cycle_count % (hb_interval * 2) == 0:
                db.backup()

        except Exception as e:
            logger.exception(f"Errore grave: {e}")
            send_telegram_message(f"**ERRORE GRAVE**\n\n`{str(e)[:500]}`")

        sleep_seconds = RUN_INTERVAL_HOURS * 3600
        logger.info(f"Sleep per {RUN_INTERVAL_HOURS}h...")

        for _ in range(sleep_seconds):
            if watchdog.triggered:
                break
            time.sleep(1)


if __name__ == "__main__":
    def signal_handler(signum, frame):
        logger.info("Segnale di arresto ricevuto")
        watchdog.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    main()
