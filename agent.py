#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================
# FINANCE NEWS AGENT v3.0 — COMPLETO & STABILE
# ============================================
# Fix: deduplicazione, logging, indicatori tecnici reali,
#      date specifiche, nomi aziende, confidence score,
#      gestione errori, heartbeat, retry

import feedparser
import requests
import os
import json
import re
import io
import sqlite3
import signal
import sys
import logging
import hashlib
import yaml
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
import time

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# ============================================
# CONFIGURAZIONE
# ============================================
CONFIG_PATH = Path("config.yaml")

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
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
    cfg.setdefault('sources', {}).setdefault('finance', [])
    cfg.setdefault('sources', {}).setdefault('geopol', [])
    return cfg

CFG = load_config()
TELEGRAM_TOKEN = CFG['telegram']['token']
CHAT_ID = CFG['telegram']['chat_id']
DB_PATH = CFG['database']['path']
RUN_INTERVAL_HOURS = CFG['run_interval_hours']

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
    "OKTA": "Okta Inc.", "S": "SentinelOne Inc.", "NET": "Cloudflare Inc.",
    "DDOG": "Datadog Inc.", "MDB": "MongoDB Inc.", "VEEV": "Veeva Systems Inc.",
    "DOCU": "DocuSign Inc.", "SHOP": "Shopify Inc.", "ETSY": "Etsy Inc.",
    "EBAY": "eBay Inc.", "W": "Wayfair Inc.", "RIVN": "Rivian Automotive Inc.",
    "LCID": "Lucid Group Inc.", "NIO": "NIO Inc.", "XPEV": "XPeng Inc.",
    "LI": "Li Auto Inc.", "QS": "QuantumScape Corp.", "MP": "MP Materials Corp.",
    "ALB": "Albemarle Corp.", "SQM": "Sociedad Química y Minera", "LTHM": "Livent Corp.",
    "ENI": "Eni S.p.A.", "UCG": "UniCredit S.p.A.", "ISP": "Intesa Sanpaolo S.p.A.",
    "LUX": "Luxottica Group", "TOT": "TotalEnergies SE", "OR": "L'Oréal S.A.",
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
    "CYBR": "CyberArk Software Ltd.", "PBR": "Petróleo Brasileiro S.A.",
    "ITUB": "Itaú Unibanco Holding", "BBD": "Banco Bradesco S.A.",
    "FMX": "Fomento Económico Mexicano", "AMX": "América Móvil S.A.B.",
    "CEMEX": "Cemex S.A.B. de C.V.", "GMEXIC": "Grupo México S.A.B.",
    "GGAL": "Grupo Financiero Galicia", "YPF": "YPF S.A.", "PAM": "Pampa Energía S.A.",
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
    "EL": "The Estée Lauder Companies", "COTY": "Coty Inc.", "ELF": "e.l.f. Beauty Inc.",
    "REV": "Revlon Inc.", "IPAR": "Inter Parfums Inc.", "LR": "L'Oréal S.A.",
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
# DATI STORICI YAHOO FINANCE
# ============================================
def get_stock_data(ticker: str, days: int = 30) -> Optional[Dict]:
    try:
        end = int(datetime.now().timestamp())
        start = int((datetime.now() - timedelta(days=days + 5)).timestamp())
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
    high = data["high"]
    low = data["low"]
    change = data["change"]
    ohlc = data.get("ohlc", [])
    volumes = data.get("volumes", [])
    tc = CFG.get('technical', {})
    tr = CFG.get('trading', {})
    rsi = calculate_rsi(prices, tc.get('rsi_period', 14))
    sma20 = calculate_sma(prices, tc.get('sma_short', 20))
    sma50 = calculate_sma(prices, tc.get('s
