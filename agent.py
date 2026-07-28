import feedparser
import requests
import os
import json
import re
import io
import base64
from datetime import datetime, timedelta
import time

# ============================================
# AGENTE AI TRADING AVANZATO
# Grafici con livelli: entrata, target, stop-loss
# Fix cron: invio automatico ogni 4 ore garantito
# ============================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# === FONTI ===
FINANCE_SOURCES = [
    "https://www.ft.com/rss/home/global",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.reuters.com/rssFeed/businessNews",
    "https://www.ilsole24ore.com/rss/finanza.xml",
    "https://feeds.afr.com/markets/rss",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
]

GEOPOL_SOURCES = [
    "https://news.google.com/rss/search?q=war+OR+conflict+OR+geopolitics+OR+tension+OR+sanctions+OR+nato+OR+ukraine+OR+israel+OR+iran+OR+taiwan+OR+china+tension&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=fed+speech+OR+ecb+speech+OR+boe+speech+OR+powell+OR+lagarde+OR+bailey+OR+central+bank+OR+interest+rate+decision&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=election+OR+political+crisis+OR+government+change+OR+trade+war+OR+tariff+OR+brexit+OR+trade+deal&hl=en-US&gl=US&ceid=US:en",
]

# === DATABASE COMPLETO ===
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

def classify_sectors(title, summary=""):
    text = (title + " " + summary).lower()
    affected = []
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            affected.append(sector)
    return affected if affected else ["Indici Globali"]

def find_countries(title, summary=""):
    text = (title + " " + summary).lower()
    found = []
    for country, assets in COUNTRY_ASSETS.items():
        if country in text:
            found.append((country, assets))
    return found

def find_tickers_from_news(title, summary=""):
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

def get_stock_data(ticker, days=10):
    """Ottiene dati storici estesi per analisi più approfondita"""
    try:
        end = int(datetime.now().timestamp())
        start = int((datetime.now() - timedelta(days=days+2)).timestamp())
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start}&period2={end}&interval=1d"
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        data = resp.json()

        if data.get("chart", {}).get("result"):
            result = data["chart"]["result"][0]
            timestamps = result.get("timestamp", [])
            quotes = result["indicators"]["quote"][0]
            closes = quotes.get("close", [])

            prices = []
            dates = []
            for ts, close in zip(timestamps, closes):
                if close is not None:
                    prices.append(close)
                    dates.append(datetime.fromtimestamp(ts).strftime("%m/%d"))

            if len(prices) >= 2:
                change = ((prices[-1] - prices[0]) / prices[0]) * 100
                return {
                    "ticker": ticker,
                    "prices": prices[-10:],
                    "dates": dates[-10:],
                    "current": prices[-1],
                    "change": change,
                    "high": max(prices),
                    "low": min(prices),
                    "avg": sum(prices) / len(prices)
                }
    except Exception as e:
        print(f"Errore dati {ticker}: {e}")
    return None

def calculate_trading_levels(data):
    """Calcola livelli di trading: entrata, target, stop-loss"""
    if not data:
        return None

    current = data["current"]
    high = data["high"]
    low = data["low"]
    avg = data["avg"]
    change = data["change"]

    # Calcola supporto e resistenza
    support = low * 0.98
    resistance = high * 1.02

    # Livelli di trading
    if change >= 0:
        # Trend rialzista
        entry = current
        target_1 = current * 1.03  # +3%
        target_2 = current * 1.05  # +5%
        target_3 = current * 1.10  # +10%
        stop_loss = current * 0.97  # -3%
        risk_reward = "1:3"  # Rischio 3%, reward potenziale 9%
    else:
        # Trend ribassista - possibile rimbalzo
        entry = current
        target_1 = current * 1.02  # +2%
        target_2 = current * 1.05  # +5%
        target_3 = current * 1.08  # +8%
        stop_loss = current * 0.95  # -5%
        risk_reward = "1:1.6"  # Rischio 5%, reward potenziale 8%

    return {
        "entry": entry,
        "target_1": target_1,
        "target_2": target_2,
        "target_3": target_3,
        "stop_loss": stop_loss,
        "support": support,
        "resistance": resistance,
        "risk_reward": risk_reward,
        "suggested_position": "LONG" if change >= -2 else "ATTENDERE",
        "confidence": "ALTA" if abs(change) > 5 else "MEDIA"
    }

def generate_advanced_chart(data, levels):
    """Genera grafico avanzato con livelli di trading"""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        import numpy as np

        prices = data["prices"]
        dates = data["dates"]
        ticker = data["ticker"]
        current = data["current"]
        change = data["change"]

        if len(prices) < 2:
            return None

        fig, ax = plt.subplots(figsize=(12, 6))
        fig.patch.set_facecolor('#0f172a')
        ax.set_facecolor('#0f172a')

        # Colori
        color_up = '#22c55e'
        color_down = '#ef4444'
        line_color = color_up if change >= 0 else color_down

        x = np.arange(len(prices))

        # Disegna linea prezzo
        ax.plot(x, prices, color=line_color, linewidth=3, marker='o', markersize=10,
                markerfacecolor=line_color, markeredgecolor='white', markeredgewidth=2, label='Prezzo')

        # Riempi area
        ax.fill_between(x, prices, alpha=0.15, color=line_color)

        # Disegna livelli di trading
        if levels:
            # Linea entrata
            ax.axhline(y=levels["entry"], color='#38bdf8', linestyle='--', linewidth=2, alpha=0.8, label=f'Entrata: ${levels["entry"]:.2f}')

            # Linea target
            ax.axhline(y=levels["target_1"], color='#22c55e', linestyle='--', linewidth=1.5, alpha=0.6, label=f'Target 1: ${levels["target_1"]:.2f} (+{((levels["target_1"]/levels["entry"])-1)*100:.1f}%)')
            ax.axhline(y=levels["target_2"], color='#22c55e', linestyle='--', linewidth=1.5, alpha=0.8, label=f'Target 2: ${levels["target_2"]:.2f} (+{((levels["target_2"]/levels["entry"])-1)*100:.1f}%)')

            # Linea stop-loss
            ax.axhline(y=levels["stop_loss"], color='#ef4444', linestyle='--', linewidth=2, alpha=0.8, label=f'Stop-Loss: ${levels["stop_loss"]:.2f} ({((levels["stop_loss"]/levels["entry"])-1)*100:.1f}%)')

            # Zone
            ax.axhspan(levels["entry"], levels["target_2"], alpha=0.05, color='green')
            ax.axhspan(levels["stop_loss"], levels["entry"], alpha=0.05, color='red')

        # Etichette prezzo
        for i, (xi, yi) in enumerate(zip(x, prices)):
            ax.annotate(f'${yi:.2f}', (xi, yi), textcoords="offset points",
                       xytext=(0, 12), ha='center', fontsize=9, color='white', fontweight='bold')

        # Date
        ax.set_xticks(x)
        ax.set_xticklabels(dates, color='#94a3b8', fontsize=10)

        # Titolo
        symbol = '+' if change >= 0 else ''
        ax.set_title(f'{ticker}  {symbol}{change:.1f}%  |  ${current:.2f}',
                    color='white', fontsize=16, fontweight='bold', pad=20)

        # Legenda
        ax.legend(loc='upper left', facecolor='#1e293b', edgecolor='#334155', 
                 labelcolor='white', fontsize=9)

        # Rimuovi assi Y
        ax.set_yticks([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_color('#334155')

        # Box info trading
        if levels:
            info_text = f"📊 LIVELLI TRADING\n"
            info_text += f"🎯 Entrata: ${levels['entry']:.2f}\n"
            info_text += f"🎯 Target 1: ${levels['target_1']:.2f} (+{((levels['target_1']/levels['entry'])-1)*100:.1f}%)\n"
            info_text += f"🎯 Target 2: ${levels['target_2']:.2f} (+{((levels['target_2']/levels['entry'])-1)*100:.1f}%)\n"
            info_text += f"🛑 Stop-Loss: ${levels['stop_loss']:.2f} ({((levels['stop_loss']/levels['entry'])-1)*100:.1f}%)\n"
            info_text += f"⚖️ Risk/Reward: {levels['risk_reward']}\n"
            info_text += f"📈 Posizione: {levels['suggested_position']} | Fiducia: {levels['confidence']}"

            props = dict(boxstyle='round,pad=0.6', facecolor='#1e293b', edgecolor=line_color, linewidth=2)
            ax.text(0.5, -0.28, info_text, transform=ax.transAxes, fontsize=9,
                    verticalalignment='top', horizontalalignment='center', color='white', bbox=props)

        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                   facecolor='#0f172a', edgecolor='none')
        plt.close()
        buf.seek(0)

        return buf
    except Exception as e:
        print(f"Errore grafico {data['ticker']}: {e}")
        return None

def send_photo_to_telegram(photo_buffer, caption=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    files = {'photo': ('chart.png', photo_buffer, 'image/png')}
    data = {'chat_id': CHAT_ID, 'caption': caption, 'parse_mode': 'HTML'}
    return requests.post(url, data=data, files=files)

def analyze_sentiment(title, summary=""):
    text = (title + " " + summary).lower()

    positive = ["surge", "rally", "gain", "growth", "profit", "beat", "strong", "boom", "rise", "bull", "rialzo", "aumento", "utile", "crescita", "breakthrough", "approval", "peace", "deal", "agreement", "treaty", "cut", "taglio", "lower", "reduce", "reduction", "drop", "fall", "decline", "decrease", "calo", "diminuzione", "abbassamento", "riduzione"]
    negative = ["crash", "fall", "drop", "loss", "bear", "recession", "crisis", "decline", "sell-off", "bearish", "ribasso", "caduta", "perdita", "crisi", "lawsuit", "recall", "war", "attack", "invasion", "sanctions", "embargo", "hike", "increase", "raise", "rialzo", "aumento", "alza", "incremento", "rialzare", "alzare"]

    if any(w in text for w in ["rate cut", "taglio tassi", "rate decrease", "lower rate", "tassi giù", "tassi in calo"]):
        return "🟢 Positivo", "Taglio tassi: stimolo economico", "📈 Considerare accumulo tech e growth"
    elif any(w in text for w in ["rate hike", "rialzo tassi", "rate increase", "raise rate", "tassi su", "tassi in rialzo"]):
        return "🔴 Negativo", "Rialzo tassi: pressione su valutazioni", "📉 Considerare riduzione esposizione growth"

    pos = sum(1 for w in positive if w in text)
    neg = sum(1 for w in negative if w in text)

    if pos > neg:
        return "🟢 Positivo", "Potenziale rialzo", "📈 Considerare accumulo"
    elif neg > pos:
        return "🔴 Negativo", "Potenziale ribasso", "📉 Considerare hedging"
    return "🟡 Neutro", "Impatto incerto", "⏸️ Attendere"

def generate_projection(title, summary, sectors, sentiment):
    text = (title + " " + summary).lower()
    projections = []

    if "Tech" in sectors or any(w in text for w in ["ai", "chip", "semiconductor", "cloud"]):
        if "🟢" in sentiment:
            projections.append("🔮 Tech: possibile continuazione rialzo se confermato da earnings")
        elif "🔴" in sentiment:
            projections.append("🔮 Tech: attenzione a rotazione verso value se pressione persist")
        else:
            projections.append("🔮 Tech: laterale fino a nuovi catalyst")

    if "Banche/Finanza" in sectors or any(w in text for w in ["fed", "ecb", "rate", "tassi"]):
        if "taglio" in text or "cut" in text:
            projections.append("🔮 Banche: NIM potrebbe comprimersi, ma stimolo credito positivo")
        elif "rialzo" in text or "hike" in text:
            projections.append("🔮 Banche: NIM in espansione, ma rischio credito crescente")
        else:
            projections.append("🔮 Banche: stabilità se curve yield flat")

    if "Energia" in sectors or any(w in text for w in ["oil", "petrolio", "gas"]):
        if "🟢" in sentiment:
            projections.append("🔮 Energia: momentum possibile se supply tight")
        elif "🔴" in sentiment:
            projections.append("🔮 Energia: correzione possibile, OPEC+ potrebbe intervenire")
        else:
            projections.append("🔮 Energia: range-bound, dipende da geopolitica e demand")

    if not projections:
        if "🟢" in sentiment:
            projections.append("🔮 Proiezione generale: trend rialzista possibile se momentum confermato")
        elif "🔴" in sentiment:
            projections.append("🔮 Proiezione generale: cautela, possibile continuazione correzione")
        else:
            projections.append("🔮 Proiezione generale: laterale, attendere breakout con volumi")

    return "\n".join(projections[:2])

def analyze_geopolitical_impact(title, summary=""):
    text = (title + " " + summary).lower()

    high_tension = ["war", "attack", "invasion", "missile", "strike", "bombing", "sanctions", "embargo", "break", "crisis", "conflict escalation"]
    medium_tension = ["tension", "dispute", "disagreement", "warning", "threat", "concern", "uncertainty", "risk"]

    tension_level = "🔴 ALTA" if any(w in text for w in high_tension) else                     "🟡 MEDIA" if any(w in text for w in medium_tension) else "🟢 BASSA"

    if any(w in text for w in ["oil", "petrolio", "gas", "energy"]):
        market_impact = "⛽ Energia: volatile"
    elif any(w in text for w in ["gold", "oro", "safe haven"]):
        market_impact = "🛡️ Safe Haven: possibile rialzo"
    elif any(w in text for w in ["fed", "ecb", "boe", "interest rate", "tasso"]):
        market_impact = "💰 Banche Centrali: impatto diretto su bond e azioni"
    elif any(w in text for w in ["trade", "tariff", "tariffa", "trade war"]):
        market_impact = "🌐 Commercio: settori export esposti"
    else:
        market_impact = "📊 Mercati: monitorare reazione"

    if "🔴" in tension_level:
        projection = "📉 Proiezione: volatilità aumentata, safe haven in rialzo, risk-off possibile"
    elif "🟡" in tension_level:
        projection = "➡️ Proiezione: cautela, possibile range-bound fino a risoluzione"
    else:
        projection = "📈 Proiezione: se risoluzione positiva, possibile risk-on"

    return {"tension": tension_level, "market_impact": market_impact, "projection": projection}

def collect_news(sources, max_per_source=2):
    all_news = []
    for url in sources:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:max_per_source]:
                all_news.append({
                    "title": entry.title,
                    "link": entry.link,
                    "source": url.split("/")[2] if "/" in url else "news",
                    "summary": entry.get("summary", "")[:300]
                })
        except Exception as e:
            print(f"Errore feed {url}: {e}")
    return all_news

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    return requests.post(url, json=payload)

def main():
    now = datetime.now()

    # Raccolta notizie
    finance_news = collect_news(FINANCE_SOURCES, max_per_source=2)
    geopol_news = collect_news(GEOPOL_SOURCES, max_per_source=2)

    # === MESSAGGIO 1: FINANZA ===
    msg1 = f"🎯 <b>AGENTE OPPORTUNITÀ FINANZIARIE</b>\n"
    msg1 += f"🕐 {now.strftime('%d/%m/%Y %H:%M')} | Ciclo: 4 ore\n"
    msg1 += "━" * 20 + "\n\n"

    all_tickers = set()

    for i, news in enumerate(finance_news[:5], 1):
        tickers, keywords, countries = find_tickers_from_news(news["title"], news["summary"])
        all_tickers.update(tickers)
        sectors = classify_sectors(news["title"], news["summary"])
        sentiment, impact, rec = analyze_sentiment(news["title"], news["summary"])
        projection = generate_projection(news["title"], news["summary"], sectors, sentiment)

        msg1 += f"<b>{i}. {news['title']}</b>\n"
        msg1 += f"   📰 {news['source']}\n"
        msg1 += f"   🔗 {news['link']}\n"
        if keywords:
            msg1 += f"   🔑 Keyword: {', '.join(keywords[:3])}\n"
        if countries:
            msg1 += f"   🌍 Paesi: {', '.join([c[0] for c in countries[:3]])}\n"
        msg1 += f"   🏷️ Settori: {', '.join(sectors)}\n"
        msg1 += f"   {sentiment} | {impact}\n"
        msg1 += f"   💡 {rec}\n"
        msg1 += f"   {projection}\n"
        if tickers:
            msg1 += f"   📊 Azioni: {', '.join(tickers)}\n"
        msg1 += "\n"

    msg1 += "━" * 20 + "\n"
    send_telegram_message(msg1)
    time.sleep(1)

    # === MESSAGGIO 2: GEOPOLITICA ===
    if geopol_news:
        msg2 = f"🌍 <b>GEOPOLITICA & BANCHE CENTRALI</b>\n"
        msg2 += "━" * 20 + "\n\n"

        for i, news in enumerate(geopol_news[:5], 1):
            geo = analyze_geopolitical_impact(news["title"], news["summary"])
            tickers, keywords, countries = find_tickers_from_news(news["title"], news["summary"])
            all_tickers.update(tickers)

            msg2 += f"<b>{i}. {news['title']}</b>\n"
            msg2 += f"   📰 {news['source']}\n"
            msg2 += f"   🔗 {news['link']}\n"
            msg2 += f"   {geo['tension']}\n"
            msg2 += f"   {geo['market_impact']}\n"
            msg2 += f"   {geo['projection']}\n"
            if countries:
                msg2 += f"   🌍 Paesi: {', '.join([c[0] for c in countries[:3]])}\n"
            if tickers:
                msg2 += f"   📊 Asset: {', '.join(tickers[:5])}\n"
            msg2 += "\n"

        msg2 += "━" * 20 + "\n"
        send_telegram_message(msg2)
        time.sleep(1)

    # === MESSAGGIO 3: GRAFICI AVANZATI CON LIVELLI ===
    if all_tickers:
        msg3 = "📈 <b>GRAFICI AVANZATI - LIVELLI DI TRADING</b>\n"
        msg3 += "━" * 20 + "\n"
        msg3 += "⚠️ Disclaimer: Analisi educativa. Non consiglio finanziario.\n"
        msg3 += "📊 Livelli calcolati su dati storici 10 giorni\n\n"
        send_telegram_message(msg3)
        time.sleep(1)

        # Genera e invia grafici avanzati
        for ticker in all_tickers:
            data = get_stock_data(ticker, days=10)
            if data:
                levels = calculate_trading_levels(data)
                chart_buf = generate_advanced_chart(data, levels)
                if chart_buf:
                    change = data["change"]

                    # Caption dettagliata
                    caption = f"<b>{ticker}</b>  {('+' if change >= 0 else '')}{change:.1f}%\n"
                    if levels:
                        caption += f"\n📊 <b>LIVELLI TRADING</b>\n"
                        caption += f"🎯 Entrata: <code>${levels['entry']:.2f}</code>\n"
                        caption += f"🎯 Target 1: <code>${levels['target_1']:.2f}</code> (+{((levels['target_1']/levels['entry'])-1)*100:.1f}%)\n"
                        caption += f"🎯 Target 2: <code>${levels['target_2']:.2f}</code> (+{((levels['target_2']/levels['entry'])-1)*100:.1f}%)\n"
                        caption += f"🛑 Stop-Loss: <code>${levels['stop_loss']:.2f}</code> ({((levels['stop_loss']/levels['entry'])-1)*100:.1f}%)\n"
                        caption += f"⚖️ Risk/Reward: <code>{levels['risk_reward']}</code>\n"
                        caption += f"📈 Posizione: <code>{levels['suggested_position']}</code> | Fiducia: <code>{levels['confidence']}</code>\n"
                        caption += f"\n💡 Se entri a ${levels['entry']:.2f}:"
                        caption += f"\n   Profitto potenziale: +{((levels['target_1']/levels['entry'])-1)*100:.1f}% → +{((levels['target_2']/levels['entry'])-1)*100:.1f}%"
                        caption += f"\n   Perdita max: {((levels['stop_loss']/levels['entry'])-1)*100:.1f}%"

                    send_photo_to_telegram(chart_buf, caption)
                    time.sleep(1)

    print("✅ Agente Trading Avanzato Completato!")

if __name__ == "__main__":
    main()
def analyze_sentiment(title, summary=""):
    text = (title + " " + summary).lower()
    positive = ["surge", "rally", "gain", "growth", "profit", "beat", "strong", "boom", "rise", "bull",
                "breakthrough", "approval", "peace", "deal", "agreement", "treaty", "expansion", 
                "outperform", "upgrade", "buy", "accumulate", "momentum", "record high", "all-time high",
                "rialzo", "aumento", "utile", "crescita", "rialzista", "bullish", "taglio tassi", 
                "rate cut", "lower rate", "stimolo", "stimulus", "recovery", "rebound", "bounce", "rimbalzo"]
    negative = ["crash", "fall", "drop", "loss", "bear", "recession", "crisis", "decline", 
                "sell-off", "bearish", "lawsuit", "recall", "war", "attack", "invasion", "sanctions",
                "embargo", "bankruptcy", "default", "downgrade", "sell", "underperform", "miss",
                "warning", "guidance cut", "profit warning", "caduta", "perdita", "crisi", "ribassista",
                "ribasso", "rate hike", "rialzo tassi", "rate increase", "restructuring", "layoff", "layoffs"]
    
    if any(w in text for w in ["rate cut", "taglio tassi", "lower rate"]):
        return "🟢 Positivo", "Taglio tassi: stimolo economico", "📈 Considerare accumulo tech e growth"
    elif any(w in text for w in ["rate hike", "rialzo tassi", "rate increase"]):
        return "🔴 Negativo", "Rialzo tassi: pressione su valutazioni", "📉 Considerare riduzione esposizione growth"
    
    pos = sum(1 for w in positive if w in text)
    neg = sum(1 for w in negative if w in text)
    
    strong_pos = ["surge", "rally", "breakthrough", "record high", "all-time high", "boom"]
    strong_neg = ["crash", "bankruptcy", "default", "recession", "crisis", "war"]
    for w in strong_pos:
        if w in text: pos += 1
    for w in strong_neg:
        if w in text: neg += 2
    
    if pos > neg + 1: return "🟢 Positivo", "Potenziale rialzo confermato", "📈 Considerare accumulo graduale"
    elif neg > pos + 1: return "🔴 Negativo", "Potenziale ribasso confermato", "📉 Considerare hedging o riduzione"
    elif pos > neg: return "🟢 Positivo (debole)", "Tendenza rialzista leggera", "📊 Monitorare per conferma"
    elif neg > pos: return "🔴 Negativo (debole)", "Tendenza ribassista leggera", "⚠️ Attendere segnali di inversione"
    return "🟡 Neutro", "Impatto incerto", "⏸️ Attendere sviluppi"
def generate_projection(title, summary, sectors, sentiment, levels=None):
    text = (title + " " + summary).lower()
    projections = []
    now = datetime.now()
    short_term = (now + timedelta(days=3)).strftime("%d/%m")
    mid_term = (now + timedelta(days=7)).strftime("%d/%m")
    long_term = (now + timedelta(days=14)).strftime("%d/%m")
    
    if "Tech" in sectors or any(w in text for w in ["ai", "chip", "semiconductor", "cloud"]):
        if "🟢" in sentiment: projections.append(f"🔮 Tech: momentum rialzista fino a {mid_term}. Se earnings confermano, estendere fino a {long_term}")
        elif "🔴" in sentiment: projections.append(f"🔮 Tech: correzione possibile fino a {short_term}. Valutare ricompra su supporto SMA20")
        else: projections.append(f"🔮 Tech: laterale {short_term}-{mid_term}. Catalyst: prossimi earnings o dati AI")
    
    if "Banche/Finanza" in sectors or any(w in text for w in ["fed", "ecb", "rate", "tassi"]):
        if "taglio" in text or "cut" in text: projections.append(f"🔮 Banche: NIM compresso a breve (fino {short_term}). Stimolo credito positivo a medio termine ({mid_term})")
        elif "rialzo" in text or "hike" in text: projections.append(f"🔮 Banche: NIM in espansione fino a {mid_term}. Attenzione rischio credito crescente post-{long_term}")
        else: projections.append(f"🔮 Banche: stabilità se curve yield flat. Monitorare spread BTP-Bund")
    
    if "Energia" in sectors or any(w in text for w in ["oil", "petrolio", "gas"]):
        if "🟢" in sentiment: projections.append(f"🔮 Energia: momentum se supply tight. Mantenere posizione fino a {mid_term}, rivedere su OPEC+")
        elif "🔴" in sentiment: projections.append(f"🔮 Energia: correzione possibile fino a {short_term}. OPEC+ potrebbe intervenire entro {mid_term}")
        else: projections.append(f"🔮 Energia: range-bound fino a {mid_term}. Dipende da geopolitica e demand estivo")
    
    if not projections:
        if "🟢" in sentiment: projections.append(f"🔮 Proiezione generale: trend rialzista possibile fino a {mid_term} se momentum confermato da volumi")
        elif "🔴" in sentiment: projections.append(f"🔮 Proiezione generale: cautela fino a {short_term}, possibile continuazione correzione fino a {mid_term}")
        else: projections.append(f"🔮 Proiezione generale: laterale {short_term}-{mid_term}. Attendere breakout con volumi superiori alla media")
    
    if levels:
        projections.append(f"⏳ HOLD SUGGERITO: mantenere posizione fino al {levels['valid_until']} o fino a target/stop raggiunto")
    
    return "\n".join(projections[:3])
def analyze_geopolitical_impact(title, summary=""):
    text = (title + " " + summary).lower()
    high_tension = ["war", "attack", "invasion", "missile", "strike", "bombing", "sanctions", "embargo", "crisis", "nuclear"]
    medium_tension = ["tension", "dispute", "warning", "threat", "concern", "risk", "standoff"]
    deescalation = ["peace", "treaty", "agreement", "ceasefire", "diplomatic", "talks", "negotiation", "deal"]
    
    if any(w in text for w in deescalation): tension_level = "🟢 DE-ESCALATION"
    elif any(w in text for w in high_tension): tension_level = "🔴 ALTA"
    elif any(w in text for w in medium_tension): tension_level = "🟡 MEDIA"
    else: tension_level = "🟢 BASSA"
    
    if any(w in text for w in ["oil", "petrolio", "gas", "energy", "opec"]): market_impact = "⛽ Energia: volatile — monitorare Brent e WTI"
    elif any(w in text for w in ["gold", "oro", "safe haven", "treasury"]): market_impact = "🛡️ Safe Haven: possibile rialzo oro e bond lunghi"
    elif any(w in text for w in ["fed", "ecb", "boe", "interest rate", "tasso"]): market_impact = "💰 Banche Centrali: impatto diretto su bond, azioni e forex"
    elif any(w in text for w in ["trade", "tariff", "trade war"]): market_impact = "🌐 Commercio: settori export e tech esposti"
    elif any(w in text for w in ["china", "taiwan", "semiconductor", "chip"]): market_impact = "🔌 Supply Chain: tech e chip a rischio"
    else: market_impact = "📊 Mercati: monitorare reazione VIX e futures"
    
    if "🔴" in tension_level: projection = "📉 Proiezione: volatilità aumentata 24-48h, safe haven in rialzo, risk-off probabile"
    elif "🟡" in tension_level: projection = "➡️ Proiezione: cautela, possibile range-bound fino a risoluzione"
    elif "DE-ESCALATION" in tension_level: projection = "📈 Proiezione: se confermata, possibile risk-on e rimbalzo equity"
    else: projection = "📊 Proiezione: impatto limitato, mercati potrebbero ignorare"
    
    return {"tension": tension_level, "market_impact": market_impact, "projection": projection, "valid_for": "24-72 ore"}
def collect_news(sources, max_per_source=2):
    all_news = []
    for url in sources:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:max_per_source]:
                title = entry.title
                if is_news_sent(title):
                    logger.info(f"News già inviata, saltata: {title[:50]}...")
                    continue
                all_news.append({
                    "title": title,
                    "link": entry.link,
                    "source": url.split("/")[2] if "/" in url else "news",
                    "summary": entry.get("summary", "")[:300],
                    "published": entry.get("published", "N/A")
                })
        except Exception as e:
            logger.error(f"Errore feed {url}: {e}")
    return all_news
def run_agent_cycle():
    now = datetime.now()
    logger.info(f"=== INIZIO CICLO AGENTE — {now.strftime('%d/%m/%Y %H:%M:%S')} ===")
    try:
        finance_news = collect_news(FINANCE_SOURCES, max_per_source=2)
        geopol_news = collect_news(GEOPOL_SOURCES, max_per_source=2)
        logger.info(f"News raccolte: Finanza={len(finance_news)}, Geopol={len(geopol_news)}")
        
        if not finance_news and not geopol_news:
            logger.warning("Nessuna nuova notizia trovata")
            log_execution("NO_NEWS", 0, 0, "Nessuna notizia nuova")
            return

        # MESSAGGIO 1: FINANZA
        msg1 = f"🎯 <b>AGENTE OPPORTUNITÀ FINANZIARIE v2.0</b>\n"
        msg1 += f"🕐 <b>{now.strftime('%d/%m/%Y %H:%M')}</b> | Ciclo: ogni {RUN_INTERVAL_HOURS}h\n"
        msg1 += f"📊 Dati: 30 giorni | Indicatori: RSI, SMA, BB, MACD, ATR\n"
        msg1 += "━" * 25 + "\n\n"
        all_tickers = set()
        
        for i, news in enumerate(finance_news[:5], 1):
            tickers, keywords, countries = find_tickers_from_news(news["title"], news["summary"])
            all_tickers.update(tickers)
            sectors = classify_sectors(news["title"], news["summary"])
            sentiment, impact, rec = analyze_sentiment(news["title"], news["summary"])
            projection = generate_projection(news["title"], news["summary"], sectors, sentiment)
            
            msg1 += f"<b>{i}. {news['title']}</b>\n"
            msg1 += f"📰 {news['source']} | 🕐 {news.get('published', 'N/A')}\n"
            msg1 += f"🔗 {news['link']}\n"
            if keywords: msg1 += f"🔑 Keyword: {', '.join(keywords[:3])}\n"
            if countries: msg1 += f"🌍 Paesi: {', '.join([c[0] for c in countries[:3]])}\n"
            msg1 += f"🏷️ Settori: {', '.join(sectors)}\n"
            msg1 += f"{sentiment} | {impact}\n"
            msg1 += f"💡 {rec}\n"
            msg1 += f"{projection}\n"
            if tickers:
                ticker_names = [f"{t} ({get_company_name(t).split(' —')[0]})" for t in tickers[:4]]
                msg1 += f"📊 Azioni: {', '.join(ticker_names)}\n"
            msg1 += "\n"
            mark_news_sent(news["title"], tickers)
        
        msg1 += "━" * 25 + "\n"
        send_telegram_message(msg1)
        time.sleep(1)

        # MESSAGGIO 2: GEOPOLITICA
        if geopol_news:
            msg2 = f"🌍 <b>GEOPOLITICA & BANCHE CENTRALI</b>\n"
            msg2 += f"🕐 <b>{now.strftime('%d/%m/%Y %H:%M')}</b>\n"
            msg2 += "━" * 25 + "\n\n"
            for i, news in enumerate(geopol_news[:5], 1):
                geo = analyze_geopolitical_impact(news["title"], news["summary"])
                tickers, keywords, countries = find_tickers_from_news(news["title"], news["summary"])
                all_tickers.update(tickers)
                msg2 += f"<b>{i}. {news['title']}</b>\n"
                msg2 += f"📰 {news['source']} | 🕐 {news.get('published', 'N/A')}\n"
                msg2 += f"🔗 {news['link']}\n"
                msg2 += f"{geo['tension']}\n"
                msg2 += f"{geo['market_impact']}\n"
                msg2 += f"{geo['projection']}\n"
                msg2 += f"⏳ Validità: {geo['valid_for']}\n"
                if countries: msg2 += f"🌍 Paesi: {', '.join([c[0] for c in countries[:3]])}\n"
                if tickers:
                    ticker_names = [f"{t} ({get_company_name(t).split(' —')[0]})" for t in tickers[:4]]
                    msg2 += f"📊 Asset: {', '.join(ticker_names)}\n"
                msg2 += "\n"
                mark_news_sent(news["title"], tickers)
            msg2 += "━" * 25 + "\n"
            send_telegram_message(msg2)
            time.sleep(1)

        # MESSAGGIO 3: GRAFICI
        if all_tickers:
            msg3 = "📈 <b>GRAFICI AVANZATI — ANALISI TECNICA COMPLETA</b>\n"
            msg3 += "━" * 25 + "\n"
            msg3 += "⚠️ <i>Disclaimer: Analisi educativa. Non consiglio finanziario.</i>\n"
            msg3 += f"📊 Dati storici: 30 giorni | Indicatori: RSI, SMA20, Bollinger, MACD, ATR\n"
            msg3 += f"🕐 Generato: {now.strftime('%d/%m/%Y %H:%M')}\n\n"
            send_telegram_message(msg3)
            time.sleep(1)
            
            charts_sent = 0
            for ticker in all_tickers:
                data = get_stock_data(ticker, days=30)
                if data:
                    levels = calculate_trading_levels(data)
                    chart_buf = generate_advanced_chart(data, levels)
                    if chart_buf:
                        change = data["change"]
                        company = data.get("company", ticker)
                        caption = f"<b>{ticker}</b> — {company}\n"
                        caption += f"{'+' if change >= 0 else ''}{change:.2f}% | ${data['current']:.2f}\n"
                        caption += f"🕐 Dati fino al: {data['dates'][-1]} | Generato: {now.strftime('%d/%m/%Y %H:%M')}\n\n"
                        
                        if levels:
                            caption += f"<b>📊 LIVELLI TRADING</b>\n"
                            caption += f"🎯 Entrata: <code>${levels['entry']:.2f}</code>\n"
                            caption += f"🎯 Target 1: <code>${levels['target_1']:.2f}</code> (+{((levels['target_1']/levels['entry'])-1)*100:.1f}%)\n"
                            caption += f"🎯 Target 2: <code>${levels['target_2']:.2f}</code> (+{((levels['target_2']/levels['entry'])-1)*100:.1f}%)\n"
                            caption += f"🎯 Target 3: <code>${levels['target_3']:.2f}</code> (+{((levels['target_3']/levels['entry'])-1)*100:.1f}%)\n"
                            caption += f"🛑 Stop-Loss: <code>${levels['stop_loss']:.2f}</code> ({((levels['stop_loss']/levels['entry'])-1)*100:.1f}%)\n"
                            caption += f"⚖️ Risk/Reward: <code>{levels['risk_reward']}</code>\n"
                            caption += f"📈 Posizione: <code>{levels['suggested_position']}</code>\n"
                            caption += f"🎯 Fiducia: <code>{levels['confidence']} ({levels['confidence_score']}%)</code>\n"
                            caption += f"⏳ Valida fino al: <code>{levels['valid_until']}</code>\n"
                            caption += f"📅 Timeframe: <code>{levels['timeframe']}</code>\n\n"
                            
                            caption += f"<b>📐 INDICATORI TECNICI</b>\n"
                            if levels.get('rsi'): caption += f"📊 RSI(14): <code>{levels['rsi']}</code>\n"
                            if levels.get('sma20'): caption += f"📊 SMA20: <code>${levels['sma20']:.2f}</code>\n"
                            if levels.get('atr'): caption += f"📊 ATR(14): <code>${levels['atr']:.2f}</code>\n"
                            if levels.get('volume_trend'): caption += f"📊 Volume: <code>{levels['volume_trend']}</code>\n"
                            if levels.get('signals'): caption += f"📊 Segnali: <code>{'; '.join(levels['signals'][:3])}</code>\n"
                            
                            caption += f"\n💡 <b>Scenario:</b> Se entri a ${levels['entry']:.2f}:\n"
                            caption += f"Profitto potenziale: +{((levels['target_1']/levels['entry'])-1)*100:.1f}% → +{((levels['target_2']/levels['entry'])-1)*100:.1f}%\n"
                            caption += f"Perdita max: {((levels['stop_loss']/levels['entry'])-1)*100:.1f}%\n"
                            caption += f"🎯 Sicurezza entrata: <b>{levels['confidence_score']}%</b> — {levels['confidence']}"
                            
                            save_prediction(ticker, company, "SWING", levels['entry'], 
                                          levels['target_1'], levels['target_2'], levels['target_3'],
                                          levels['stop_loss'], levels['confidence_score'], 
                                          levels['suggested_position'], levels['valid_until_dt'])
                        
                        send_photo_to_telegram(chart_buf, caption)
                        charts_sent += 1
                        time.sleep(1.5)
            
            logger.info(f"Ciclo completato. Grafici inviati: {charts_sent}")
            log_execution("SUCCESS", len(finance_news) + len(geopol_news), charts_sent)
        else:
            log_execution("SUCCESS_NO_CHARTS", len(finance_news) + len(geopol_news), 0)
            
    except Exception as e:
        logger.exception("Errore nel ciclo agente")
        log_execution("ERROR", 0, 0, str(e))
        try:
            send_telegram_message(f"⚠️ <b>ERRORE AGENTE</b>\n{now.strftime('%d/%m/%Y %H:%M')}\n{e}\n\nL'agente riproverà al prossimo ciclo.")
        except:
            pass

# SCHEDULER ROBUSTO
_running = True

def signal_handler(signum, frame):
    global _running
    logger.info("Segnale di arresto ricevuto. Arresto graceful...")
    _running = False

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def scheduler_loop():
    global _running
    logger.info(f"Scheduler avviato. Intervallo: {RUN_INTERVAL_HOURS} ore")
    run_agent_cycle()
    while _running:
        next_run = datetime.now() + timedelta(hours=RUN_INTERVAL_HOURS)
        logger.info(f"Prossima esecuzione: {next_run.strftime('%d/%m/%Y %H:%M:%S')}")
        while datetime.now() < next_run and _running:
            time.sleep(30)
        if _running:
            run_agent_cycle()

def main():
    init_db()
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.error("TELEGRAM_TOKEN o TELEGRAM_CHAT_ID mancanti!")
        print("Errore: imposta le variabili d'ambiente TELEGRAM_TOKEN e TELEGRAM_CHAT_ID")
        return
    logger.info("=" * 60)
    logger.info("FINANCE NEWS AGENT v2.0 — Avvio")
    logger.info("=" * 60)
    last = get_last_execution()
    if last:
        logger.info(f"Ultima esecuzione: {last[0]} | Status: {last[1]} | News: {last[2]} | Charts: {last[3]}")
    scheduler_loop()
    logger.info("Agente arrestato. Arrivederci!")

if __name__ == "__main__":
    main()