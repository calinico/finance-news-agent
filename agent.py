import feedparser
import requests
import os
import json
import re
from datetime import datetime, timedelta
import time

# ============================================
# AGENTE AI FINANZIARIO ULTIMATE
# Geopolitica + Speech Banche Centrali + Opportunità
# Frequenza: ogni 4 ore
# ============================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# === FONTI NOTIZIE FINANZIARIE ===
FINANCE_SOURCES = [
    "https://www.ft.com/rss/home/global",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.reuters.com/rssFeed/businessNews",
    "https://www.ilsole24ore.com/rss/finanza.xml",
    "https://feeds.afr.com/markets/rss",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
]

# === FONTI GEOPOLITICA & MACRO ===
GEOPOL_SOURCES = [
    # Google News - Geopolitica & Guerra
    "https://news.google.com/rss/search?q=war+OR+conflict+OR+geopolitics+OR+tension+OR+sanctions+OR+nato+OR+ukraine+OR+israel+OR+iran+OR+taiwan+OR+china+tension&hl=en-US&gl=US&ceid=US:en",
    # Google News - Banche Centrali & Speech
    "https://news.google.com/rss/search?q=fed+speech+OR+ecb+speech+OR+boe+speech+OR+powell+OR+lagarde+OR+bailey+OR+central+bank+OR+interest+rate+decision&hl=en-US&gl=US&ceid=US:en",
    # Google News - Elezioni & Politica
    "https://news.google.com/rss/search?q=election+OR+political+crisis+OR+government+change+OR+trade+war+OR+tariff+OR+brexit+OR+trade+deal&hl=en-US&gl=US&ceid=US:en",
]

# === DATABASE: Paesi → Azioni/ETF/Commodities ===
COUNTRY_ASSETS = {
    # USA
    "united states": ["SPY", "QQQ", "DIA", "XLF", "TLT", "GLD", "USO", "UUP"],
    "usa": ["SPY", "QQQ", "DIA", "XLF", "TLT", "GLD", "USO", "UUP"],
    "fed": ["SPY", "QQQ", "XLF", "TLT", "KRE", "JPM", "BAC", "BLK"],
    "powell": ["SPY", "QQQ", "XLF", "TLT", "KRE"],

    # Europa
    "europe": ["VGK", "EZU", "EWG", "EWQ", "EWI", "EWP", "EWN", "FEZ"],
    "european union": ["VGK", "EZU", "EWG", "EWQ", "EWI", "FEZ"],
    "ecb": ["VGK", "EZU", "EWG", "EWQ", "DB", "UBS", "CS", "SAN"],
    "lagarde": ["VGK", "EZU", "EWG", "EWQ"],
    "germany": ["EWG", "VGK", "EZU", "SAP", "SIE", "BMWYY", "VWAGY"],
    "france": ["EWQ", "VGK", "EZU", "TOT", "OR", "SAN", "AIR"],
    "italy": ["EWI", "VGK", "EZU", "ENI", "UCG", "ISP", "LUX"],
    "spain": ["EWP", "VGK", "EZU", "SAN", "BBVA", "ITX", "TEF"],
    "uk": ["EWU", "VGK", "EZU", "HSBC", "BP", "SHEL", "AZN", "UL"],
    "britain": ["EWU", "VGK", "HSBC", "BP", "SHEL", "AZN"],
    "boe": ["EWU", "VGK", "HSBC", "BP", "SHEL"],
    "bailey": ["EWU", "VGK", "HSBC", "BP"],

    # Asia
    "china": ["FXI", "MCHI", "KWEB", "ASHR", "BABA", "TCEHY", "JD", "PDD"],
    "taiwan": ["EWT", "FXI", "TSM", "UMC", "ASML"],
    "japan": ["EWJ", "DXJ", "HEWJ", "TM", "HMC", "SONY", "NTDOY", "SNE"],
    "india": ["INDA", "EPI", "MINDX", "INFY", "TCS", "WIT", "HDB"],
    "south korea": ["EWY", "SKM", "KB", "KEP", "POSCO", "LPL"],
    "australia": ["EWA", "BHP", "RIO", "WPL", "NAB", "WBC", "ANZ"],

    # Medio Oriente
    "israel": ["ISRA", "EIS", "TEVA", "ICL", "CHKP", "CYBR"],
    "iran": ["USO", "UCO", "BNO", "OIL", "XLE", "CVX", "XOM"],
    "saudi arabia": ["KSA", "USO", "XLE", "CVX", "XOM", "ARAMCO"],
    "uae": ["UAE", "USO", "XLE"],
    "qatar": ["QAT", "USO", "XLE"],

    # Russia & Commodities
    "russia": ["RSX", "ERUS", "USO", "GLD", "UNG", "WEAT", "CORN", "SOYB"],
    "ukraine": ["USO", "UNG", "WEAT", "CORN", "SOYB", "GLD", "RSX"],

    # America Latina
    "brazil": ["EWZ", "BRZU", "PBR", "VALE", "ITUB", "BBD"],
    "mexico": ["EWW", "FMX", "AMX", "CEMEX", "GMEXIC"],
    "argentina": ["ARGT", "GGAL", "YPF", "PAM", "TEO"],

    # Commodities & Safe Haven
    "gold": ["GLD", "IAU", "PHYS", "GOLD", "NEM", "AEM", "KGC"],
    "oil": ["USO", "UCO", "BNO", "XLE", "XOM", "CVX", "COP", "OXY"],
    "natural gas": ["UNG", "BOIL", "KOLD", "KBR", "SWN", "EQT"],
    "wheat": ["WEAT", "CORN", "SOYB", "DBA", "TEUC", "ADM"],
    "corn": ["CORN", "WEAT", "SOYB", "DBA", "ADM", "INGR"],

    # Cripto
    "bitcoin": ["MSTR", "COIN", "HOOD", "BITO", "BITW", "GBTC", "RIOT", "MARA"],
    "ethereum": ["COIN", "HOOD", "BITW", "ETHE", "RIOT", "MARA", "HIVE"],
}

# === DATABASE: Keyword → Azioni (esteso) ===
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

    "tesla": ["TSLA", "RIVN", "LCID", "FSR", "NIO", "XPEV", "LI", "BYDDF"],
    "ev": ["TSLA", "RIVN", "LCID", "FSR", "NIO", "XPEV", "LI", "QS", "MP"],
    "electric vehicle": ["TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI", "QS", "MP"],
    "automaker": ["F", "GM", "STLA", "TM", "HMC", "HYMTF", "VWAGY", "BMWYY"],
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
}

# === SECTOR ETFS (fallback) ===
SECTOR_ETFS = {
    "Tech": ["XLK", "VGT", "FTEC", "SMH", "SOXX", "IGV"],
    "Banche/Finanza": ["XLF", "VFH", "KRE", "KBE", "IYF"],
    "Energia": ["XLE", "VDE", "FENY", "OIH", "XOP"],
    "Farmaceutica/Biotech": ["XBI", "IBB", "VHT", "XLV", "IHI"],
    "Auto/Elettrici": ["DRIV", "IDRV", "LIT", "BATT", "CARZ"],
    "Crypto": ["BITO", "BITW", "WGMI", "BKCH"],
    "Immobiliare": ["VNQ", "SCHH", "USRT", "REET", "FREL"],
    "Materie Prime": ["PDBC", "USCI", "GCC", "GSG", "COMT"],
    "Indici Globali": ["SPY", "QQQ", "IWM", "DIA", "VTI", "VEU"],
    "Geopolitica/Safe Haven": ["GLD", "IAU", "TLT", "IEF", "VIXY", "SQQQ"],
}

# === SECTOR KEYWORDS ===
SECTOR_KEYWORDS = {
    "Tech": ["apple", "microsoft", "google", "meta", "nvidia", "ai", "artificial intelligence", "chip", "semiconductor", "cloud", "cybersecurity", "data center"],
    "Banche/Finanza": ["bank", "banca", "fed", "ecb", "interest rate", "tasso", "banche", "credit", "loan", "mortgage", "central bank"],
    "Energia": ["oil", "petrolio", "gas", "energy", "renewable", "solar", "wind", "opec"],
    "Farmaceutica/Biotech": ["pharma", "drug", "vaccine", "biotech", "fda", "clinical trial"],
    "Auto/Elettrici": ["tesla", "ev", "electric vehicle", "automaker", "car", "battery"],
    "Crypto": ["bitcoin", "ethereum", "crypto", "blockchain"],
    "Immobiliare": ["real estate", "housing", "property", "mortgage", "construction"],
    "Materie Prime": ["gold", "oro", "silver", "copper", "commodity", "steel"],
    "Indici Globali": ["sp500", "nasdaq", "dow", "ftse", "dax", "nikkei"],
    "Geopolitica/Safe Haven": ["war", "conflict", "sanctions", "tension", "missile", "attack", "invasion", "peace", "treaty", "diplomatic"],
}

def classify_sectors(title, summary=""):
    text = (title + " " + summary).lower()
    affected = []
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            affected.append(sector)
    return affected if affected else ["Indici Globali"]

def find_countries(title, summary=""):
    """Trova paesi menzionati nella notizia"""
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

    # Aggiungi asset paesi
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

def get_stock_data(ticker, days=5):
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
            for ts, close in zip(timestamps, closes):
                if close is not None:
                    prices.append(close)

            if len(prices) >= 2:
                change = ((prices[-1] - prices[0]) / prices[0]) * 100
                return {
                    "ticker": ticker,
                    "prices": prices[-5:],
                    "current": prices[-1],
                    "change": change,
                    "high": max(prices),
                    "low": min(prices)
                }
    except Exception as e:
        print(f"Errore dati {ticker}: {e}")
    return None

def generate_mini_chart(data, width=30):
    if not data or len(data["prices"]) < 2:
        return "[N/A]"

    prices = data["prices"]
    min_p = min(prices)
    max_p = max(prices)
    range_p = max_p - min_p if max_p != min_p else 1

    change = data["change"]
    symbol = "🟢" if change >= 0 else "🔴"

    bars = []
    for price in prices:
        if range_p > 0:
            level = int(((price - min_p) / range_p) * 4)
        else:
            level = 2
        bars.append("▁▂▃▄▅▆▇█"[level])

    chart = "".join(bars)
    return f"{symbol} {data['ticker']} ${data['current']:.2f} ({change:+.1f}%) {chart}"

def analyze_opportunity(ticker, news_title):
    data = get_stock_data(ticker, days=5)
    if not data:
        return None

    change = data["change"]

    if change < -5:
        opportunity = "⚠️ RIBASSO FORTE - Possibile oversold"
        action = "🎯 Controllare punto di ingresso"
    elif change < -2:
        opportunity = "📉 RIBASSO MODERATO - Monitorare"
        action = "👀 Attendere conferma inversione"
    elif change > 5:
        opportunity = "🚀 RIALZO FORTE - Momentum attivo"
        action = "⚡ Considerare profit-taking"
    elif change > 2:
        opportunity = "📈 RIALZO MODERATO - Trend positivo"
        action = "✅ Valutare ingresso"
    else:
        opportunity = "➡️ LATERALE - Nessuna opportunità"
        action = "⏸️ Attendere breakout"

    return {
        "ticker": ticker,
        "chart": generate_mini_chart(data),
        "opportunity": opportunity,
        "action": action,
        "data": data
    }

def analyze_geopolitical_impact(title, summary=""):
    """Analizza impatto geopolitico sulla notizia"""
    text = (title + " " + summary).lower()

    # Livello di tensione
    high_tension = ["war", "attack", "invasion", "missile", "strike", "bombing", "sanctions", "embargo", "break", "crisis", "conflict escalation"]
    medium_tension = ["tension", "dispute", "disagreement", "warning", "threat", "concern", "uncertainty", "risk"]

    tension_level = "🔴 ALTA" if any(w in text for w in high_tension) else                     "🟡 MEDIA" if any(w in text for w in medium_tension) else "🟢 BASSA"

    # Impatto mercati
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

    # Proiezione
    if "🔴" in tension_level:
        projection = "📉 Proiezione: volatilità aumentata, safe haven in rialzo, risk-off possibile"
    elif "🟡" in tension_level:
        projection = "➡️ Proiezione: cautela, possibile range-bound fino a risoluzione"
    else:
        projection = "📈 Proiezione: se risoluzione positiva, possibile risk-on"

    return {
        "tension": tension_level,
        "market_impact": market_impact,
        "projection": projection
    }

def analyze_sentiment(title, summary=""):
    text = (title + " " + summary).lower()

    positive = ["surge", "rally", "gain", "growth", "profit", "beat", "strong", "boom", "rise", "bull", "rialzo", "aumento", "utile", "crescita", "breakthrough", "approval", "peace", "deal", "agreement", "treaty"]
    negative = ["crash", "fall", "drop", "loss", "bear", "recession", "crisis", "decline", "sell-off", "bearish", "ribasso", "caduta", "perdita", "crisi", "lawsuit", "recall", "war", "attack", "invasion", "sanctions", "embargo"]

    pos = sum(1 for w in positive if w in text)
    neg = sum(1 for w in negative if w in text)

    if pos > neg:
        return "🟢 Positivo", "Potenziale rialzo", "📈 Considerare accumulo"
    elif neg > pos:
        return "🔴 Negativo", "Potenziale ribasso", "📉 Considerare hedging"
    return "🟡 Neutro", "Impatto incerto", "⏸️ Attendere"

def collect_news(sources, max_per_source=2):
    """Raccoglie notizie da fonti RSS"""
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
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    return requests.post(url, json=payload)

def main():
    now = datetime.now()

    # === RACCOLTA NOTIZIE ===
    finance_news = collect_news(FINANCE_SOURCES, max_per_source=2)
    geopol_news = collect_news(GEOPOL_SOURCES, max_per_source=2)

    # === MESSAGGIO 1: FINANZA + OPPORTUNITÀ ===
    msg1 = f"💼 <b>FINANZA & OPPORTUNITÀ</b>\n"
    msg1 += f"🕐 {now.strftime('%d/%m/%Y %H:%M')} | Ciclo: 4 ore\n"
    msg1 += "━" * 20 + "\n\n"

    all_tickers = set()

    for i, news in enumerate(finance_news[:5], 1):
        tickers, keywords, countries = find_tickers_from_news(news["title"], news["summary"])
        all_tickers.update(tickers)
        sectors = classify_sectors(news["title"], news["summary"])
        sentiment, impact, rec = analyze_sentiment(news["title"], news["summary"])

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
        if tickers:
            msg1 += f"   📊 Asset: {', '.join(tickers)}\n"
        msg1 += "\n"

    msg1 += "━" * 20 + "\n"
    send_telegram_message(msg1)
    time.sleep(1)

    # === MESSAGGIO 2: GEOPOLITICA & BANCHE CENTRALI ===
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
                msg2 += f"   🌍 Paesi coinvolti: {', '.join([c[0] for c in countries[:3]])}\n"
            if tickers:
                msg2 += f"   📊 Asset monitorare: {', '.join(tickers[:5])}\n"
            msg2 += "\n"

        msg2 += "━" * 20 + "\n"
        send_telegram_message(msg2)
        time.sleep(1)

    # === MESSAGGIO 3: GRAFICI & OPPORTUNITÀ ===
    if all_tickers:
        msg3 = "📈 <b>ANALISI ASSET COINVOLTI</b>\n"
        msg3 += "━" * 20 + "\n\n"

        opportunities = []
        for ticker in all_tickers:
            opp = analyze_opportunity(ticker, "")
            if opp:
                opportunities.append(opp)

        opportunities.sort(key=lambda x: abs(x["data"]["change"]), reverse=True)

        for opp in opportunities[:10]:
            msg3 += f"{opp['chart']}\n"
            msg3 += f"   {opp['opportunity']}\n"
            msg3 += f"   {opp['action']}\n\n"

        msg3 += "━" * 20 + "\n"
        msg3 += "⚠️ <b>Disclaimer:</b> Analisi educativa. Non consiglio finanziario.\n"
        msg3 += "Dati: Yahoo Finance (non ufficiale).\n"
        msg3 += "🕐 Prossimo aggiornamento: 4 ore"

        send_telegram_message(msg3)

    print("✅ Agente Ultimate completato!")

if __name__ == "__main__":
    main()
