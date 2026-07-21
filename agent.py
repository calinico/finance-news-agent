import feedparser
import requests
import os
import json
import re
from datetime import datetime, timedelta
import time

# ============================================
# AGENTE AI: SCOPRI OPPORTUNITÀ FINANZIARIE
# Trova azioni REALI coinvolte dalle notizie
# Frequenza: ogni 4 ore
# ============================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SOURCES = [
    "https://www.ft.com/rss/home/global",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.reuters.com/rssFeed/businessNews",
    "https://www.ilsole24ore.com/rss/finanza.xml",
    "https://feeds.afr.com/markets/rss",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
]

# DATABASE: Parole chiave → Azioni/ETF reali (Yahoo Finance tickers)
# Mappa estesa per trovare occasioni anche in titoli meno famosi
KEYWORD_TICKERS = {
    # Tech & AI
    "apple": ["AAPL", "AVGO", "LITE", "QRVO", "SWKS"],
    "iphone": ["AAPL", "LGL", "CRUS", "STM"],
    "microsoft": ["MSFT", "QLYS", "VEEV", "DOCU"],
    "google": ["GOOGL", "GOOG", "TDC", "TRMB"],
    "meta": ["META", "SNAP", "PINS", "MTCH"],
    "nvidia": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "SWKS"],
    "ai": ["NVDA", "AMD", "PLTR", "AI", "SNOW", "MDB", "DDOG", "NET"],
    "artificial intelligence": ["NVDA", "AMD", "PLTR", "AI", "SNOW", "MDB", "DDOG"],
    "chip": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "SWKS", "QRVO", "MPWR"],
    "semiconductor": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "SWKS", "AMAT", "LRCX"],
    "cloud": ["MSFT", "AMZN", "GOOGL", "CRM", "NOW", "SNOW", "DDOG", "MDB"],
    "cybersecurity": ["CRWD", "PANW", "FTNT", "ZS", "OKTA", "CYBR", "S", "NET"],
    "data center": ["NVDA", "AMD", "INTC", "SMCI", "DELL", "HPE", "ANET"],

    # Banche & Finanza
    "bank": ["JPM", "BAC", "WFC", "C", "GS", "MS", "PNC", "USB", "TFC", "RF"],
    "banca": ["JPM", "BAC", "WFC", "C", "GS", "MS"],
    "fed": ["XLF", "KRE", "JPM", "BAC", "GS", "MS", "BLK", "STT", "BK"],
    "ecb": ["VGK", "EZU", "EWG", "EWQ", "EWI", "DB", "CS", "UBS"],
    "interest rate": ["XLF", "KRE", "JPM", "BAC", "GS", "TLT", "TLH", "IEF"],
    "tasso": ["XLF", "KRE", "JPM", "BAC", "TLT", "IEF"],
    "credit": ["JPM", "BAC", "WFC", "C", "DFS", "COF", "SYF", "ALLY"],
    "mortgage": ["RKT", "UWMC", "LDI", "PFSI", "COOP", "FNMA", "FMCC"],

    # Energia
    "oil": ["XOM", "CVX", "COP", "EOG", "MPC", "VLO", "PSX", "MRO", "OXY", "DVN"],
    "petrolio": ["XOM", "CVX", "COP", "EOG", "MPC", "VLO", "OXY"],
    "gas": ["XOM", "CVX", "COP", "EOG", "MRO", "DVN", "EQT", "RRC", "SWN"],
    "energy": ["XOM", "CVX", "COP", "EOG", "XLE", "OXY", "DVN", "MRO", "FANG"],
    "renewable": ["ENPH", "SEDG", "FSLR", "RUN", "NOVA", "SPWR", "CSIQ", "JKS"],
    "solar": ["ENPH", "SEDG", "FSLR", "RUN", "NOVA", "SPWR", "CSIQ", "JKS"],
    "wind": ["GE", "VWDRY", "NPI", "BEP", "NEE", "ORA", "TPIC"],
    "opec": ["XOM", "CVX", "COP", "EOG", "MPC", "VLO", "PSX"],

    # Farmaceutica
    "pharma": ["JNJ", "PFE", "MRK", "ABBV", "BMY", "LLY", "NVO", "AZN", "GILD", "BIIB"],
    "drug": ["JNJ", "PFE", "MRK", "ABBV", "BMY", "LLY", "NVO", "AZN", "GILD", "VRTX"],
    "vaccine": ["PFE", "MRNA", "BNTX", "NVAX", "GSK", "SNY", "JNJ"],
    "biotech": ["AMGN", "GILD", "BIIB", "VRTX", "REGN", "ALNY", "SRPT", "BMRN"],
    "fda": ["JNJ", "PFE", "MRK", "ABBV", "BMY", "LLY", "VRTX", "REGN", "ALNY"],
    "clinical trial": ["BIIB", "VRTX", "REGN", "ALNY", "SRPT", "BMRN", "IONS", "EXEL"],

    # Auto & EV
    "tesla": ["TSLA", "RIVN", "LCID", "FSR", "NIO", "XPEV", "LI", "BYDDF"],
    "ev": ["TSLA", "RIVN", "LCID", "FSR", "NIO", "XPEV", "LI", "BYDDF", "QS", "MP"],
    "electric vehicle": ["TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI", "QS", "MP"],
    "automaker": ["F", "GM", "STLA", "TM", "HMC", "HYMTF", "VWAGY", "BMWYY"],
    "car": ["F", "GM", "STLA", "TM", "HMC", "VWAGY", "BMWYY", "RACE"],
    "battery": ["TSLA", "QS", "MP", "ALB", "SQM", "LTHM", "PLL", "LAC"],

    # Crypto
    "bitcoin": ["MSTR", "COIN", "HOOD", "BITO", "BITW", "GBTC", "ETHE", "RIOT", "MARA"],
    "ethereum": ["COIN", "HOOD", "BITW", "ETHE", "RIOT", "MARA", "HIVE", "HUT"],
    "crypto": ["MSTR", "COIN", "HOOD", "BITO", "RIOT", "MARA", "HIVE", "HUT", "BITF"],
    "blockchain": ["IBM", "COIN", "MSTR", "RIOT", "MARA", "SQ", "PYPL"],

    # Immobiliare
    "real estate": ["VNQ", "SPG", "O", "AMT", "PLD", "WPC", "NNN", "STAG", "EXR", "PSA"],
    "housing": ["DHI", "LEN", "PHM", "TOL", "NVR", "KBH", "MTH", "TMHC", "TPH"],
    "property": ["VNQ", "SPG", "O", "AMT", "PLD", "WPC", "NNN", "STAG"],
    "construction": ["DHI", "LEN", "PHM", "TOL", "NVR", "KBH", "CAT", "DE", "URI"],

    # Materie Prime
    "gold": ["GLD", "GOLD", "NEM", "AEM", "KGC", "WPM", "RGLD", "FNV", "OR"],
    "oro": ["GLD", "GOLD", "NEM", "AEM", "KGC", "WPM", "RGLD", "FNV"],
    "silver": ["SLV", "PAAS", "HL", "CDE", "EXK", "MAG", "FSM", "SVM"],
    "copper": ["FCX", "SCCO", "TECK", "VALE", "RIO", "BHP", "GLNCY", "ANTO"],
    "commodity": ["PDBC", "USCI", "GCC", "DJP", "DBC", "GSG", "COMT"],
    "steel": ["NUE", "STLD", "MT", "VALE", "RIO", "BHP", "CLF", "TX"],

    # Retail & Consumer
    "amazon": ["AMZN", "SHOP", "ETSY", "EBAY", "W", "OSTK", "CVNA"],
    "retail": ["WMT", "TGT", "COST", "HD", "LOW", "BBY", "TJX", "ROST", "BURL"],
    "consumer": ["PG", "KO", "PEP", "WMT", "COST", "MCD", "SBUX", "DPZ", "YUM"],

    # Industria & Difesa
    "defense": ["LMT", "NOC", "RTX", "GD", "BA", "HII", "KTOS", "BWXT"],
    "aerospace": ["BA", "AIR", "SAFRF", "GE", "HON", "RTX", "LMT", "NOC"],
    "infrastructure": ["CAT", "DE", "URI", "PCAR", "VMI", "MLI", "TREX", "AWP"],

    # Telecom & Media
    "telecom": ["T", "VZ", "TMUS", "CMCSA", "CHTR", "LUMN", "FYBR", "CNSL"],
    "streaming": ["NFLX", "DIS", "WBD", "PARA", "ROKU", "FUBO", "AMC", "CNK"],

    # Indici
    "sp500": ["SPY", "VOO", "IVV", "SPLG", "SPXL", "SPXS", "UPRO", "SDS"],
    "nasdaq": ["QQQ", "TQQQ", "SQQQ", "QLD", "QID", "ONEQ", "QQQM"],
    "dow": ["DIA", "UDOW", "SDOW", "DOG", "DXD"],
}

# ETF per settore (se non troviamo azioni specifiche)
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
}

SECTOR_KEYWORDS = {
    "Tech": ["apple", "microsoft", "google", "meta", "nvidia", "ai", "artificial intelligence", "chip", "semiconductor", "cloud", "cybersecurity", "data center"],
    "Banche/Finanza": ["bank", "banca", "fed", "ecb", "interest rate", "tasso", "banche", "credit", "loan", "mortgage"],
    "Energia": ["oil", "petrolio", "gas", "energy", "renewable", "solar", "wind", "opec"],
    "Farmaceutica/Biotech": ["pharma", "drug", "vaccine", "biotech", "fda", "clinical trial"],
    "Auto/Elettrici": ["tesla", "ev", "electric vehicle", "automaker", "car", "battery"],
    "Crypto": ["bitcoin", "ethereum", "crypto", "blockchain"],
    "Immobiliare": ["real estate", "housing", "property", "mortgage", "construction"],
    "Materie Prime": ["gold", "oro", "silver", "copper", "commodity", "steel"],
    "Indici Globali": ["sp500", "nasdaq", "dow", "ftse", "dax", "nikkei"],
}

def find_tickers_from_news(title, summary=""):
    """Trova azioni REALI coinvolte dalla notizia"""
    text = (title + " " + summary).lower()
    found_tickers = set()
    matched_keywords = []

    # Cerca keyword → ticker
    for keyword, tickers in KEYWORD_TICKERS.items():
        if keyword in text:
            matched_keywords.append(keyword)
            for ticker in tickers[:3]:  # Max 3 per keyword
                found_tickers.add(ticker)

    # Se non troviamo nulla, usa i settori
    if not found_tickers:
        sectors = classify_sectors(title, summary)
        for sector in sectors:
            if sector in SECTOR_ETFS:
                for etf in SECTOR_ETFS[sector][:2]:
                    found_tickers.add(etf)

    return list(found_tickers)[:6], matched_keywords  # Max 6 tickers totali

def classify_sectors(title, summary=""):
    text = (title + " " + summary).lower()
    affected = []
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            affected.append(sector)
    return affected if affected else ["Indici Globali"]

def get_stock_data(ticker, days=5):
    """Ottiene dati storici da Yahoo Finance"""
    try:
        end = int(datetime.now().timestamp())
        start = int((datetime.now() - timedelta(days=days+2)).timestamp())
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start}&period2={end}&interval=1d"
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        data = resp.json()

        if data.get("chart", {}).get("result"):
            result = data["chart"]["result"][0]
            meta = result.get("meta", {})
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
                    "prices": prices[-5:],  # Ultimi 5 giorni
                    "current": prices[-1],
                    "change": change,
                    "high": max(prices),
                    "low": min(prices)
                }
    except Exception as e:
        print(f"Errore dati {ticker}: {e}")
    return None

def generate_mini_chart(data, width=30):
    """Genera mini chart ASCII"""
    if not data or len(data["prices"]) < 2:
        return "[N/A]"

    prices = data["prices"]
    min_p = min(prices)
    max_p = max(prices)
    range_p = max_p - min_p if max_p != min_p else 1

    change = data["change"]
    symbol = "🟢" if change >= 0 else "🔴"

    # Crea barra ASCII
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
    """Analizza se c'è un'opportunità"""
    data = get_stock_data(ticker, days=5)
    if not data:
        return None

    change = data["change"]

    # Logica opportunità
    if change < -5:
        opportunity = "⚠️ RIBASSO FORTE - Possibile oversold"
        action = "🎯 Controllare se è un buon punto di ingresso"
    elif change < -2:
        opportunity = "📉 RIBASSO MODERATO - Monitorare"
        action = "👀 Attendere conferma di inversione"
    elif change > 5:
        opportunity = "🚀 RIALZO FORTE - Momentum attivo"
        action = "⚡ Considerare profit-taking se già in posizione"
    elif change > 2:
        opportunity = "📈 RIALZO MODERATO - Trend positivo"
        action = "✅ Buon segnale, valutare ingresso"
    else:
        opportunity = "➡️ LATERALE - Nessuna opportunità evidente"
        action = "⏸️ Attendere breakout"

    return {
        "ticker": ticker,
        "chart": generate_mini_chart(data),
        "opportunity": opportunity,
        "action": action,
        "data": data
    }

def analyze_sentiment(title, summary=""):
    text = (title + " " + summary).lower()

    positive = ["surge", "rally", "gain", "growth", "profit", "beat", "strong", "boom", "rise", "bull", "rialzo", "aumento", "utile", "crescita", "breakthrough", "approval"]
    negative = ["crash", "fall", "drop", "loss", "bear", "recession", "crisis", "decline", "sell-off", "bearish", "ribasso", "caduta", "perdita", "crisi", "lawsuit", "recall"]

    pos = sum(1 for w in positive if w in text)
    neg = sum(1 for w in negative if w in text)

    if pos > neg:
        return "🟢 Positivo", "Potenziale rialzo", "📈 Considerare accumulo"
    elif neg > pos:
        return "🔴 Negativo", "Potenziale ribasso", "📉 Considerare hedging"
    return "🟡 Neutro", "Impatto incerto", "⏸️ Attendere"

def main():
    all_news = []

    for url in SOURCES:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:2]:
                all_news.append({
                    "title": entry.title,
                    "link": entry.link,
                    "source": url.split("/")[2],
                    "summary": entry.get("summary", "")[:300]
                })
        except Exception as e:
            print(f"Errore feed {url}: {e}")

    top_news = all_news[:5]
    now = datetime.now()

    # Messaggio 1: Notizie + Analisi
    msg1 = f"🎯 <b>AGENTE OPPORTUNITÀ FINANZIARIE</b>\n"
    msg1 += f"🕐 {now.strftime('%d/%m/%Y %H:%M')} | Ciclo: 4 ore\n"
    msg1 += "━" * 20 + "\n\n"

    all_tickers = set()

    for i, news in enumerate(top_news, 1):
        tickers, keywords = find_tickers_from_news(news["title"], news["summary"])
        all_tickers.update(tickers)
        sectors = classify_sectors(news["title"], news["summary"])
        sentiment, impact, rec = analyze_sentiment(news["title"], news["summary"])

        msg1 += f"<b>{i}. {news['title']}</b>\n"
        msg1 += f"   📰 {news['source']}\n"
        msg1 += f"   🔗 {news['link']}\n"
        if keywords:
            msg1 += f"   🔑 Keyword: {', '.join(keywords[:3])}\n"
        msg1 += f"   🏷️ Settori: {', '.join(sectors)}\n"
        msg1 += f"   {sentiment} | {impact}\n"
        msg1 += f"   💡 {rec}\n"
        if tickers:
            msg1 += f"   📊 Azioni coinvolte: {', '.join(tickers)}\n"
        msg1 += "\n"

    msg1 += "━" * 20 + "\n"

    # Invia messaggio 1
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg1, "parse_mode": "HTML", "disable_web_page_preview": True})
    time.sleep(1)

    # Messaggio 2: Grafici e Opportunità
    if all_tickers:
        msg2 = "📈 <b>ANALISI OPPORTUNITÀ - AZIONI COINVOLTE</b>\n"
        msg2 += "━" * 20 + "\n\n"

        opportunities = []
        for ticker in all_tickers:
            opp = analyze_opportunity(ticker, "")
            if opp:
                opportunities.append(opp)

        # Ordina per cambio (migliori opportunità prima)
        opportunities.sort(key=lambda x: abs(x["data"]["change"]), reverse=True)

        for opp in opportunities[:8]:  # Max 8
            msg2 += f"{opp['chart']}\n"
            msg2 += f"   {opp['opportunity']}\n"
            msg2 += f"   {opp['action']}\n\n"

        msg2 += "━" * 20 + "\n"
        msg2 += "⚠️ <b>Disclaimer:</b> Analisi educativa.\n"
        msg2 += "Non è consiglio finanziario.\n"
        msg2 += "Dati: Yahoo Finance (non ufficiale)"

        requests.post(url, json={"chat_id": CHAT_ID, "text": msg2, "parse_mode": "HTML", "disable_web_page_preview": True})

    print("✅ Agente completato!")

if __name__ == "__main__":
    main()
