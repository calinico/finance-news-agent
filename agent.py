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
# AGENTE AI CON GRAFICI VISIVI REALI
# Genera immagini PNG e le invia su Telegram
# Frequenza: ogni 4 ore
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

# === DATABASE (abbreviato per lunghezza, completo nel file) ===
COUNTRY_ASSETS = {
    "united states": ["SPY", "QQQ", "DIA", "XLF", "TLT", "GLD", "USO"],
    "usa": ["SPY", "QQQ", "DIA", "XLF", "TLT", "GLD", "USO"],
    "fed": ["SPY", "QQQ", "XLF", "TLT", "KRE", "JPM", "BAC", "BLK"],
    "europe": ["VGK", "EZU", "EWG", "EWQ", "EWI", "FEZ"],
    "ecb": ["VGK", "EZU", "EWG", "EWQ", "DB", "UBS", "SAN"],
    "china": ["FXI", "MCHI", "KWEB", "ASHR", "BABA", "TCEHY", "JD"],
    "uk": ["EWU", "HSBC", "BP", "SHEL", "AZN", "UL"],
    "japan": ["EWJ", "DXJ", "TM", "SONY", "NTDOY"],
    "brazil": ["EWZ", "PBR", "VALE", "ITUB", "BBD"],
    "india": ["INDA", "EPI", "INFY", "TCS", "HDB"],
    "israel": ["ISRA", "EIS", "TEVA", "ICL", "CHKP"],
    "iran": ["USO", "UCO", "BNO", "XLE", "CVX", "XOM"],
    "saudi arabia": ["KSA", "USO", "XLE", "CVX", "XOM"],
    "russia": ["RSX", "ERUS", "USO", "GLD", "UNG", "WEAT"],
    "ukraine": ["USO", "UNG", "WEAT", "CORN", "GLD", "RSX"],
    "gold": ["GLD", "IAU", "GOLD", "NEM", "AEM", "KGC"],
    "oil": ["USO", "UCO", "BNO", "XLE", "XOM", "CVX", "COP", "OXY"],
    "bitcoin": ["MSTR", "COIN", "HOOD", "BITO", "RIOT", "MARA"],
}

KEYWORD_TICKERS = {
    "apple": ["AAPL", "AVGO", "LITE", "QRVO", "SWKS"],
    "microsoft": ["MSFT", "QLYS", "VEEV", "DOCU"],
    "google": ["GOOGL", "GOOG", "TDC", "TRMB"],
    "meta": ["META", "SNAP", "PINS", "MTCH"],
    "nvidia": ["NVDA", "AMD", "INTC", "MRVL", "QCOM"],
    "ai": ["NVDA", "AMD", "PLTR", "AI", "SNOW", "MDB", "DDOG", "NET"],
    "chip": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "SWKS", "QRVO"],
    "semiconductor": ["NVDA", "AMD", "INTC", "MRVL", "QCOM", "AMAT", "LRCX"],
    "cloud": ["MSFT", "AMZN", "GOOGL", "CRM", "NOW", "SNOW", "DDOG"],
    "cybersecurity": ["CRWD", "PANW", "FTNT", "ZS", "OKTA", "CYBR", "S", "NET"],
    "bank": ["JPM", "BAC", "WFC", "C", "GS", "MS", "PNC", "USB"],
    "banca": ["JPM", "BAC", "WFC", "C", "GS", "MS"],
    "fed": ["SPY", "QQQ", "XLF", "TLT", "KRE", "JPM", "BAC"],
    "ecb": ["VGK", "EZU", "EWG", "EWQ", "DB", "UBS"],
    "oil": ["XOM", "CVX", "COP", "EOG", "MPC", "VLO", "OXY"],
    "petrolio": ["XOM", "CVX", "COP", "EOG", "MPC", "VLO"],
    "gas": ["XOM", "CVX", "COP", "EOG", "MRO", "DVN", "EQT"],
    "energy": ["XOM", "CVX", "COP", "EOG", "XLE", "OXY", "DVN"],
    "renewable": ["ENPH", "SEDG", "FSLR", "RUN", "NOVA", "SPWR"],
    "solar": ["ENPH", "SEDG", "FSLR", "RUN", "NOVA", "SPWR"],
    "pharma": ["JNJ", "PFE", "MRK", "ABBV", "BMY", "LLY", "NVO", "AZN"],
    "drug": ["JNJ", "PFE", "MRK", "ABBV", "BMY", "LLY", "NVO", "AZN"],
    "vaccine": ["PFE", "MRNA", "BNTX", "NVAX", "GSK", "SNY", "JNJ"],
    "biotech": ["AMGN", "GILD", "BIIB", "VRTX", "REGN", "ALNY", "SRPT"],
    "tesla": ["TSLA", "RIVN", "LCID", "FSR", "NIO", "XPEV", "LI"],
    "ev": ["TSLA", "RIVN", "LCID", "FSR", "NIO", "XPEV", "LI", "QS"],
    "electric vehicle": ["TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI", "QS"],
    "automaker": ["F", "GM", "STLA", "TM", "HMC", "VWAGY", "BMWYY"],
    "car": ["F", "GM", "STLA", "TM", "HMC", "VWAGY", "BMWYY", "RACE"],
    "battery": ["TSLA", "QS", "MP", "ALB", "SQM", "LTHM", "PLL"],
    "bitcoin": ["MSTR", "COIN", "HOOD", "BITO", "RIOT", "MARA"],
    "ethereum": ["COIN", "HOOD", "BITW", "ETHE", "RIOT", "MARA"],
    "crypto": ["MSTR", "COIN", "HOOD", "BITO", "RIOT", "MARA", "HIVE"],
    "blockchain": ["IBM", "COIN", "MSTR", "RIOT", "MARA", "SQ", "PYPL"],
    "real estate": ["VNQ", "SPG", "O", "AMT", "PLD", "WPC", "NNN"],
    "housing": ["DHI", "LEN", "PHM", "TOL", "NVR", "KBH", "MTH"],
    "gold": ["GLD", "IAU", "GOLD", "NEM", "AEM", "KGC", "WPM", "RGLD"],
    "silver": ["SLV", "PAAS", "HL", "CDE", "EXK", "MAG", "FSM"],
    "copper": ["FCX", "SCCO", "TECK", "VALE", "RIO", "BHP"],
    "commodity": ["PDBC", "USCI", "GCC", "DJP", "DBC", "GSG"],
    "steel": ["NUE", "STLD", "MT", "VALE", "RIO", "BHP", "CLF"],
    "amazon": ["AMZN", "SHOP", "ETSY", "EBAY", "W", "OSTK"],
    "retail": ["WMT", "TGT", "COST", "HD", "LOW", "BBY", "TJX"],
    "consumer": ["PG", "KO", "PEP", "WMT", "COST", "MCD", "SBUX"],
    "defense": ["LMT", "NOC", "RTX", "GD", "BA", "HII", "KTOS"],
    "aerospace": ["BA", "AIR", "GE", "HON", "RTX", "LMT", "NOC"],
    "telecom": ["T", "VZ", "TMUS", "CMCSA", "CHTR", "LUMN"],
    "streaming": ["NFLX", "DIS", "WBD", "PARA", "ROKU", "FUBO"],
    "ozempic": ["NVO", "LLY", "PFE", "MRK", "ABBV", "BMY"],
    "wegovy": ["NVO", "LLY"],
    "weight loss": ["NVO", "LLY", "PFE", "MRK"],
    "diabetes": ["NVO", "LLY", "PFE", "MRK", "JNJ"],
    "spacex": ["RKLB", "ASTS", "SPCE", "LUNR", "VORB", "MNTS"],
    "space": ["RKLB", "ASTS", "SPCE", "LUNR", "VORB", "BA", "LMT"],
    "paramount": ["PARA", "WBD", "DIS", "NFLX", "CMCSA", "FOX"],
    "warner": ["WBD", "PARA", "DIS", "NFLX", "CMCSA", "FOX"],
    "mercedes": ["MBGYY", "VWAGY", "BMWYY", "TM", "HMC", "STLA"],
    "vat": ["SPY", "QQQ", "DIA", "XLU", "NEE", "DUK", "SO"],
    "electricity": ["XLU", "NEE", "DUK", "SO", "AEP", "EXC", "SRE"],
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
            dates = []
            for ts, close in zip(timestamps, closes):
                if close is not None:
                    prices.append(close)
                    dates.append(datetime.fromtimestamp(ts).strftime("%m/%d"))

            if len(prices) >= 2:
                change = ((prices[-1] - prices[0]) / prices[0]) * 100
                return {
                    "ticker": ticker,
                    "prices": prices[-5:],
                    "dates": dates[-5:],
                    "current": prices[-1],
                    "change": change,
                    "high": max(prices),
                    "low": min(prices)
                }
    except Exception as e:
        print(f"Errore dati {ticker}: {e}")
    return None

def generate_visual_chart(data):
    """Genera un grafico visivo reale usando matplotlib e lo invia su Telegram"""
    try:
        import matplotlib
        matplotlib.use('Agg')  # Backend non-interattivo
        import matplotlib.pyplot as plt
        import numpy as np

        prices = data["prices"]
        dates = data["dates"]
        ticker = data["ticker"]
        current = data["current"]
        change = data["change"]

        if len(prices) < 2:
            return None

        # Setup figura
        fig, ax = plt.subplots(figsize=(10, 4))
        fig.patch.set_facecolor('#0f172a')
        ax.set_facecolor('#0f172a')

        # Colori
        color_up = '#22c55e'
        color_down = '#ef4444'
        line_color = color_up if change >= 0 else color_down

        # Disegna linea
        x = np.arange(len(prices))
        ax.plot(x, prices, color=line_color, linewidth=3, marker='o', markersize=12,
                markerfacecolor=line_color, markeredgecolor='white', markeredgewidth=2)

        # Riempi area
        ax.fill_between(x, prices, alpha=0.15, color=line_color)

        # Etichette prezzo
        for i, (xi, yi) in enumerate(zip(x, prices)):
            ax.annotate(f'${yi:.2f}', (xi, yi), textcoords="offset points",
                       xytext=(0, 15), ha='center', fontsize=10, color='white', fontweight='bold')

        # Date
        ax.set_xticks(x)
        ax.set_xticklabels(dates, color='#94a3b8', fontsize=11)

        # Titolo
        symbol = '+' if change >= 0 else ''
        ax.set_title(f'{ticker}  {symbol}{change:.1f}%  |  ${current:.2f}',
                    color='white', fontsize=16, fontweight='bold', pad=20)

        # Rimuovi assi Y
        ax.set_yticks([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_color('#334155')

        # Box opportunità
        if change < -5:
            opp_text = "RIBASSO FORTE - Possibile oversold\nControllare punto ingresso"
        elif change < -2:
            opp_text = "RIBASSO MODERATO - Monitorare\nAttendere conferma inversione"
        elif change > 5:
            opp_text = "RIALZO FORTE - Momentum attivo\nConsiderare profit-taking"
        elif change > 2:
            opp_text = "RIALZO MODERATO - Trend positivo\nValutare ingresso"
        else:
            opp_text = "LATERALE - Nessuna opportunità\nAttendere breakout"

        props = dict(boxstyle='round,pad=0.5', facecolor='#1e293b', edgecolor=line_color, linewidth=2)
        ax.text(0.5, -0.22, opp_text, transform=ax.transAxes, fontsize=10,
                verticalalignment='top', horizontalalignment='center', color='white', bbox=props)

        plt.tight_layout()

        # Salva in buffer
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
    """Invia foto su Telegram"""
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

    # === MESSAGGIO 3: GRAFICI VISIVI ===
    if all_tickers:
        msg3 = "📈 <b>GRAFICI VISIVI - AZIONI COINVOLTE</b>\n"
        msg3 += "━" * 20 + "\n"
        msg3 += "⚠️ Disclaimer: Analisi educativa. Non consiglio finanziario.\n\n"
        send_telegram_message(msg3)
        time.sleep(1)

        # Genera e invia grafici visivi
        opportunities = []
        for ticker in all_tickers:
            data = get_stock_data(ticker, days=5)
            if data:
                chart_buf = generate_visual_chart(data)
                if chart_buf:
                    change = data["change"]
                    if change < -5:
                        caption = f"<b>{ticker}</b>\n⚠️ RIBASSO FORTE - Possibile oversold\n🎯 Controllare punto ingresso"
                    elif change < -2:
                        caption = f"<b>{ticker}</b>\n📉 RIBASSO MODERATO - Monitorare\n👀 Attendere conferma inversione"
                    elif change > 5:
                        caption = f"<b>{ticker}</b>\n🚀 RIALZO FORTE - Momentum attivo\n⚡ Considerare profit-taking"
                    elif change > 2:
                        caption = f"<b>{ticker}</b>\n📈 RIALZO MODERATO - Trend positivo\n✅ Valutare ingresso"
                    else:
                        caption = f"<b>{ticker}</b>\n➡️ LATERALE - Nessuna opportunità\n⏸️ Attendere breakout"

                    send_photo_to_telegram(chart_buf, caption)
                    time.sleep(0.5)

    print("✅ Agente con Grafici Visivi Completato!")

if __name__ == "__main__":
    main()