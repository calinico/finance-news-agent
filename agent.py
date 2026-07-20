import feedparser
import requests
import os
from datetime import datetime

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SOURCES = [
    "https://www.ft.com/rss/home/global",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.reuters.com/rssFeed/businessNews",
    "https://www.ilsole24ore.com/rss/finanza.xml",
]

def main():
    all_news = []
    for url in SOURCES:
        feed = feedparser.parse(url)
        for entry in feed.entries[:3]:
            all_news.append({
                "title": entry.title,
                "link": entry.link,
                "source": url
            })
    
    top_news = all_news[:5]
    
    message = "📰 <b>Notizie Finanziarie del Giorno</b>\n\n"
    message += f"📅 {datetime.now().strftime('%d/%m/%Y')}\n\n"
    
    for i, news in enumerate(top_news, 1):
        message += f"{i}. {news['title']}\n"
        message += f"   🔗 {news['link']}\n\n"
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        print("✅ Messaggio inviato con successo!")
    else:
        print(f"❌ Errore: {response.text}")

if __name__ == "__main__":
    main()
