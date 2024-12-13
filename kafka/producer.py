import json
from kafka import KafkaProducer
import time
import os
import utils

API_KEY = os.getenv("API_KEY")
CATEGORIES = ["business", "entertainment", "general", 
              "health", "science", 
              "sports", "technology"]
COUNTRIES = ['ae', 'ar', 'at', 'au', 'be', 'bg', 'br', 'ca', 'ch',
             'cn', 'co', 'cu', 'cz', 'de', 'eg', 'fr', 'gb', 'gr', 
             'hk', 'hu', 'id', 'ie', 'il', 'in', 'it', 'jp', 'kr', 
             'lt', 'lv', 'ma', 'mx', 'my', 'ng', 'nl', 'no', 'nz', 
             'ph', 'pl', 'pt', 'ro', 'rs', 'ru', 'sa', 'se', 'sg', 
             'si', 'sk', 'th', 'tr', 'tw', 'ua', 'us', 've', 'za']

TOPIC = 'news-topic'

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sent_article_urls = set()

def process_and_send_articles(articles, category=None, country=None):
    for article in articles:
        try:
            article_url = article['url']
            if article_url in sent_article_urls:
                print(f"Duplicate article found: {article_url}, skipping.")
                continue
            
            sent_article_urls.add(article_url)
            message = {
                "category": category,
                "country": country,
                "title": article['title'],
                "content": article['content'],
                "publisher": article['source']['name'],
                "publishedAt": article['publishedAt'],
                "url": article['url']
            }

            producer.send(TOPIC, value=message)
            print('Sent article:', message)

        except Exception as e:
            print(f"Error processing article: {e}")

def fetch_and_send():
    for category in CATEGORIES:
        print(f"Fetching articles for category: {category}")
        articles = utils.fetch_data_by_category(api_key=API_KEY, category=category)
        if articles:
            process_and_send_articles(articles, category=category)
        else:
            print(f"No articles found for category: {category}")

    for country in COUNTRIES:
        print(f"Fetching articles for country: {country}")
        articles = utils.fetch_data_by_category(api_key=API_KEY, country=country)
        if articles:
            process_and_send_articles(articles, country=country)
        else:
            print(f"No articles found for country: {country}")

while True:
    fetch_and_send()
    print("Batch sent to Kafka.")
    time.sleep(60) 