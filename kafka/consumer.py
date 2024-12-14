from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import requests
import json

TOPIC = "news-topic"
es = Elasticsearch([{"host": "elasticsearch", "port": 9200}])

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:29092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Start reading messages from the beginning
    enable_auto_commit=True  # Automatically commit offsets
)

def fetch_data(endpoint):
    ''' Fetch data from the API'''
    try:
        response = requests.get(endpoint)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content,"html.parser")
            return soup.get_text()
        else:
            print(f"API error: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

def process_message(message):
    try:
        print("Processing article:")
        print(f"Message ID: {message.get('id', 'N/A')}")
        print(f"Category: {message.get('category', 'N/A')}")
        print(f"Country: {message.get('country', 'N/A')}")
        print(f"Title: {message['title']}")
        print(f"Content: {message['content'][:100]}..." if message.get('content') else "Content: N/A")
        print(f"Publisher: {message.get('publisher', 'N/A')}")
        print(f"Published At: {message.get('publishedAt', 'N/A')}")
        print(f"URL: {message.get('url', 'N/A')}")
        print("-")

    except Exception as e:
        print(f"Error processing message: {e}")

print(f"Subscribed to topic '{TOPIC}' and waiting for messages...")

msg_counter = 1

for msg in consumer:
    try:
        message = msg.value
        message["content"] = fetch_data(message["url"])
        message["id"] = msg_counter

        process_message(message)
        es.index(index="news",id=msg_counter,document=message)
        msg_counter += 1

    except Exception as e:
        print(f"Error reading message: {e}")
