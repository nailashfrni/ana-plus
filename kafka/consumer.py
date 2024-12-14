from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

TOPIC = "news-topic"
# es = Elasticsearch([{"host": "elasticsearch", "port": 9200}])

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:29092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Start reading messages from the beginning
    enable_auto_commit=True  # Automatically commit offsets
)

def process_message(message):
    try:
        print("Processing article:")
        print(f"Category: {message.get('category', 'N/A')}")
        print(f"Country: {message.get('country', 'N/A')}")
        print(f"Title: {message['title']}")
        print(f"Content: {message['content'][:100]}..." if message['content'] else "Content: N/A")
        print(f"Publisher: {message['publisher']}")
        print(f"Published At: {message['publishedAt']}")
        print(f"URL: {message['url']}")
        print("-")

    except Exception as e:
        print(f"Error processing message: {e}")

print(f"Subscribed to topic '{TOPIC}' and waiting for messages...")

for msg in consumer:
    try:
        message = msg.value  # Deserialized message
        process_message(message)
    except Exception as e:
        print(f"Error reading message: {e}")