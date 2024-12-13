from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

TOPIC = "news-topic"
es = Elasticsearch([{"host": "elasticsearch", "port": 9200}])

consumer = KafkaConsumer(TOPIC, bootstrap_servers="kafka:9092", auto_offset_reset="earliest", value_deserializer=lambda x: x.decode("utf-8"))

def index_data(document):
    es.index(index="news-index", body=json.loads(document))

for message in consumer:
    print(f"Consuming message: {message.value}")
    index_data(message.value)
