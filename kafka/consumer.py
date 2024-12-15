from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

KAFKA_TOPIC = 'floods-topic'
KAFKA_BOOTSTRAP_SERVER = 'localhost:29092'

ES_HOST = "http://localhost:9200"
ES_INDEX = 'floods'

es = Elasticsearch([ES_HOST])

if not es.indices.exists(index=ES_INDEX):
    es.indices.create(index=ES_INDEX, ignore=400)
    print(f"Created index: {ES_INDEX}")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Start from the earliest message
    enable_auto_commit=True,       # Auto commit offsets
    group_id='floods-group'
)

def index_to_elasticsearch(data):
    """
    Index a document into Elasticsearch.
    """
    try:
        doc_id = data.get("id")

        es.index(index=ES_INDEX, id=doc_id, body=data)
        print(f"Indexed document ID: {doc_id}")
    except Exception as e:
        print(f"Error indexing data: {e}")

def consume_messages():
    """
    Consume messages from Kafka and index them into Elasticsearch.
    """
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
            data = message.value
            index_to_elasticsearch(data)

    except Exception as e:
        print(f"Error consuming messages: {e}")

if __name__ == "__main__":
    print("Starting Kafka consumer...")
    consume_messages()
