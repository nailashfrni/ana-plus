import time
import requests
from elasticsearch import Elasticsearch

ELASTICSEARCH_HOST = 'http://localhost:9200'
es = Elasticsearch([ELASTICSEARCH_HOST])

API_KEY = "your_api_key"
EVERYTHING_ENDPOINT = "https://newsapi.org/v2/everything"
HEADLINES_ENDPOINT = "https://newsapi.org/v2/top-headlines"

def create_index(index_name, mappings):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mappings)
        print(f"Index '{index_name}' created.")
    else:
        print(f"Index '{index_name}' already exists.")

def index_to_elasticsearch(index_name, document):
    '''Index a single document'''
    try:
        es.index(index=index_name, body=document)
        print(f"Indexed document: {document['title']}")
    except Exception as e:
        print(f"Error indexing document: {e}")

def fetch_data(endpoint, params):
    ''' Fetch data from the API'''
    try:
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            return response.json().get("articles", [])
        else:
            print(f"API error: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def stream_data():
    mappings = {
        "mappings": {
            "properties": {
                "source": { "type": "object" },
                "author": { "type": "text" },
                "title": { "type": "text" },
                "description": { "type": "text" },
                "url": { "type": "text" },
                "urlToImage": { "type": "text" },
                "publishedAt": { "type": "date" },
                "content": { "type": "text" }
            }
        }
    }

    create_index('everything-index', mappings)
    create_index('headlines-index', mappings)

    while True:
        print("Fetching new data...")

        everything_params = {
            "q": "Apple",
            "from": "2024-12-12",
            "sortBy": "popularity",
            "apiKey": API_KEY
        }
        everything_data = fetch_data(EVERYTHING_ENDPOINT, everything_params)
        for article in everything_data:
            index_to_elasticsearch('everything-index', article)

        headlines_params = {
            "country": "us",
            "apiKey": API_KEY
        }
        headlines_data = fetch_data(HEADLINES_ENDPOINT, headlines_params)
        for article in headlines_data:
            index_to_elasticsearch('headlines-index', article)

        print("Waiting for the next fetch cycle...")
        time.sleep(60)  # Fetch data every 60 seconds

if __name__ == "__main__":
    stream_data()
