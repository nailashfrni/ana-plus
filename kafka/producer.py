import json
from kafka import KafkaProducer
import time
import requests

ROOT = "https://environment.data.gov.uk/flood-monitoring"
TOPIC = 'floods-topic'

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    api_version=(2,0,2),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sent_floods_data = set()

def extract_id_from_uri(uri):
    return uri.split('/')[-1] if uri else None

def process_and_send_data(items):
    for item in items:
        flood_id = extract_id_from_uri(item.get('@id', ''))

        if (flood_id in sent_floods_data):
            continue

        sent_floods_data.add(flood_id)
    
        message = {
            "id": flood_id,
            "description": item.get("description"),
            "ea_area": item.get("eaAreaName"),
            "ea_region": item.get("eaRegionName"),
            "county": item.get("floodArea", {}).get("county"),
            "notation": item.get("floodArea", {}).get("notation"),
            "river_or_sea": item.get("floodArea", {}).get("riverOrSea"),
            "polygon_url": item.get("floodArea", {}).get("polygon"),
            "severity": item.get("severity"),
            "severity_level": item.get("severityLevel"),
            "is_tidal": item.get("isTidal"),
            "time_message_changed": item.get("timeMessageChanged"),
            "time_raised": item.get("timeRaised"),
            "time_severity_changed": item.get("timeSeverityChanged")
        }

        producer.send(TOPIC, value=message)
        print("SENT", message)

def fetch_and_send():
    res = requests.get(f"{ROOT}/id/floods")

    if (res.status_code == 200):
        process_and_send_data(res.json().get("items"))
    else:
        print("Fail to fetch API")

while True:
    fetch_and_send()
    print("Batch sent to Kafka.")
    time.sleep(60)
