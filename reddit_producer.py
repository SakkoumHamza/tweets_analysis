# reddit_producer.py
import requests
import time
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    response = requests.get(
        "https://www.reddit.com/r/worldnews/new.json?limit=10",
        headers={'User-agent': 'Mozilla/5.0'}
    )
    for post in response.json()["data"]["children"]:
        text = post["data"]["title"]
        producer.send("reddi", {"text": text})
        print(f"Sent: {text}")
    time.sleep(15)
