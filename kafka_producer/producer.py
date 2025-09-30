from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

products = ["Laptop", "Phone", "Headphones", "Keyboard", "Mouse"]

while True:
    event = {
        "order_id": random.randint(1, 10000),
        "user_id": random.randint(1, 500),
        "product": random.choice(products),
        "quantity": random.randint(1, 3),
        "price": random.randint(20, 2000),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send("orders", value=event)
    print(f"Produced: {event}")
    time.sleep(2)
