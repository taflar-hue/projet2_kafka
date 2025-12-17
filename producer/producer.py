from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    message = {
        "capteur_id": random.randint(1, 5),
        "temperature": round(random.uniform(18, 35), 2),
        "humidity": round(random.uniform(30, 80), 2),
        "timestamp": time.time()
    }
    producer.send("capteurs", message)
    print("Message envoyé :", message)
    time.sleep(1)
