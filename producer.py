
import os
import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Read configuration from environment variables
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")
SASL_USERNAME = os.environ.get("SASL_USERNAME")
SASL_PASSWORD = os.environ.get("SASL_PASSWORD")
INTERVAL_MS = int(os.environ.get("INTERVAL_MS", "1000"))  # Default to 1000ms

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    sasl_mechanism='PLAIN',
    security_protocol='SASL_SSL',
    sasl_plain_username=SASL_USERNAME,
    sasl_plain_password=SASL_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_vitals():
    age = random.randint(18, 85)
    systolic_bp = random.randint(90, 160)
    diastolic_bp = random.randint(60, 100)
    oxygen_saturation = random.randint(95, 100)
    temperature = round(random.uniform(97.0, 99.0), 1)
    heart_rate = random.randint(60, 100)
    breaths_per_minute = random.randint(12, 20)

    # Introduce anomalies (approx. 1% chance)
    if random.random() < 0.01:
        heart_rate = random.randint(150, 220)  # Extremely high heart rate
        breaths_per_minute = random.randint(30, 50) #Extremely high breaths

    vitals = {
        "age": age,
        "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp,
        "oxygen_saturation": oxygen_saturation,
        "temperature": temperature,
        "heart_rate": heart_rate,
        "breaths_per_minute": breaths_per_minute
    }
    return vitals

if __name__ == "__main__":
    while True:
        vitals_data = generate_vitals()
        try:
            producer.send(KAFKA_TOPIC, vitals_data)
            print(f"Sent: {vitals_data}")
        except KafkaError as e:
            print(f"Error sending message: {e}")
        time.sleep(INTERVAL_MS / 1000)
