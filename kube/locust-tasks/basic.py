import json
import random
import time
from kafka import KafkaProducer, KafkaConsumer
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "demo-topic")
KAFKA_SECURITY_PROTOCAL = os.getenv("KAFKA_SECURITY_PROTOCAL", "PLAINTEXT")
KAFKA_CA_LOCATION = os.getenv("KAFKA_CA_LOCATION", "/test/kafka-auth/ca.crt")
KAFKA_CERT_LOCATION = os.getenv("KAFKA_CERT_LOCATION", "/test/kafka-auth/user.crt")
KAFKA_KEY_LOCATION = os.getenv("KAFKA_KEY_LOCATION", "/test/kafka-auth/user.key")
KAFKA_BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "65536"))
producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
    "batch.size": KAFKA_BATCH_SIZE,
    "linger.ms": 10,
}
consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    # "group.id": "locust_group",
    "group.id": "demo-group",
    "auto.offset.reset": "earliest",
}

if KAFKA_SECURITY_PROTOCAL != "PLAINTEXT":
    for conf in [producer_config, consumer_config]:
        conf["security.protocol"] = KAFKA_SECURITY_PROTOCAL
        conf["ssl.ca.location"] = KAFKA_CA_LOCATION
        conf["ssl.certificate.location"] = KAFKA_CERT_LOCATION
        conf["ssl.key.location"] = KAFKA_KEY_LOCATION


def produce_random_data():

    producer = KafkaProducer(**producer_config)

    while True:
        random_data = {"timestamp": time.time(), "value": random.randint(1, 100)}

        serialized_data = json.dumps(random_data).encode("utf-8")
        producer.send("random_data", serialized_data)
        print(f"Sent message: {random_data}")

        time.sleep(1)

    producer.close()


def consume_random_data():

    consumer = KafkaConsumer(consumer_config)

    for message in consumer:
        deserialized_data = json.loads(message.value.decode("utf-8"))
        print(f"Received message: {deserialized_data}")

    consumer.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Kafka producer and consumer for random data"
    )
    parser.add_argument(
        "role",
        choices=["producer", "consumer"],
        help="Role to play (producer or consumer)",
    )
    args = parser.parse_args()

    if args.role == "producer":
        produce_random_data()
    elif args.role == "consumer":
        consume_random_data()
    else:
        print('Invalid role. Please choose "producer" or "consumer".')
