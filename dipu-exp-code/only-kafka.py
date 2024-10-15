import json
import random
import string
import threading
import time
from confluent_kafka import Producer, Consumer, KafkaError
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s:%(message)s")

kafka_topic = "dipoo_data"
kafka_bootstrap_servers = "localhost:9092"


def generate_random_json(size_kb):
    data = "".join(
        random.choices(string.ascii_letters + string.digits, k=size_kb * 1024)
    )
    return json.dumps({"data": data})


class KafkaProducer:
    def __init__(self, producer_id):
        self.producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
        self.producer_id = producer_id

    def send_message(self):
        while True:
            message_size_kb = 100
            message = generate_random_json(message_size_kb)
            try:
                self.producer.produce(
                    kafka_topic, message, callback=self.delivery_report
                )
                self.producer.poll(0)
                logging.info(
                    f"Producer {self.producer_id} produced message of size {len(message)}"
                )
            except Exception as e:
                logging.error(
                    f"Producer {self.producer_id} failed to produce message: {e}"
                )
            time.sleep(1)

    def delivery_report(self, err, msg):
        if err:
            logging.error(f"Producer {self.producer_id} message delivery failed: {err}")
        else:
            logging.info(
                f"Producer {self.producer_id} message delivered to {msg.topic()} [{msg.partition()}]"
            )


class KafkaConsumer:
    def __init__(self, consumer_id):
        self.consumer = Consumer(
            {
                "bootstrap.servers": kafka_bootstrap_servers,
                "group.id": "my_group",
                "auto.offset.reset": "earliest",
            }
        )
        self.consumer_id = consumer_id
        self.consumer.subscribe([kafka_topic])

    def consume_messages(self):
        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                else:
                    logging.error(f"Consumer {self.consumer_id} error: {msg.error()}")
            else:
                logging.info(
                    f"Consumer {self.consumer_id} consumed message of size {len(msg.value())}"
                )
                with open("data.txt", "a") as file:
                    file.write(msg.value().decode("utf-8") + "\n")


def start_producer():
    producer = KafkaProducer(producer_id=1)
    threading.Thread(target=producer.send_message).start()


def start_consumer():
    consumer = KafkaConsumer(consumer_id=1)
    threading.Thread(target=consumer.consume_messages).start()


if __name__ == "__main__":
    mode = input("Enter mode (producer/consumer): ").strip().lower()

    if mode == "producer":
        start_producer()
    elif mode == "consumer":
        start_consumer()
    else:
        print("Invalid mode. Please use 'producer' or 'consumer'.")
