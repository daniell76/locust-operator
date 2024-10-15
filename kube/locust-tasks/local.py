import logging
import os
import time
from dataclasses import dataclass

from locust import HttpUser, task, between, events
import random
import string
import subprocess
try:
    from confluent_kafka import Producer, KafkaError, Consumer, KafkaException
except ModuleNotFoundError:
    subprocess.check_call(['pip', 'install', "confluent-kafka"])
    from confluent_kafka import Producer, KafkaError, Consumer, KafkaException


@dataclass
class TestParams:
    def __post_init__(self):
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "demo-topic")
        self.KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "perf-test")
        self.KAFKA_MSG_SIZE = int(os.getenv("KAFKA_MSG_SIZE", "10"))
        self.KAFKA_MSG_NUMBER_PER_USER = int(os.getenv("KAFKA_MSG_NUMBER_PER_USER", "100"))
        self.KAFKA_MSG_INTERVAL_MS = int(os.getenv("KAFKA_MSG_INTERVAL", "100"))  # wait time between messages
        self.KAFKA_SECURITY_PROTOCAL = os.getenv("KAFKA_SECURITY_PROTOCAL", "PLAINTEXT")
        self.KAFKA_CA_LOCATION = os.getenv("KAFKA_CA_LOCATION", "/test/kafka-auth/ca.crt")
        self.KAFKA_CERT_LOCATION = os.getenv("KAFKA_CERT_LOCATION", "/test/kafka-auth/user.crt")
        self.KAFKA_KEY_LOCATION = os.getenv("KAFKA_KEY_LOCATION", "/test/kafka-auth/user.key")
        self.KAFKA_BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "16384"))


def generate_random_text(size) -> str:
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for i in range(size))


class KafkaProducer(HttpUser):
    wait_time = between(1, 5)
    params = TestParams()
    logger = logging.getLogger(__name__)
    producer = None

    def on_start(self):
        producer_config = {
            "bootstrap.servers": self.params.KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "retries": 5,
            "batch.size": self.params.KAFKA_BATCH_SIZE,
            "linger.ms": 10
        }
        if self.params.KAFKA_SECURITY_PROTOCAL != "PLAINTEXT":
            for conf in [producer_config]:
                conf["security.protocol"] = self.params.KAFKA_SECURITY_PROTOCAL
                conf["ssl.ca.location"] = self.params.KAFKA_CA_LOCATION
                conf["ssl.certificate.location"] = self.params.KAFKA_CERT_LOCATION
                conf["ssl.key.location"] = self.params.KAFKA_KEY_LOCATION
        self.producer = Producer(producer_config)  # Create Kafka producer

    @task
    def send_kafka_message(self):
        message_size = self.params.KAFKA_MSG_SIZE
        for _ in range(self.params.KAFKA_MSG_NUMBER_PER_USER):
            message = generate_random_text(message_size)
            start_time = time.time()
            try:
                self.producer.produce(self.params.KAFKA_TOPIC, message)
                self.producer.poll(0)  # Poll for delivery reports
                events.request.fire(request_type='kafka', name='produce_message', response_length = message_size,
                                            response_time=time.time() - start_time, exception=None)
            except Exception as e:
                # self.logger.error("Kafka error: %s", e)
                events.request.fire(request_type='kafka', name='produce_message', response_length = message_size,
                                            response_time=time.time() - start_time, exception=e)
            time.sleep(self.params.KAFKA_MSG_INTERVAL_MS/1000)

    def on_stop(self):
        self.producer.flush()

    def graceful_shutdown(self, signum, frame):
        self.on_stop()
        self.environment.runner.quit()

