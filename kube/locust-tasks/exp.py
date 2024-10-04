import logging
import os
import time
import json
import signal
from locust import User, task, between
from locust.env import Environment
import subprocess

try:
    from confluent_kafka import Producer, KafkaError, Consumer, KafkaException
except ModuleNotFoundError:
    subprocess.check_call(["pip", "install", "confluent-kafka"])
    from confluent_kafka import Producer, KafkaError, Consumer, KafkaException
from locust.stats import RequestStats
from concurrent.futures import ThreadPoolExecutor

try:
    from prometheus_client import start_http_server, Summary, Counter, Gauge
except ModuleNotFoundError:
    subprocess.check_call(["pip", "install", "prometheus_client"])
    from prometheus_client import start_http_server, Summary, Counter, Gauge

REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")
SUCCESSFUL_REQUESTS = Counter("successful_requests_total", "Total successful requests")
FAILED_REQUESTS = Counter("failed_requests_total", "Total failed requests")
LATENCY = Summary("message_latency_seconds", "Latency of messages")
CONSUMER_LAG = Gauge("consumer_lag", "Consumer lag in messages")


class KafkaUser(User):
    wait_time = between(1, 5)

    def __init__(self, environment, **kwargs):
        super().__init__(environment, **kwargs)
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "demo-topic")
        self.KAFKA_SECURITY_PROTOCAL = os.getenv("KAFKA_SECURITY_PROTOCAL", "PLAINTEXT")
        self.KAFKA_CA_LOCATION = os.getenv(
            "KAFKA_CA_LOCATION", "/test/kafka-auth/ca.crt"
        )
        self.KAFKA_CERT_LOCATION = os.getenv(
            "KAFKA_CERT_LOCATION", "/test/kafka-auth/user.crt"
        )
        self.KAFKA_KEY_LOCATION = os.getenv(
            "KAFKA_KEY_LOCATION", "/test/kafka-auth/user.key"
        )
        self.KAFKA_BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "65536"))
        self.producer_config = {
            "bootstrap.servers": self.KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "retries": 5,
            "batch.size": self.KAFKA_BATCH_SIZE,
            "linger.ms": 10,
        }
        self.consumer_config = {
            "bootstrap.servers": self.KAFKA_BOOTSTRAP_SERVERS,
            # "group.id": "locust_group",
            "group.id": "demo-group",
            "auto.offset.reset": "earliest",
        }
        if self.KAFKA_SECURITY_PROTOCAL != "PLAINTEXT":
            for conf in [self.producer_config, self.consumer_config]:
                conf["security.protocol"] = self.KAFKA_SECURITY_PROTOCAL
                conf["ssl.ca.location"] = self.KAFKA_CA_LOCATION
                conf["ssl.certificate.location"] = self.KAFKA_CERT_LOCATION
                conf["ssl.key.location"] = self.KAFKA_KEY_LOCATION
        self.producer = Producer(self.producer_config)
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([self.KAFKA_TOPIC])
        self.logger = logging.getLogger(__name__)
        self.request_stats = RequestStats()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.total_data_sent = 0
        self.successful_requests = 0
        self.failed_requests = 0

        signal.signal(signal.SIGINT, self.graceful_shutdown)
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

        # start_http_server(8001)

    @task
    @REQUEST_TIME.time()
    def send_kafka_message(self):
        timestamp = time.time()
        message = f"Hello from Locust at {timestamp}"
        message_size = len(message.encode("utf-8"))

        def delivery_report(err, msg):
            if err is not None:
                self.logger.error("Message delivery failed: %s", err)
                self.failed_requests += 1
                FAILED_REQUESTS.inc()
                self.environment.events.request_failure.fire(
                    request_type="send_kafka_message",
                    name="Kafka",
                    response_time=0,
                    response_length=0,
                    exception=err,
                )
            else:
                response_time = time.time() - float(
                    msg.value().decode("utf-8").split()[-1]
                )
                self.successful_requests += 1
                SUCCESSFUL_REQUESTS.inc()
                LATENCY.observe(response_time)
                self.total_data_sent += message_size
                self.request_stats.log_request(
                    "send_kafka_message", response_time, message_size, message_size
                )
                self.environment.events.request.fire(
                    request_type="send_kafka_message",
                    name="Kafka",
                    response_time=response_time,
                    response_length=message_size,
                    exception=None,
                    context=None,
                )

        try:
            self.producer.produce(
                self.KAFKA_TOPIC, message.encode("utf-8"), callback=delivery_report
            )
            self.producer.poll(0)
        except KafkaError as e:
            self.logger.error("Kafka error: %s", e)
            self.failed_requests += 1
            FAILED_REQUESTS.inc()
            self.environment.events.request.fire(
                request_type="send_kafka_message",
                name="Kafka",
                response_time=0,
                response_length=0,
                exception=e,
                context=None,
            )

    @task
    def consume_kafka_message(self):
        try:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                return
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    self.logger.info(
                        "End of partition reached %s %d %d",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                message = msg.value().decode("utf-8")
                self.logger.info(f"Consumed message: {message}")
                try:
                    parts = message.split()
                    if len(parts) < 5:
                        raise ValueError("Unexpected message format")
                    timestamp = float(parts[-1])
                    response_time = time.time() - timestamp
                    LATENCY.observe(response_time)
                    CONSUMER_LAG.set(msg.offset())
                except ValueError as e:
                    self.logger.error(f"Error parsing message timestamp: {e}")
                    self.logger.error(f"Message content: {message}")
        except KafkaException as e:
            self.logger.error("Kafka error: %s", e)

    def on_stop(self):
        self.producer.flush()
        self.consumer.close()
        self.executor.shutdown()

    def graceful_shutdown(self, signum, frame):
        self.logger.info("Shutting down gracefully...")
        self.on_stop()
        self.environment.runner.quit()


def on_request_success(
    request_type, name, response_time, response_length, exception, context, **kwargs
):
    if request_type == "send_kafka_message":
        user = kwargs["user"]
        total_requests = user.request_stats.num_requests
        avg_response_time = (
            user.request_stats.total_response_time / total_requests
            if total_requests > 0
            else 0
        )
        avg_message_size = (
            user.total_data_sent / user.successful_requests
            if user.successful_requests > 0
            else 0
        )
        logging.info(f"Average response time: {avg_response_time:.2f} ms")
        logging.info(f"Total requests: {total_requests}")
        logging.info(f"Successful requests: {user.successful_requests}")
        logging.info(f"Failed requests: {user.failed_requests}")
        logging.info(f"Total data sent: {user.total_data_sent} bytes")
        logging.info(f"Average message size: {avg_message_size:.2f} bytes")


environment = Environment()
environment.events.request.add_listener(on_request_success)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# if __name__ == "__main__":
#     from locust import run_single_user
#
#     run_single_user(KafkaUser)
