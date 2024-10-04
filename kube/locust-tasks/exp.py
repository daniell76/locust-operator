import logging
import os
import time
from locust import User, task, between
from locust.env import Environment
from confluent_kafka import Producer, KafkaError
from locust.stats import RequestStats
from concurrent.futures import ThreadPoolExecutor
import json
import signal


class KafkaUser(User):
    wait_time = between(1, 5)

    def __init__(self, environment, **kwargs):
        super().__init__(environment, **kwargs)
        self.producer = Producer(
            {
                "bootstrap.servers": os.getenv(
                    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
                ),
                "acks": "all",
                "retries": 5,
                "batch.size": 65536,
                "linger.ms": 10,
            }
        )
        self.logger = logging.getLogger(__name__)
        self.request_stats = RequestStats()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.total_data_sent = 0
        self.successful_requests = 0
        self.failed_requests = 0

        signal.signal(signal.SIGINT, self.graceful_shutdown)
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    @task
    def send_kafka_message(self):
        message = f"Hello from Locust at {time.time()}"
        message_size = len(message.encode("utf-8"))

        def delivery_report(err, msg):
            if err is not None:
                self.logger.error("Message delivery failed: %s", err)
                self.failed_requests += 1
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
                "random_data", message.encode("utf-8"), callback=delivery_report
            )
            self.producer.poll(0)
        except KafkaError as e:
            self.logger.error("Kafka error: %s", e)
            self.failed_requests += 1
            self.environment.events.request.fire(
                request_type="send_kafka_message",
                name="Kafka",
                response_time=0,
                response_length=0,
                exception=e,
                context=None,
            )

    def on_stop(self):
        self.producer.flush()
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

if __name__ == "__main__":
    from locust import run_single_user

    run_single_user(KafkaUser)
