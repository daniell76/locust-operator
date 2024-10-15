import logging
import json
import random
import string
import threading
import time
import sys
from confluent_kafka import Producer, Consumer, KafkaError
from locust import HttpUser, TaskSet, task, between, events
from locust.env import Environment
from locust.runners import MasterRunner

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s:%(message)s")

# Kafka configurations
kafka_topic = "dipoo_data"
kafka_bootstrap_servers = "localhost:9092"


# Generate random JSON data
def generate_random_json(size_kb):
    data = "".join(
        random.choices(string.ascii_letters + string.digits, k=size_kb * 1024)
    )
    return json.dumps({"data": data})


class KafkaProducerTaskSet(TaskSet):
    @task
    def send_message(self):
        message_size_kb = random.choice(
            range(10, 101, 10)
        )  # Uniform random size between 10 KB and 100 KB
        message = generate_random_json(message_size_kb)
        start_time = time.time()
        try:
            self.producer.produce(kafka_topic, message, callback=self.delivery_report)
            self.producer.poll(0)
            end_time = time.time()
            response_time = (end_time - start_time) * 1000
            logging.info(
                f"Produced message of size {len(message)} in {response_time:.2f} ms"
            )
            events.request.fire(
                request_type="Kafka",
                name="producer",
                response_time=response_time,
                response_length=len(message),
                exception=None,
            )
        except Exception as e:
            end_time = time.time()
            response_time = (end_time - start_time) * 1000
            logging.error(
                f"Failed to produce message of size {len(message)} in {response_time:.2f} ms: {e}"
            )
            events.request.fire(
                request_type="Kafka",
                name="producer",
                response_time=response_time,
                response_length=len(message),
                exception=e,
            )
        time.sleep(1)

    def delivery_report(self, err, msg):
        if err:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


class KafkaConsumerTaskSet(TaskSet):
    @task
    def consume_messages(self):
        start_time = time.time()
        msg = self.consumer.poll(timeout=1.0)
        if msg is None:
            return
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return
            else:
                logging.error(f"Consumer error: {msg.error()}")
                end_time = time.time()
                response_time = (end_time - start_time) * 1000
                logging.error(
                    f"Failed to consume message in {response_time:.2f} ms: {msg.error()}"
                )
                events.request.fire(
                    request_type="Kafka",
                    name="consumer",
                    response_time=response_time,
                    response_length=0,
                    exception=msg.error(),
                )
        else:
            logging.info(f"Consumed message of size {len(msg.value())}")
            end_time = time.time()
            response_time = (end_time - start_time) * 1000
            logging.info(f"Consumed message in {response_time:.2f} ms")
            with open("data.txt", "a") as file:
                file.write(msg.value().decode("utf-8") + "\n")
            events.request.fire(
                request_type="Kafka",
                name="consumer",
                response_time=response_time,
                response_length=len(msg.value()),
                exception=None,
            )


class KafkaProducerUser(HttpUser):
    tasks = [KafkaProducerTaskSet]
    wait_time = between(1, 2)
    host = "localhost:9092"
    producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})


class KafkaConsumerUser(HttpUser):
    tasks = [KafkaConsumerTaskSet]
    wait_time = between(1, 2)
    host = "localhost:9092"
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap_servers,
            "group.id": "my_group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([kafka_topic])


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    if not isinstance(environment.runner, MasterRunner):
        logging.info("Beginning test setup")
    else:
        logging.info("Started test from Master node")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    if not isinstance(environment.runner, MasterRunner):
        logging.info("Cleaning up test data")
    else:
        logging.info("Stopped test from Master node")


if __name__ == "__main__":
    environment = Environment(user_classes=[KafkaProducerUser, KafkaConsumerUser])
    environment.create_local_runner()

    if len(sys.argv) != 2:
        print("Usage: python start.py [producer|consumer]")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "producer":
        logging.info("Starting Kafka Producer Performance Test")
        environment.create_local_runner()
        environment.runner.start(1, spawn_rate=1)
    elif mode == "consumer":
        logging.info("Starting Kafka Consumer Performance Test")
        environment.create_local_runner()
        environment.runner.start(1, spawn_rate=1)
    else:
        print("Invalid mode. Please use 'producer' or 'consumer'.")
        sys.exit(1)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Stopping Locust...")
        environment.runner.quit()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        environment.runner.quit()
    finally:
        logging.info("Kafka Performance Test has stopped.")
