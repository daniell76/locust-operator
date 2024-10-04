import locust
import requests
import random
from locust import HttpUser, task, between
from locust.contrib.locust_plugins import CustomMetrics
from locust.contrib.fail_fast import FailFast
from tenacity import retry, stop_after_attempt, wait_fixed
import json
import string  


class MessageMetrics(CustomMetrics):
    """
    Custom metrics class for latencies, sizes, throughput, and error rate.
    """

    def __init__(self, environment, *args, **kwargs):
        super().__init__(environment, *args, **kwargs)
        self.message_latencies = []
        self.message_sizes = []
        self.total_requests = 0
        self.request_failures = 0

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def log(
        self,
        request_type,
        name,
        response_time,
        content_length,
        response_status,
        response_headers,
        request_headers,
    ):
        super().log(
            request_type,
            name,
            response_time,
            content_length,
            response_status,
            response_headers,
            request_headers,
        )
        self.message_latencies.append(response_time)
        self.message_sizes.append(content_length)
        self.total_requests += 1
        if response_status >= 400:
            self.request_failures += 1

    def calculate_throughput(self):
        if not self.environment.elapsed_time:
            return 0
        return self.total_requests / self.environment.elapsed_time

    def calculate_error_rate(self):
        if not self.total_requests:
            return 0
        return (self.request_failures / self.total_requests) * 100

    def get_additional_metrics(self):
        return {
            "throughput": self.calculate_throughput(),
            "error_rate": self.calculate_error_rate(),
        }


class MyTaskSet(locust.TaskSet):

    producer_topics = ["random_data", "random_data1"]
    consumer_topics = ["random_data2", "random_data4"]

    def on_start(self):
        pass

    @task(weight=2)
    def produce_message(self):
        topic = random.choice(self.producer_topics)

        # Generate random JSON data with size between 1 KB and 100 KB
        data_size = random.randint(1024, 1024 * 100) 
        random_data = "".join(
            random.choice(string.ascii_letters + string.digits)
            for _ in range(data_size)
        )
        message = {"data": random_data}

        try:
            response = self.client.post(f"topics/{topic}", json=message)
            response.raise_for_status()
            self.environment.metrics.message_latencies.append(
                response.elapsed.total_seconds()
            )
            self.environment.metrics.message_sizes.append(len(json.dumps(message)))
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error producing message to {topic}: {e}")

    @task(weight=1)
    def consume_message(self):
        topic = random.choice(self.consumer_topics)

        try:
            response = self.client.get(f"topics/{topic}")
            response.raise_for_status()
            self.environment.metrics.message_latencies.append(
                response.elapsed.total_seconds()
            )
            self.environment.metrics.message_sizes.append(len(response.content))
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error consuming message from {topic}: {e}")


class MyUser(HttpUser):
    """
    User class with wait time and custom task set.
    """

    wait_time = between(1, 3)
    tasks = [MyTaskSet]
    catch_response = True


class MyFailFast(FailFast):
    """
    Custom FailFast class with a configurable failure rate.
    """

    def __init__(self, environment, *args, **kwargs):
        super().__init__(environment, *args, **kwargs)
        self.failure_rate = 0.1

    def should_fail(self):
        return random.random() < self.failure_rate


try:
    from locust_plugins.opentelemetry import JaegerListener
except ImportError:
    JaegerListener = None

environment = locust.Environment(user_count=100, rate=5)
environment.metrics_class = MessageMetrics
environment.create_local_taskset(MyUser)

if JaegerListener:
    environment.listener = JaegerListener()

environment.run(
    host="http://your_kafka_broker:9092", stop_timeout=300, fail_fast=MyFailFast()
)
