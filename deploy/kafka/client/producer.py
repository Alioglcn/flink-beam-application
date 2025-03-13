import os
import time
import random
from kafka import KafkaProducer

class TemperatureProducer:
    def __init__(self, bootstrap_servers: list, topic_name: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.kafka_producer = self.create_producer()

    def create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: str(v).encode("utf-8"), 
        )

    def send_to_kafka(self, value: float, timestamp_ms: int = None):
        try:
            args = {"topic": self.topic_name, "value": value}
            if timestamp_ms is not None:
                args = {**args, **{"timestamp_ms": timestamp_ms}}
            self.kafka_producer.send(**args)
            self.kafka_producer.flush()
        except Exception as e:
            raise RuntimeError("Failed to send a message") from e


if __name__ == "__main__":
    producer = TemperatureProducer(
        os.getenv("BOOTSTRAP_SERVERS", "demo-cluster-kafka-bootstrap.default.svc.cluster.local:9092"),
        os.getenv("TOPIC_NAME", "input-topic"),
    )

    num_events = 0
    while True:
        num_events += 1
        temperature = round(random.uniform(-10, 50), 2)  
        producer.send_to_kafka(temperature)

        if num_events % 5 == 0:
            print(f"<<<<<{num_events} temperature values sent... current>>>>\n{temperature}")

        time.sleep(int(os.getenv("DELAY_SECONDS", "90")))  
