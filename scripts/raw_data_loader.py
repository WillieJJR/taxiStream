from scripts.data_ingestion_setup import BaseKafka
from kafka import KafkaConsumer
import json

class RawDataLoader(BaseKafka):
    def __init__(self):
        super().__init__()
        self.consumer = KafkaConsumer(self.kafka_topic,
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      group_id='my-group'
                                      )

    def load_messages_to_db(self):
        for message in self.consumer:
            message_value = message.value  # This is already bytes
            print(f"Received message: {message_value}")

    def close_consumer(self):
        self.consumer.close()


if __name__ == "__main__":
    kafka_base = BaseKafka()
    loader = RawDataLoader()
    loader.load_messages_to_db()
    loader.close_consumer()

