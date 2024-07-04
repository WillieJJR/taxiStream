from scripts.data_ingestion_setup import BaseKafka
from kafka import KafkaConsumer
from src.database.database import CommonDatabase
from config import DATABASE_URL
from sqlalchemy import Integer, String, Column, DateTime, Float
import json

class RawDataLoader(BaseKafka):
    def __init__(self):
        super().__init__()
        self.consumer = KafkaConsumer(self.kafka_topic,
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      group_id='my-group'
                                      )
        self.database = CommonDatabase(database_url=DATABASE_URL)
        self.schema = [
            Column('trip_id', Integer, primary_key=True),
            Column('vendor_id', Integer, nullable=True),
            Column('pickup_datetime', String(50), nullable=False),
            Column('dropoff_datetime', DateTime, nullable=False),
            Column('passenger_count', DateTime, nullable=False),
            Column('trip_distance', Float, nullable=False),
            Column('rate_code_id', Integer, nullable=True),
            Column('pickup_loc_id', Integer, nullable=True),
            Column('dropoff_loc_id', Integer, nullable=True),
            Column('payment_type', Integer, nullable=True),
            Column('fare_amount', Float, nullable=True),
            Column('extra_amount', Float, nullable=True),
            Column('mta_tax', Float, nullable=True),
            Column('tip_amount', Float, nullable=True),
            Column('toll_amount', Float, nullable=True),
            Column('improvement_surcharge', Float, nullable=True),
            Column('congestion_surcharge', Float, nullable=True),
            Column('total_amount', Float, nullable=True)

        ]

    def create_raw_table(self, table_name):
        if self.database.check_table_exists():
            print(f'{table_name} already exists in schema')
        else:
            self.database.create_table(table_name, columns=self.schema)

    def load_messages_to_db(self):
        for message in self.consumer:
            message_value = message.value  # This is already bytes
            print(f"Received message: {message_value}")

    def run(self):
        ...

    def close_consumer(self):
        self.consumer.close()


if __name__ == "__main__":
    kafka_base = BaseKafka()
    loader = RawDataLoader()
    loader.load_messages_to_db()
    loader.close_consumer()

