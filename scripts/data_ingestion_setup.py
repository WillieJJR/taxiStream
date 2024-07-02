from src.database.database import CommonDatabase
from sqlalchemy import Column, Integer, String
from config import *
from kafka import KafkaProducer
import pandas
import csv
import time
import os
import json

class BaseKafka:
    def __init__(self):
        self.data = 'taxi_pickup_data.csv'
        self.kafka_topic = 'nyc_taxi_data'
        self.kafka_server = 'localhost:9092'
class DataIngestSimulator(BaseKafka):
    '''Push all data from the csv file into the kafka queue to simulate real-time data production'''
    def __init__(self):
        super().__init__()
        #self.db = CommonDatabase(database_url=DATABASE_URL)
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_server)
        print(f"Data: {self.data}, Kafka Topic: {self.kafka_topic}, Kafka Server: {self.kafka_server}")


    def read_csv(self):
        script_dir = os.path.dirname(__file__)
        data_dir = os.path.join(script_dir, '..', 'data')
        print(self.data)
        file_path = os.path.join(data_dir, self.data)
        with open(file_path, 'r') as file:
            importer = csv.DictReader(file)
            data = [row for row in importer]
        return data

    def simulate_real_time_ingest(self, data, interval=1):
        for record in data:
            json_record = json.dumps(record)  # Convert record to JSON string
            self.producer.send(self.kafka_topic, value=str(record).encode('utf-8'))
            print(f"record waiting in queue: {record}")
            time.sleep(interval)

    def run_simulated_ingest(self, num_records, interval=1):
        record=self.read_csv()[:num_records]
        self.simulate_real_time_ingest(record, interval)

if __name__ == "__main__":
    kafka_base = BaseKafka()
    simulator = DataIngestSimulator()
    simulator.run_simulated_ingest(interval=2)


