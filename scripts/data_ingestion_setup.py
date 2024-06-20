from src.database.database import CommonDatabase
from sqlalchemy import Column, Integer, String
from config import *
from kafka import KafkaProducer
import pandas
import csv
import time
import os

class DataIngestSimulator:
    '''Push all data from the csv file into the kafka queue to simulate real-time data production'''
    def __init__(self, data, topic, server):
        #self.db = CommonDatabase(database_url=DATABASE_URL)
        self.data = data
        self.kafka_topic = topic
        self.kafka_server = server
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_server)

    def read_csv(self):
        script_dir = os.path.dirname(__file__)
        data_dir = os.path.join(script_dir, '..', 'data')
        file_path = os.path.join(data_dir, self.data)
        with open(file_path, 'r') as file:
            importer = csv.DictReader(file)
            data = [row for row in importer]
        return data

    def simulate_real_time_ingest(self, data, interval=1):
        for record in data:
            self.producer.send(self.kafka_topic, value=str(record).encode('utf-8'))
            print(f"record waiting in queue: {record}")
            time.sleep(interval)

    def run_simulated_ingest(self, interval=1):
        record=self.read_csv()
        self.simulate_real_time_ingest(record, interval)

if __name__ == "__main__":
    simulator = DataIngestSimulator(
        data='data/nyc_taxi_data.csv',
        topic='nyc_taxi_data',
        server=['localhost:9092']
    )
    simulator.run_simulated_ingest(interval=2)


