from src.database.database import CommonDatabase
from sqlalchemy import Column, Integer, String
from config import *
from kafka import KafkaProducer
import pandas
import csv

class DataIngestSimulator:
    '''Push all data from the csv file into the kafka queue to simulate real-time data production'''
    def __init__(self, data, topic, server):
        self.db = CommonDatabase(database_url=DATABASE_URL)
        self.data = data
        self.kafka_topic = topic
        self.kafka_server = server
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_servers)

    def read_csv(self):
        with open(self.data, 'r') as file:
            importer = csv.DictReader(file)
            data = [row for row in importer]
        return data



