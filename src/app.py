from scripts.data_ingestion_setup import DataIngestSimulator, BaseKafka
from scripts.raw_data_loader import RawDataLoader


kakfa_instance = BaseKafka()

# Set the total number of records to simulate
total_records_to_simulate = 4
simulator = DataIngestSimulator()
simulator.run_simulated_ingest(interval=2, num_records=total_records_to_simulate)

loader = RawDataLoader()
loader.load_messages_to_db()






