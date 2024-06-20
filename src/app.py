from scripts.data_ingestion_setup import DataIngestSimulator

simulator = DataIngestSimulator(
        data='taxi_pickup_data.csv',
        topic='nyc_taxi_data',
        server='localhost:9092'
    )

simulator.run_simulated_ingest(interval=2)

