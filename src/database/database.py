import sqlalchemy

class CommonDatabase:

    def __init__(self, database_url):

        self.database_url = database_url
        self.engine = sqlalchemy.create_engine(database_url)
        self.metadata = sqlalchemy.MetaData()
        self.connection = None
        self.connect()


    def connect(self)-> None:

        try:
            self.connection = self.engine.connect()
            print("Success! Connected to the Database")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def disconnect(self)-> None:
        if self.connection:
            self.connection.close()

    def execute_query(self, query):
        if not self.connection:
            print("Error: Database connection not established.")
            return None

        try:
            result = self.connection.execute(sqlalchemy.text(query))
            return result
        except Exception as e:
            print(f"Error executing query: {e}")

    def create_table(self, table_name, columns) -> None:
        if not self.connection:
            print("Error: Database connection not established.")
        try:
            table = sqlalchemy.Table(table_name, self.metadata, *columns)
            table.create(bind=self.engine)
            print(f"Table '{table_name}' created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")

    def check_table_exists(self, table) -> bool:
        if not self.connection:
            print("Error: Database connection not established.")

        query = f"SELECT * FROM {table} LIMIT 1"
        results = self.execute_query(query)
        if results:
            table_exists = True
        else:
            table_exists = False

        return table_exists

    def insert_values_to_table(self, values_dict, table_name, columns) -> None:
        if not self.connection:
            print("Error: Database connection not established.")
        try:
            table_struct = sqlalchemy.Table(
                table_name,
                self.metadata,
                *columns
            )
            self.connection.execute(table_struct.insert().values(**values_dict))
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"Error caused by checkthisout: {e}")


    def purge_table(self, table_name):
        if not self.connection:
            print("Error: Database connection not established.")
            return

        try:
            # Execute the TRUNCATE statement to delete all records from the table
            table_struct = sqlalchemy.Table(
                table_name,
                self.metadata,
                autoload_with=self.engine
            )
            self.connection.execute(sqlalchemy.delete(table_struct))
            self.connection.commit()
            print(f"All records deleted from table '{table_name}'.")
        except Exception as e:
            self.connection.rollback()
            print(f"Error purging table: {e}")

if __name__ == "__main__":
    db = CommonDatabase()