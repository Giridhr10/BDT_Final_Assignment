from utils import connect_to_cassandra

KEYSPACE = "tripdata"

def drop_all_tables(session):
    rows = session.execute(
        f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{KEYSPACE}'"
    )
    table_names = [row.table_name for row in rows]

    for table in table_names:
        try:
            session.execute(f"DROP TABLE IF EXISTS {KEYSPACE}.{table}")
            print(f"Dropped table: {table}")
        except Exception as e:
            print(f"Error dropping table {table}: {e}")

if __name__ == "__main__":
    session = connect_to_cassandra()
    drop_all_tables(session)
