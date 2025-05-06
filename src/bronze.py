import pandas as pd
from cassandra.query import BatchStatement
from utils import connect_to_cassandra

def create_bronze_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS tripdata.bronze_trip_data (
            ride_id TEXT PRIMARY KEY,
            rideable_type TEXT,
            started_at TEXT,
            ended_at TEXT,
            start_station_name TEXT,
            start_station_id TEXT,
            end_station_name TEXT,
            end_station_id TEXT,
            start_lat DOUBLE,
            start_lng DOUBLE,
            end_lat DOUBLE,
            end_lng DOUBLE,
            member_casual TEXT
        );
    """)
    session.execute("TRUNCATE tripdata.bronze_trip_data")
    print("Bronze table created and truncated.")

def insert_bronze_data(session, df, batch_size=500):
    prepared = session.prepare("""
        INSERT INTO tripdata.bronze_trip_data (
            ride_id, rideable_type, started_at, ended_at,
            start_station_name, start_station_id,
            end_station_name, end_station_id,
            start_lat, start_lng, end_lat, end_lng,
            member_casual
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for i in range(0, len(df), batch_size):
        batch = BatchStatement()
        for _, row in df.iloc[i:i+batch_size].iterrows():
            batch.add(prepared, (
                row["ride_id"], row["rideable_type"], row["started_at"], row["ended_at"],
                row["start_station_name"], row["start_station_id"],
                row["end_station_name"], row["end_station_id"],
                row["start_lat"], row["start_lng"], row["end_lat"], row["end_lng"],
                row["member_casual"]
            ))
        session.execute(batch)
        print(f"Inserted rows {i} to {min(i + batch_size, len(df))}")

def analyze_data(df):
    print("\n\n\nDATASET ANALYSIS\n\n")
    print(f"Total rows: {len(df)}")
    print("\n\nColumn Types:\n\n", df.dtypes)
    print("\n\nMissing Values per Column:\n\n", df.isnull().sum())
    print("\n\nNumber of unique values (for categorical columns):\n")
    for col in ['ride_id','rideable_type', 'member_casual', 'start_station_name', 'end_station_name']:
        print(f"{col}: {df[col].nunique()} unique values")
    print("\n\nUnique Values:\n")
    for col in ['rideable_type', 'member_casual']:
        print(f"{col}: {df[col].dropna().unique()}")

    print("\n\nSample rows:")
    print(df.sample(3).to_string(index=False))

if __name__ == "__main__":
    session = connect_to_cassandra()
    create_bronze_table(session)

    df = pd.read_csv("data/202202-divvy-tripdata.csv")
    df = df.where(pd.notnull(df), None)

    insert_bronze_data(session, df)
    analyze_data(df)