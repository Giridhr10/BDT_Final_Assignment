import pandas as pd
import logging
from cassandra.query import BatchStatement
from utils import connect_to_cassandra, timing_decorator

logging.basicConfig(level=logging.INFO)
KEYSPACE = "tripdata"
SILVER_TABLE = "silver_trip_data"

@timing_decorator
def create_silver_table(session):
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{SILVER_TABLE} (
            ride_id TEXT PRIMARY KEY,
            rideable_type TEXT,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            trip_duration INT,
            start_station_name TEXT,
            end_station_name TEXT,
            start_lat DOUBLE,
            start_lng DOUBLE,
            end_lat DOUBLE,
            end_lng DOUBLE,
            member_casual TEXT
        );
    """)
    session.execute(f"TRUNCATE {KEYSPACE}.{SILVER_TABLE}")
    logging.info("Silver table created and truncated.")

def safe_str(val):
    if pd.isna(val):
        return None
    return str(val)

def clean_data(path):
    df = pd.read_csv(path)
    logging.info(f"Number of rows in raw data: {len(df)}")

    # Drop rows with missing geo-coordinates
    df.dropna(subset=["start_lat", "start_lng", "end_lat", "end_lng"], inplace=True)

    # Convert datetime
    df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
    df["ended_at"] = pd.to_datetime(df["ended_at"], errors="coerce")

    # Create coordinate keys
    df["start_coord"] = list(zip(df["start_lat"], df["start_lng"]))
    df["end_coord"] = list(zip(df["end_lat"], df["end_lng"]))
    
    # Fill station name
    df["start_station_name"] = df.groupby("start_coord")["start_station_name"].transform(lambda x: x.ffill().bfill())
    df["end_station_name"] = df.groupby("end_coord")["end_station_name"].transform(lambda x: x.ffill().bfill())

    # Drop location ID columns if they exist
    df.drop(columns=["start_station_id", "end_station_id"], errors="ignore", inplace=True)

    # Drop coordinate helper columns
    df.drop(columns=["start_coord", "end_coord"], inplace=True)

    # Drop any remaining nulls
    df.dropna(inplace=True)

    # Calculate trip duration
    df["trip_duration"] = (df["ended_at"] - df["started_at"]).dt.total_seconds().astype(int)

    # Normalize text fields
    df["rideable_type"] = df["rideable_type"].str.lower()
    df["member_casual"] = df["member_casual"].str.lower()

    # Drop duplicate ride_ids
    df = df.drop_duplicates(subset=["ride_id"])
    logging.info(f"Rows after cleaning: {len(df)}")

    return df

@timing_decorator
def insert_cleaned_data(session, df):
    stmt = session.prepare(f"""
        INSERT INTO {KEYSPACE}.{SILVER_TABLE} (
            ride_id, rideable_type, started_at, ended_at, trip_duration,
            start_station_name, end_station_name,
            start_lat, start_lng, end_lat, end_lng, member_casual
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    batch = BatchStatement()
    batch_size = 500

    for i, row in df.iterrows():
        batch.add(stmt, (
            safe_str(row.ride_id), safe_str(row.rideable_type),
            row.started_at, row.ended_at, row.trip_duration,
            safe_str(row.start_station_name), safe_str(row.end_station_name),
            row.start_lat, row.start_lng, row.end_lat, row.end_lng,
            safe_str(row.member_casual)
        ))

        if (i + 1) % batch_size == 0:
            session.execute(batch)
            logging.info(f"Inserted batch ending at row {i + 1}")
            batch = BatchStatement()

    if batch:
        session.execute(batch)
        logging.info("Inserted final batch.")

def analyze_data(df):
    print("\nSILVER DATASET ANALYSIS")
    print(f"Total rows: {len(df)} | Columns: {len(df.columns)}\n")

    print("Column Types:")
    print(df.dtypes)

    print("\nMissing Values:")
    print(df.isnull().sum())

    print("\nNumber of unique values:")
    for col in ["rideable_type", "member_casual", "start_station_name", "end_station_name"]:
        print(f"{col}: {df[col].nunique()} unique")

    print("\nUnique Values:")
    for col in ["rideable_type", "member_casual"]:
        print(f"{col}: {df[col].dropna().unique()}\n")

    print("\nTrip Duration Summary (in seconds):")
    print(df["trip_duration"].describe())

    print("\nSample rows:")
    print(df.sample(3).to_string(index=False))

if __name__ == "__main__":
    session = connect_to_cassandra()
    create_silver_table(session)

    cleaned_df = clean_data("data/202202-divvy-tripdata.csv")
    insert_cleaned_data(session, cleaned_df)
    analyze_data(cleaned_df)