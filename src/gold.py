import pandas as pd
import matplotlib.pyplot as plt
import logging
import os
from cassandra.query import dict_factory, BatchStatement
from utils import connect_to_cassandra, timing_decorator

logging.basicConfig(level=logging.INFO)

KEYSPACE = "tripdata"
SILVER_TABLE = "silver_trip_data"
GOLD_DAILY_TABLE = "gold_daily_trips"
GOLD_USER_TABLE = "gold_user_type"
GOLD_STATION_TABLE = "gold_top_stations"
OUTPUT_DIR = "output"

@timing_decorator
def create_gold_tables(session):
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{GOLD_DAILY_TABLE} (
            trip_date DATE PRIMARY KEY,
            total_trips INT
        );
    """)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{GOLD_USER_TABLE} (
            member_type TEXT PRIMARY KEY,
            total_users INT
        );
    """)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{GOLD_STATION_TABLE} (
            station_name TEXT PRIMARY KEY,
            total_arrivals INT
        );
    """)
    logging.info("Gold tables created.")

@timing_decorator
def truncate_gold_tables(session):
    session.execute(f"TRUNCATE {KEYSPACE}.{GOLD_DAILY_TABLE}")
    session.execute(f"TRUNCATE {KEYSPACE}.{GOLD_USER_TABLE}")
    session.execute(f"TRUNCATE {KEYSPACE}.{GOLD_STATION_TABLE}")
    logging.info("Gold tables truncated.")

def get_dataframe_from_cassandra(session, limit=100000):
    session.row_factory = dict_factory
    rows = session.execute(f"SELECT * FROM {KEYSPACE}.{SILVER_TABLE} LIMIT {limit}")
    return pd.DataFrame(rows)

def insert_daily_trips(session, df):
    stmt = session.prepare(f"INSERT INTO {KEYSPACE}.{GOLD_DAILY_TABLE} (trip_date, total_trips) VALUES (?, ?)")
    batch = BatchStatement()
    for _, row in df.iterrows():
        batch.add(stmt, (row["trip_date"], int(row["value"])))
    session.execute(batch)

def insert_user_type(session, df):
    stmt = session.prepare(f"INSERT INTO {KEYSPACE}.{GOLD_USER_TABLE} (member_type, total_users) VALUES (?, ?)")
    batch = BatchStatement()
    for _, row in df.iterrows():
        batch.add(stmt, (row["label"], int(row["value"])))
    session.execute(batch)

def insert_top_stations(session, df):
    stmt = session.prepare(f"INSERT INTO {KEYSPACE}.{GOLD_STATION_TABLE} (station_name, total_arrivals) VALUES (?, ?)")
    batch = BatchStatement()
    for _, row in df.iterrows():
        batch.add(stmt, (row["label"], int(row["value"])))
    session.execute(batch)

def save_plot(fig, filename):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    full_path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(full_path, bbox_inches='tight')
    logging.info(f"Saved plot: {full_path}")

def analyze_and_plot(df):
    df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
    df = df.dropna(subset=["started_at", "ride_id"])

    #DAILY TRIPS
    daily = df.groupby(df["started_at"].dt.date).size().reset_index(name="value")
    daily["trip_date"] = pd.to_datetime(daily["started_at"])
    fig1 = plt.figure(figsize=(10, 5))
    plt.plot(daily["trip_date"], daily["value"], marker="o")
    plt.title("Daily Trip Count")
    plt.xlabel("Trip Dates")
    plt.ylabel("Trips count")
    plt.xticks(rotation=45)
    plt.grid(True)
    save_plot(fig1, "daily_trip_count.png")

    #USER TYPE
    member_types = df["member_casual"].value_counts().reset_index()
    member_types.columns = ["label", "value"]
    fig2 = plt.figure(figsize=(6, 6))
    plt.pie(member_types["value"], labels=member_types["label"], autopct="%1.1f%%", startangle=90)
    plt.title("User Type Distribution")
    save_plot(fig2, "user_type_distribution.png")

    #TOP STATIONS
    station_df = df[df["end_station_name"] != "Unknown End"]
    top_stations = station_df["end_station_name"].value_counts().head(5).reset_index()
    top_stations.columns = ["label", "value"]
    fig3 = plt.figure(figsize=(8, 5))
    plt.bar(top_stations["label"], top_stations["value"], color="skyblue")
    plt.title("Top 5 End Stations")
    plt.xlabel("End Station")
    plt.ylabel("Trips count")
    plt.xticks(rotation=45)
    save_plot(fig3, "top_end_stations.png")

    return daily[["trip_date", "value"]], member_types, top_stations

if __name__ == "__main__":
    session = connect_to_cassandra()
    create_gold_tables(session)
    truncate_gold_tables(session)

    df = get_dataframe_from_cassandra(session)

    #Analyze and generate visualizations
    daily_df, member_df, station_df = analyze_and_plot(df)

    #Insert to gold tables
    insert_daily_trips(session, daily_df)
    insert_user_type(session, member_df)
    insert_top_stations(session, station_df)