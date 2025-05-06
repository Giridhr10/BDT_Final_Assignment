import time
import logging
import toml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO)

def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logging.info(f"{func.__name__} executed in {end - start:.2f} seconds")
        return result
    return wrapper

@timing_decorator
def connect_to_cassandra():
    config = toml.load("noshare.toml")["astra"]
    cloud_config = {"secure_connect_bundle": config["secure_connect_path"]}
    auth_provider = PlainTextAuthProvider(config["client_id"], config["client_secret"])
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    logging.info("Connected to Cassandra successfully")
    return session

if __name__ == "__main__":
    connect_to_cassandra()
