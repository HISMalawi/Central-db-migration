import pymysql
from sqlalchemy import create_engine
import yaml

def load_config(file_path="config.yml"):
    """Load database configurations from a YAML file."""
    with open(file_path, "r") as file: 
        return yaml.safe_load(file)

def create_engine_connection(db_url):
    """Create an SQLAlchemy engine connection."""
    return create_engine(db_url)

def create_source_connection(db_config):
    """Create a connection to the source database."""
    return pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"]
    )
