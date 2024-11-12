import asyncpg
from configparser import ConfigParser

# Load database configuration from config.ini once
config = ConfigParser()
config.read('config.ini')

DB_CONFIG = {
    'user': config["DB"]['user'],
    'password': config["DB"]['password'],
    'database': config["DB"]['dbname'],
    'host': config["DB"]['host'],
    'port': config["DB"]['port']
}

async def get_db_connection():
    """Create and return an async connection to the PostgreSQL database."""
    conn = await asyncpg.connect(**DB_CONFIG)
    return conn
