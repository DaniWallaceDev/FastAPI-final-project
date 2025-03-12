import psycopg, logging


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("db")

def db_connection() -> psycopg.connection:
    conn = psycopg.connect(
        "dbname=postgres host=localhost password=1234 user=postgres")

    if conn:
        logger.info("Successful connection")
        return conn
    else:
        logger.error("Connection error")
        return None