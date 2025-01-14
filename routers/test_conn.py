from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) 

db_params = {
    'dbname': 'etl_db',
    'user': 'etluser',
    'password': 'etlpassword',
    'host': 'localhost',
    'port': '5432'
}

def test_postgresql_connection():
    try:
        engine = create_engine(
            f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}'
        )

        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.fetchone()
            logger.info(f"Connected to PostgreSQL: {version[0]}")

            result = connection.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            )
            tables = [row[0] for row in result.fetchall()]
            logger.info(f"Available tables: {tables}")

    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

if __name__ == "__main__":
    test_postgresql_connection()
