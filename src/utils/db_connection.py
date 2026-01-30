from sqlalchemy import create_engine, text
from src.utils.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
import logging

# 3 Connect to database
def connect_to_weather_data_database():
    try:
        # create engine
        engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}",
        echo=True
        )

        # create connection
        with engine.begin() as conn:
            query_database = text("""
                SELECT SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME = :db
            """)
            database = conn.execute(query_database, {"db": MYSQL_DATABASE})

            # if database is none then create database
            if database.first() is None:
                conn.execute(text(f"CREATE DATABASE {MYSQL_DATABASE}"))
                print(f"Database '{MYSQL_DATABASE}' created")
            # if database is not none then show messege
            else:
                print(f"Database '{MYSQL_DATABASE}' already exists")
    except Exception:
        logging.exception("Error occured in connect_to_weather_data_database")
        raise

# 4 Create table
def create_daily_weather_table():
    try:
        db_engine = create_engine(
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
            echo=True
        )
        print("create database engine successfully")

        create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS weather_data.daily_weather (
            id INT PRIMARY KEY AUTO_INCREMENT,
            date_time DATETIME,
            temperature_2m DECIMAL(6,3),
            rain DECIMAL(6,3),
            snowfall DECIMAL(6,3),
            snow_depth DECIMAL(6,3),
            weather_code INT,
            cloud_cover INT,
            cloud_cover_low INT,
            cloud_cover_mid INT,
            cloud_cover_high INT,
            wind_speed_10m DECIMAL(6,3),
            wind_speed_80m DECIMAL(6,3),
            wind_speed_120m DECIMAL(6,3),
            wind_speed_180m DECIMAL(6,3)
        );
        """)

        with db_engine.begin() as conn:
            conn.execute(create_table_sql)

            logging.info("Table 'daily_weather' created (if not exists)")
    except Exception:
        logging.exception("Error occured in create_daily_weather_table")
        raise
