import pymysql
import logging
from src.transform.clean_data import clean_weather_data
from src.utils.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_TABLE

# 5 
def load_weather_data_to_db(df_cleaned):
    try:
        rows = [
            tuple(row.values())
            for row in df_cleaned.iter_rows(named=True)
        ]
        print("get data input successfully")

        # pymysql database connection
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            port=MYSQL_PORT,
            autocommit=False
        )

        cursor = conn.cursor()
        print("create pymysql connection successfully")

        # sql insert data
        ingestion_sql = f"""
        INSERT INTO {MYSQL_TABLE} (
            date_time, temperature_2m, rain, snowfall, snow_depth,
            weather_code, cloud_cover, cloud_cover_low,
            cloud_cover_mid, cloud_cover_high,
            wind_speed_10m, wind_speed_80m,
            wind_speed_120m, wind_speed_180m   
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(ingestion_sql, rows)
        conn.commit()
        print("create pymysql connection successfully")
    except Exception:
        logging.exception("Error occured in load_weather_data_to_db")
        raise
