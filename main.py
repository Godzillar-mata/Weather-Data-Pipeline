import logging
from src.utils.db_connection import connect_to_weather_data_database, create_daily_weather_table
from src.extract.fetch_api import fetch_weather_data_from_api
from src.transform.clean_data import clean_weather_data
from src.load.load_to_database import load_weather_data_to_db



def main():
    try:
        # Connect to database
        connect_to_weather_data_database()

        # Create table
        create_daily_weather_table()

        # Extract
        data = fetch_weather_data_from_api()

        # Transform
        cleaned_data = clean_weather_data(data)

        # Load
        load_weather_data_to_db(cleaned_data)
    except Exception:
        logging.error("Pipeline failed")

if __name__ == "__main__":
    main()