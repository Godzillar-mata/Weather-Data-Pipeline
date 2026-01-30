import polars as pl
import logging
from datetime import datetime
from src.extract.fetch_api import fetch_weather_data_from_api

# 2 clean data 
def clean_weather_data(data):
    try:
        if data is None:
            raise ValueError("fetch_weather_data_from_api return no data")
        
        # select only hourly data
        hourly_data = data["hourly"]

        # make time into ISO 8601-like format.
        df_cleaned = (
            pl.DataFrame(hourly_data)
            .with_columns(
                pl.col("time").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M")
            )
        )
        return df_cleaned
    except Exception:
        logging.exception("Error occurred in clean_weather_data")
        raise