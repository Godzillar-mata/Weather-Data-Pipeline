import requests
import logging
from src.utils.config import API_URL

# 1 fetch weather data from api.
def fetch_weather_data_from_api():
    try:
        api_url = API_URL
        response = requests.get(api_url)
        return response.json()
    except Exception:
        logging.exception("Error occurred in fetch_weather_data_from_api")
        raise
