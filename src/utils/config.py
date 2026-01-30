import os
from dotenv import load_dotenv

load_dotenv()

# API keys
API_URL = os.getenv("API_URL")

# MySQL config
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_USER = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = "weather_data"
MYSQL_TABLE = "daily_weather"