import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(filename='stock_data_logs.log', level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s") #  Configure logging to display time, log level, and message
logger = logging.getLogger(__name__)    # Create a logger for this module

BASEURL = "alpha-vantage.p.rapidapi.com"  # API endpoint for fetching time series data

url = f"https://{BASEURL}/query"  # Construct the full URL for the API request

api_key = os.getenv("API_KEY")  # Retrieve the API key from environment variables

headers = {
	"x-rapidapi-key": api_key,
	"x-rapidapi-host": BASEURL
}


