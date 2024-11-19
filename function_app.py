import logging
import azure.functions as func
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 11 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    fetch_and_upload_all()
  
    logging.info('Python timer trigger function executed.')

# Load environment variables from .env file
load_dotenv()

# Retrieve API key and Azure connection string from environment variables
apiKey = os.getenv("API_KEY")
connectStr = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

symbols = ['RIVN', 'COIN', 'WM', 'AAPL', 'BK', 'TSLA', 'MNMD']
base_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY'


def fetch_and_upload_data(symbol):
    """Function to fetch and upload data for a single symbol"""
    try:
        # Fetch data from API
        url = f"{base_url}&symbol={symbol}&apikey={apiKey}"
        response = requests.get(url)
        logging.info(f"Response for {symbol}: {response.status_code}")
        response.raise_for_status()  # Raise an error if the request failed

        # Parse JSON response
        data = response.json()
        
        if "Time Series (Daily)" not in data:
            logging.error(f"Unexpected API response for {symbol}: {data}")
            return

        # Convert JSON to DataFrame    
        time_series = data["Time Series (Daily)"]
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df = df.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume",
        })
        df.index.name = "date"

        # Save to CSV
        file_name = f"{symbol}_data.csv"
        df.to_csv(file_name)
        print(f"Data for {symbol} saved to {file_name}")

        # Upload to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(connectStr)
        container_client = blob_service_client.get_container_client("market-data")
        with open(file_name, "rb") as data_file:
            container_client.upload_blob(name=file_name, data=data_file, overwrite=True)
        logging.info(f"Data for {symbol} uploaded to Azure Blob Storage.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error fetching data for {symbol}: {e}")
    except ValueError as e:
        logging.error(f"Error parsing JSON response for {symbol}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error for {symbol}: {e}")


def fetch_and_upload_all():
    for symbol in symbols:
        fetch_and_upload_data(symbol)
