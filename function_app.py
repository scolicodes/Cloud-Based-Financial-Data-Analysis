import logging
import azure.functions as func
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 11 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Starting fetch and upload process for all symbols...')
    fetchAndUploadAll()
    logging.info('Python timer trigger function executed successfully.')

# Load environment variables from .env file
load_dotenv()

# Retrieve API key and Azure connection string from environment variables
apiKey = os.getenv("API_KEY")
connectStr = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

symbols = ['RIVN', 'COIN', 'WM', 'AAPL', 'BK', 'TSLA', 'MNMD']
baseUrl = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY'


def fetchAndUploadData(symbol):
    """Fetch and upload data for a single symbol directly to Azure Blob Storage."""
    try:
        logging.info(f"Fetching data for {symbol}...")
        
        # Fetch data from API
        url = f"{baseUrl}&symbol={symbol}&apikey={apiKey}"
        response = requests.get(url)
        logging.info(f"Response for {symbol}: {response.status_code}")
        response.raise_for_status()  # Raise an error if the request failed

        # Parse JSON response
        data = response.json()
        if "Time Series (Daily)" not in data:
            logging.error(f"Unexpected API response for {symbol}: {data}")
            return

        # Convert JSON to DataFrame
        timeSeries = data["Time Series (Daily)"]
        df = pd.DataFrame.from_dict(timeSeries, orient='index')
        df = df.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume",
        })
        df.index.name = "date"

        # Convert DataFrame to CSV in memory
        csvData = df.to_csv(index=True)

        # Upload to Azure Blob Storage
        blobServiceClient = BlobServiceClient.from_connection_string(connectStr)
        containerClient = blobServiceClient.get_container_client("market-data")
        blobName = f"{symbol}_data.csv"

        logging.info(f"Uploading {symbol} data to Azure Blob Storage as {blobName}...")
        containerClient.upload_blob(name=blobName, data=csvData, overwrite=True)
        logging.info(f"Data for {symbol} successfully uploaded to Azure Blob Storage.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error fetching data for {symbol}: {e}")
    except ValueError as e:
        logging.error(f"Error parsing JSON response for {symbol}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error for {symbol}: {e}")


def fetchAndUploadAll():
    for symbol in symbols:
        fetchAndUploadData(symbol)
