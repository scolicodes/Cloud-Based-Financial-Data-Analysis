import schedule
import time
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve API key and Azure connection string from environment variables
apiKey = os.getenv("API_KEY")
connectStr = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

symbols = ['RIVN', 'COIN', 'WM', 'AAPL', 'BK', 'TSLA', 'MNMD']
base_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY'

# Function to fetch and upload data for a single symbol
def fetch_and_upload_data(symbol):
    try:
        url = f"{base_url}&symbol={symbol}&apikey={apiKey}"
        response = requests.get(url)
        response.raise_for_status()  # Raise an error if the request failed
        data = response.json()

        # Convert data to a DataFrame
        if "Time Series (Daily)" not in data:
            print(f"Error fetching data for {symbol}: {data.get('Note', 'Unknown error')}")
            return
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
        print(f"Data for {symbol} uploaded to Azure Blob Storage.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
    except Exception as e:
        print(f"Error processing data for {symbol}: {e}")

# Schedule the task to run for all symbols every day
def fetch_and_upload_all():
    for symbol in symbols:
        fetch_and_upload_data(symbol)

# Schedule the task daily at a specific time (e.g., 6:00 AM)
schedule.every().day.at("06:00").do(fetch_and_upload_all)

# Run the scheduler
print("Scheduler started. Press Ctrl+C to exit.")
while True:
    schedule.run_pending()
    time.sleep(1)
