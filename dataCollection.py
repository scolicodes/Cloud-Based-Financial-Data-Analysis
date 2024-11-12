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

# Define a list of stock symbols to track
symbols = ['RIVN', 'COIN', 'WM', 'AAPL', 'BK', 'TSLA', 'MNMD'] # Rivian, Coinbase, Waste Management, Apple, BNY, MindMed

# Initialize Azure Blob Storage client
blob_service_client = BlobServiceClient.from_connection_string(connectStr)
container_client = blob_service_client.get_container_client("market-data")

for symbol in symbols:
    # Define API URL for the current symbol
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={apiKey}'

    # Fetch data from Alpha Vantage API
    response = requests.get(url)
    data = response.json()

    # Check if the response contains data
    if "Time Series (Daily)" in data:
        # Convert data to DataFrame
        dailyData = data["Time Series (Daily)"]
        df = pd.DataFrame.from_dict(dailyData, orient="index")
        df.columns = ["open", "high", "low", "close", "volume"]
        df.index.name = "date"

        # Save data to CSV
        filename = f'{symbol}_financial_data.csv'
        df.to_csv(filename)

         # Upload CSV to Azure Blob Storage
        with open(filename, "rb") as data_file:
            container_client.upload_blob(name=filename, data=data_file, overwrite=True)
        
        print(f'Data for {symbol} saved and uploaded successfully.')
    else:
        print(f"No data found for {symbol}. Please check the API limits or symbol name.")





