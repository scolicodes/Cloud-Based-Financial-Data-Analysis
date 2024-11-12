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

# Define stock symbol and API URL
symbol = 'MNMD'
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={apiKey}'

# Fetch data from Alpha Vantage API
response = requests.get(url)
data = response.json()

# Convert data to a DataFrame and save as CSV
df = pd.DataFrame(data)
print(df.head())
df.to_csv('financial_data.csv', index=False)

# Upload CSV to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(connectStr)
container_client = blob_service_client.get_container_client("market-data")

with open("financial_data.csv", "rb") as data:
    container_client.upload_blob("financial_data.csv", data)
