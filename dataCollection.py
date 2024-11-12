import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient

apiKey = '***REMOVED***'
symbol = 'MNMD'
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={apiKey}'

response = requests.get(url)
data = response.json()

response = requests.get(url)
data = response.json()


# convert data to a CSV file
df = pd.DataFrame(data)
print(df.head())
df.to_csv('financial_data.csv', index=False)

#uploaf CSV to Blob Storage
connect_str = 'DefaultEndpointsProtocol=https;AccountName=bnyprojectstorage;AccountKey=***REMOVED***;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client("market-data")

with open("financial_data.csv", "rb") as data:
    container_client.upload_blob("financial_data.csv", data)


