import pandas
import requests 
import datetime
from datetime import datetime
import uuid 
import json
import os 
from google.cloud import storage 


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # Initialize a storage client
    storage_client = storage.Client()

    # Get the bucket that the file will be uploaded to
    bucket = storage_client.bucket(bucket_name)

    # Create a new blob and upload the file's content
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def fetch_news_data():
    # Define the API endpoint and parameters
    url = "https://newsapi.org/v2/everything"
    api_key = os.getenv('NEWS_API_KEY')
    query = "technology"
    from_date = (datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    to_date = datetime.now().strftime('%Y-%m-%d')

    params = {
        'q': query,
        'from': from_date,
        'to': to_date,
        'sortBy': 'relevancy',
        'apiKey': api_key
    }

    # Make the API request
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        return data['articles']
    else:
        print(f"Error fetching data: {response.status_code}")
        return []