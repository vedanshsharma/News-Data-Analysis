import requests 
import datetime
from datetime import timedelta
import requests
from datetime import datetime
import uuid 
import json
import os 
from google.cloud import storage
import pandas as pd
from dotenv import load_dotenv
load_dotenv()


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
    from_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
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
    else:
        print(f"Error fetching data: {response.status_code}")
        return None 

    df = pd.DataFrame(columns=['newsTitle', 'timestamp', 'url_source', 'content', 'source', 'author', 'urlToImage'])

    for article in data['articles']:
        news_title = article['title']
        timestamp = article['publishedAt']
        url_source = article['url']
        source = article['source']['name']
        author = article['author']
        urlToImage = article['urlToImage']
    
        partial_content = article['content'] if article['content'] is not None else ""
        
        if len(partial_content) >= 200:
            trimmed_part = partial_content[:199]
        if '.' in partial_content:
            trimmed_part = partial_content[:partial_content.rindex('.')]
        else:
            trimmed_part = partial_content
        
        new_row = pd.DataFrame({
            'newsTitle': [news_title],
            'timestamp': [timestamp],
            'url_source': [url_source],
            'content': [trimmed_part],
            'source': [source],
            'author': [author],
            'urlToImage': [urlToImage]
        })

        df = pd.concat([df, new_row], ignore_index=True)
    
    print(df.head())
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f'run_{current_time}.parquet'

    #check current working directory
    print("Current working directory : " , os.getcwd())

    #write to parquet file 
    df.to_parquet(filename)

    #uplaod to GCS bucket
    bucket_name = os.getenv('BUCKET_NAME')
    destination_blob_name = f'news_data_analysis/{filename}'
    upload_to_gcs(bucket_name, filename, destination_blob_name)

    #remove file from local storage 
    os.remove(filename)

# fetch_news_data()