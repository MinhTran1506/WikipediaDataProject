import json
from geopy import Nominatim
import pandas as pd
import time
from geopy.exc import GeocoderUnavailable
import requests

NO_IMAGE = 'https://upload.wikipedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'
def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # check if the request is successful or not

        return response.text

    except requests.RequestException as e:
        print(f"An error occured: {e}")


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Extract the table
    table = soup.find_all("table", {"class": "wikitable sortable sticky-header"})[0]

    table_rows = table.find_all('tr')

    return table_rows

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]
    
    return text.replace('\n', '')

def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home': clean_text(tds[6].text)
        }
        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "OK"

def get_lat_long(country, city):
    url = f'https://nominatim.openstreetmap.org/search?q={city}+{country}&format=json&limit=1'

    headers = {

        'User-Agent': 'My custom user agent'

    }

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 200:

        data = response.json()

        if data:

            return data[0]['lat'], data[0]['lon']

    return None

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data =json.loads(data)

    stadiums_df = pd.DataFrame(data)

    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis = 1)

    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)

    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # Handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis = 1)
    stadiums_df.update(duplicates)

    # Push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())
    
    return "OK"

def write_wikipedia_data(**kwargs):
    # from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
    # from datetime import datetime

    # data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    # data = json.loads(data)

    # data = pd.DataFrame(data)

    # filename = ("stadium_cleaned_" + str(datetime.now().date()) + "_" + str(datetime.now().time()).replace(":", "_") + ".csv")

    # #data.to_csv('data/' + filename, index=False)

    # data.to_csv('abfs://minhfootballproject@footballdataeng.dfs.core.windows.get/data/' + filename,
    #             storage_options={
    #                 'account_key': 'FtYAieWSGCRs7GPzCyHPy2n21SSMWiTCFq2sBCJCLVxFDkRovNqzx+GrYj6PBp0VH1ORI5C9h/6G+AStCPbazQ=='
    #             }, index=False)

    from azure.storage.blob import BlobServiceClient
    from datetime import datetime
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    data = json.loads(data)
    data = pd.DataFrame(data)

    filename = "stadium_cleaned_" + str(datetime.now().date()) + "_" + str(datetime.now().time()).replace(":", "_") + ".csv"

    # Your connection string
    connection_string = "DefaultEndpointsProtocol=https;AccountName=minhfootballproject;AccountKey=FtYAieWSGCRs7GPzCyHPy2n21SSMWiTCFq2sBCJCLVxFDkRovNqzx+GrYj6PBp0VH1ORI5C9h/6G+AStCPbazQ==;EndpointSuffix=core.windows.net"

    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Name of the container
    container_name = "footballdataeng"

    # Name of the folder
    folder_name = "data"

    # Upload the CSV data to the container
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(folder_name + '/' + filename)
    blob_client.upload_blob(data.to_csv(index=False), overwrite=True)