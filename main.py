#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
import json
import os
import time

import requests
import pandas as pd
import settings

import google.cloud.exceptions
from google.cloud import storage
from google.cloud import bigquery


def upload_df_to_bigquery(dataframe: pd.DataFrame, project_id: str, dataset_id: str, table_name: str):
    """Uploads a pandas DataFrame to a BigQuery table.

    Args:
        dataframe (pd.DataFrame): The pandas DataFrame to upload.
        project_id (str): Your GCP project ID.
        dataset_id (str): The ID of the BigQuery dataset where the table will be created.
        table_name (str): The name of the BigQuery table to create.

    Returns:
        None
    """

    # Read Google Application Credentials only if working on a local environment
    if settings.is_local_environment:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.bigquery_service_account

    client = bigquery.Client()
    dataset_id = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "europe-west8"

    try:
        dataset = client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except google.cloud.exceptions.Conflict as e:
        print(e)

    table_id = f"{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED'
    )
    print("Created a BigQuery job_config variable")

    try:
        job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
        job.result()
        print("Saved data into BigQuery")
    except Exception as e:
        print(dataframe.dtypes)
        print(table_id)
        print(job_config)
        print(e)
        raise e


def upload_json_gcs(json_data: dict, bucket_name: str, folder_path: str) -> None:
    """Uploads a JSON object to Google Cloud Storage.

    Args:
        json_data (dict): The JSON data to upload.
        bucket_name (str): The name of the bucket to upload the data to.
        folder_path (str): The folder path within the bucket to store the data (optional).
    """

    # Read Google Application Credentials only if working on a local environment
    if settings.is_local_environment:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.cloud_storage_service_account

    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket_name)
    except (google.cloud.exceptions.NotFound, google.cloud.exceptions.Forbidden) as e:
        print(e)
        bucket = client.create_bucket(bucket_name)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    filename = f"{timestamp}.json"
    object_path = os.path.join(folder_path, filename)
    blob = bucket.blob(object_path)
    blob.upload_from_string(json.dumps(json_data).encode("utf-8"), content_type="application/json")

    print(f"Uploaded {filename} to {bucket_name}/{object_path}")


def fetch_api_data(url: str) -> dict:
    """ Fetches data from the specified API URL and returns the JSON response.

     Args:
        url: The URL of the API endpoint.

        Returns:
            The JSON data parsed from the API response, or raises an exception on error.

        Raises:
            requests.exceptions.HTTPError: If the API request fails.
    """

    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def fetch_weather_data(loc: dict, key: str) -> dict:
    """ Fetches current weather data for multiple locations.

        Args:
            loc (dict): A dictionary containing location information.
                Each key represents a location name, and the value is another dictionary
                with 'lat' and 'lon' keys for latitude and longitude.
            key (str): Your OpenWeatherMap API key.

        Returns:
            dictionary: containing a dictionary for current weather data
                keyed by location name.

        Raises:
            requests.exceptions.RequestException: If an error occurs during API requests.
        """

    base_url = 'https://api.openweathermap.org/data/2.5/'
    weather_data = {"current": {}}

    for location_name, location in loc.items():
        try:
            weather_url = f"{base_url}weather?lat={location['lat']}&lon={location['lon']}&appid={key}&units=metric"
            weather_data["current"][location_name] = fetch_api_data(weather_url)
            weather_data["current"][location_name]["created_at"] = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {location_name}: {e}")
    return weather_data


def fetch_forecast_data(loc: dict, key: str) -> dict:
    """ Fetches forecast weather data for multiple locations.

        Args:
            loc (dict): A dictionary containing location information.
                Each key represents a location name, and the value is another dictionary
                with 'lat' and 'lon' keys for latitude and longitude.
            key (str): Your OpenWeatherMap API key.

        Returns:
            dictionary: containing a dictionary for forecasted weather data
                keyed by location name.

        Raises:
            requests.exceptions.RequestException: If an error occurs during API requests.
        """

    base_url = 'https://api.openweathermap.org/data/2.5/'
    forecast_data = {"forecast": {}}

    for location_name, location in loc.items():
        try:
            forecast_url = f"{base_url}forecast?lat={location['lat']}&lon={location['lon']}&appid={key}&units=metric"
            forecast_data["forecast"][location_name] = fetch_api_data(forecast_url)
            forecast_data["forecast"][location_name]["city"]["created_at"] = datetime.datetime.now().strftime(
                "%d/%m/%Y %H:%M")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {location_name}: {e}")
    return forecast_data


def transform_current_weather_data(data_dict: dict) -> pd.DataFrame:
    """Transforms weather API data into a Pandas DataFrame suitable for BigQuery.

    Args:
        data_dict (dict): A dictionary containing weather data.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the transformed weather data.
    """

    if isinstance(data_dict['weather'], list):
        data_dict['weather'] = data_dict['weather'][0]

    flattened_data = {}
    for key, value in data_dict.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flattened_data[f"{key}_{sub_key}"] = sub_value
        else:
            flattened_data[key] = value

    data_df = pd.DataFrame([flattened_data])
    if 'dt' in data_df.columns:
        data_df['dt_txt'] = pd.to_datetime(data_df['dt'], unit='s')
        data_df['dt_txt'] = data_df['dt_txt'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return data_df


def convert_weather_api_dict_to_dataframe(data_dict: dict) -> pd.DataFrame:
    """
    Converts a nested dictionary containing weather data from the Weather API to a Pandas DataFrame.

    Args:
        data_dict (dict): The dictionary containing the weather data.

    Returns:
        pd.DataFrame: A DataFrame representing the weather data.
    """

    extracted_data = {}
    for key, value in data_dict.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                extracted_data[f"{key}_{sub_key}"] = sub_value
        else:
            extracted_data[key] = value
    return pd.DataFrame([extracted_data])


def transform_forecasted_weather_data(data_dict: dict) -> pd.DataFrame:
    """
     Transforms the forecasted weather data from the Weather API into a Pandas DataFrame.

     Args:
         data_dict (dict): The dictionary containing the forecasted weather data.

     Returns:
         pd.DataFrame: A DataFrame containing the transformed forecasted weather data.
     """

    city_dict = data_dict['city']
    city_df = convert_weather_api_dict_to_dataframe(city_dict)

    forecast_dict = data_dict['list']
    forecast_df = pd.DataFrame()
    for forecast_item in forecast_dict:
        forecast_item['weather'] = forecast_item['weather'][0]
        forecast_item_df = convert_weather_api_dict_to_dataframe(forecast_item)
        forecast_df = pd.concat([forecast_df, forecast_item_df], ignore_index=True)
    return forecast_df.merge(city_df, how='cross')


def main(request: dict) -> str:
    try:
        request_boy = request.get_json()
    except:
        request_body = json.loads(request)

    api_key = settings.api_key
    my_bucket_name = settings.bucket_name

    locations = {
        'Athens': {'lat': '37.97945', 'lon': '23.71622'},
        'Thessaloniki': {'lat': '40.629269', 'lon': '22.947412'},
        'Patras': {'lat': '38.246639', 'lon': '21.734573'},
        'Piraeus': {'lat': '37.94745', 'lon': '23.63708'},
        'Heraklion': {'lat': '35.341846', 'lon': '25.148254'},
    }

    weather_data = fetch_weather_data(locations, api_key)
    forecast_data = fetch_forecast_data(locations, api_key)
    current_weather = pd.DataFrame()
    forecast_weather = pd.DataFrame()

    for key, value in weather_data['current'].items():
        current_weather = pd.concat([current_weather, transform_current_weather_data(value)])

    for key, value in forecast_data['forecast'].items():
        forecast_weather = pd.concat([forecast_weather, transform_forecasted_weather_data(value)])

    for key, value in weather_data['current'].items():
        folder_path_current = f'current_weather/{key}'
        json_data = value
        folder_path_current = folder_path_current
        upload_json_gcs(json_data, my_bucket_name, folder_path_current)

    for key, value in forecast_data['forecast'].items():
        folder_path_current = f'forecasted_weather/{key}'
        json_data = value
        folder_path_current = folder_path_current
        upload_json_gcs(json_data, my_bucket_name, folder_path_current)

    upload_df_to_bigquery(dataframe=current_weather, project_id=settings.project_id, dataset_id=settings.dataset_id,
                          table_name='current_weather')
    upload_df_to_bigquery(dataframe=forecast_weather, project_id=settings.project_id, dataset_id=settings.dataset_id,
                          table_name='forecasted_weather')

    return '200, Success'


if __name__ == '__main__':
    data = {}
    payload = json.dumps(data)
    print(main(payload))
