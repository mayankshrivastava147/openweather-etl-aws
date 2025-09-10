import json
import requests, pandas as pd,os
from datetime import datetime
import boto3

def lambda_handler(event, context):

    API_KEY=os.environ.get('API_KEY')
    API_KEY = API_KEY
    CITIES = [("Bhopal","IN"), ("Sydney","AU"), ("Dubai","AE"), ("Delhi","IN"), ("Mumbai","IN")]

    def ow_fetch(endpoint, params):
        url = f"https://api.openweathermap.org/data/2.5/{endpoint}"
        params = {**params, "appid": API_KEY, "units": "metric"}
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    current_weather=[]
    forecast_data=[]

    for city,country in CITIES:
        j = ow_fetch("weather", {"q": f"{city},{country}"})
        current_weather.append(j)


    timestamp = "current_weather_"+str(datetime.now())+".json"
    client = boto3.client('s3')
    client.put_object(
        Bucket='weather-api-etl-project',
        Key='raw_data/to_be_processed/current_data/'+timestamp,
        Body=json.dumps(current_weather)
    )


    for city,country in CITIES:
        f = ow_fetch("forecast", {"q": f"{city},{country}"})
        forecast_data.append(f)


    timestamp = "forecast_"+str(datetime.now()) +".json"    
    client = boto3.client('s3')
    client.put_object(
        Bucket='weather-api-etl-project',
        Key='raw_data/to_be_processed/forecast_data/'+timestamp,
        Body=json.dumps(forecast_data)
    )

