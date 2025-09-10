import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd
from urllib.parse import unquote_plus   

def current(data):
    rows = []
    for j in data:
        row = {
            "city": j["name"],
            "country": j['sys']['country'],
            "lat": j['coord']['lat'],
            "lon": j['coord']['lon'],
            "temp": j['main']['temp'],
            "feels_like": j['main']['feels_like'],
            "max_temp": j['main']['temp_max'],
            "min_temp": j['main']['temp_min'],
            "humidity": j['main']['humidity'],
            "pressure": j['main']['pressure'],
            "wind_speed": j['wind']['speed'],
            "visibility": j.get('visibility', 0),
            "sunrise": j['sys']['sunrise'],
            "sunset": j['sys']['sunset'],
            "date": j['dt']
        }
        rows.append(row)
    return rows

def forecaste_data(data):
    out = []
    for d in data:
        for it in d["list"]:
            out.append({
                "city": d["city"]["name"],
                "country": d["city"]["country"],
                "dt_txt": it["dt_txt"],
                "temp": it["main"]["temp"],
                "feels_like": it["main"]["feels_like"],
                "pressure": it["main"]["pressure"],
                "humidity": it["main"]["humidity"],
                "wind_speed": it["wind"]["speed"],
                "clouds_pct": it["clouds"]["all"],
                "pop": it.get("pop"),
                "rain_3h": (it.get("rain") or {}).get("3h", 0),
                "weather": it["weather"][0]["main"],
                "weather_desc": it["weather"][0]["description"],
            })
    return out

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    s3r = boto3.resource('s3')
    bucket = 'weather-api-etl-project'

    # ===== read event key =====
    event_key = None
    if isinstance(event, dict) and 'Records' in event:
        event_key = event['Records'][0]['s3']['object']['key']
        event_key = unquote_plus(event_key)     
        print("EVENT KEY:", event_key)          

    # ---------- CURRENT ----------
    if (event_key and "raw_data/to_be_processed/current_data/" in event_key):
        file_key = event_key
        resp = s3.get_object(Bucket=bucket, Key=file_key)
        data = json.loads(resp['Body'].read().decode('utf-8'))

        if data:
            df = pd.DataFrame(current(data))
            df['date']    = pd.to_datetime(df['date'],    unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")
            df['sunrise'] = pd.to_datetime(df['sunrise'], unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")
            df['sunset']  = pd.to_datetime(df['sunset'],  unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")
            df['visibility'] = df['visibility'] / 1000
            df = df.rename(columns={'visibility':'visibility_km'})
            df['wind_speed'] = df['wind_speed'] * 3.6
            df = df.rename(columns={'wind_speed':'wind_speed_km'})
            df = df.drop_duplicates()

            out_key = "transformed-data/current_transformed/current_transformed_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
            buf = StringIO(); df.to_csv(buf, index=False)
            s3.put_object(Bucket=bucket, Key=out_key, Body=buf.getvalue())

        copy_source = {'Bucket': bucket, 'Key': file_key}
        s3r.meta.client.copy(copy_source, bucket, "raw_data/processed/current_data/" + file_key.split('/')[-1])
        s3.delete_object(Bucket=bucket, Key=file_key)

    # ---------- FORECAST ----------
    elif (event_key and "raw_data/to_be_processed/forecast_data/" in event_key):
        file_key = event_key
        resp = s3.get_object(Bucket=bucket, Key=file_key)
        payload = json.loads(resp['Body'].read().decode('utf-8'))
        forecast_data = payload if isinstance(payload, list) else [payload]

        if forecast_data:
            df = pd.DataFrame(forecaste_data(forecast_data))
            df['wind_speed'] = df['wind_speed'] * 3.6
            df = df.rename(columns={'wind_speed':'wind_speed_km'})
            df['dt_txt'] = pd.to_datetime(df['dt_txt'], errors='coerce')
            df['date'] = df['dt_txt'].dt.date
            df['time'] = df['dt_txt'].dt.time
            df['hour'] = df['dt_txt'].dt.hour
            df = df[['city','country','dt_txt','date','time','hour','temp','feels_like',
                     'pressure','humidity','wind_speed_km','clouds_pct','pop','rain_3h',
                     'weather','weather_desc']].drop_duplicates()

            out_key = "transformed-data/forecast_transformed/forecast_transformed_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
            buf = StringIO(); df.to_csv(buf, index=False)
            s3.put_object(Bucket=bucket, Key=out_key, Body=buf.getvalue())

        copy_source = {'Bucket': bucket, 'Key': file_key}
        s3r.meta.client.copy(copy_source, bucket, "raw_data/processed/forecast_data/" + file_key.split('/')[-1])
        s3.delete_object(Bucket=bucket, Key=file_key)

    else:
        # Fallback (console test)
        cur_keys = []
        for file in s3.list_objects_v2(Bucket=bucket, Prefix="raw_data/to_be_processed/current_data/").get('Contents', []):
            k = file['Key']
            if not k.endswith('.json'):
                continue
            body = s3.get_object(Bucket=bucket, Key=k)['Body'].read().decode('utf-8')
            data = json.loads(body)
            if data:
                df = pd.DataFrame(current(data))
                df['date']    = pd.to_datetime(df['date'],    unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")
                df['sunrise'] = pd.to_datetime(df['sunrise'], unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")
                df['sunset']  = pd.to_datetime(df['sunset'],  unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")
                df['visibility'] = df['visibility'] / 1000
                df = df.rename(columns={'visibility':'visibility_km'})
                df['wind_speed'] = df['wind_speed'] * 3.6
                df = df.rename(columns={'wind_speed':'wind_speed_km'}).drop_duplicates()
                out_key = "transformed-data/current_transformed/current_transformed_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
                buf = StringIO(); df.to_csv(buf, index=False)
                s3.put_object(Bucket=bucket, Key=out_key, Body=buf.getvalue())
            cur_keys.append(k)
        for k in cur_keys:
            copy_source = {'Bucket': bucket, 'Key': k}
            s3r.meta.client.copy(copy_source, bucket, "raw_data/processed/current_data/" + k.split('/')[-1])
            s3.delete_object(Bucket=bucket, Key=k)

        fc_keys = []
        for file in s3.list_objects_v2(Bucket=bucket, Prefix="raw_data/to_be_processed/forecast_data/").get('Contents', []):
            k = file['Key']
            if not k.endswith('.json'):
                continue
            body = s3.get_object(Bucket=bucket, Key=k)['Body'].read().decode('utf-8')
            payload = json.loads(body)
            data = payload if isinstance(payload, list) else [payload]
            if data:
                df = pd.DataFrame(forecaste_data(data))
                df['wind_speed'] = df['wind_speed'] * 3.6
                df = df.rename(columns={'wind_speed':'wind_speed_km'})
                df['dt_txt'] = pd.to_datetime(df['dt_txt'], errors='coerce')
                df['date'] = df['dt_txt'].dt.date
                df['time'] = df['dt_txt'].dt.time
                df['hour'] = df['dt_txt'].dt.hour
                df = df[['city','country','dt_txt','date','time','hour','temp','feels_like',
                         'pressure','humidity','wind_speed_km','clouds_pct','pop','rain_3h',
                         'weather','weather_desc']].drop_duplicates()
                out_key = "transformed-data/forecast_transformed/forecast_transformed_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
                buf = StringIO(); df.to_csv(buf, index=False)
                s3.put_object(Bucket=bucket, Key=out_key, Body=buf.getvalue())
            fc_keys.append(k)
        for k in fc_keys:
            copy_source = {'Bucket': bucket, 'Key': k}
            s3r.meta.client.copy(copy_source, bucket, "raw_data/processed/forecast_data/" + k.split('/')[-1])
            s3.delete_object(Bucket=bucket, Key=k)
