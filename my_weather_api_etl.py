#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().run_line_magic('env', 'OPENWEATHER_API_KEY=730d9869721adfcf066937a0a290cefd')


# In[3]:


import os
import requests, pandas as pd, json
from datetime import datetime

# DO NOT hardcode the key. Read from environment variable.
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise RuntimeError("Set OPENWEATHER_API_KEY env var before running this notebook.")

CITIES = [("Bhopal","IN"), ("Sydney","AU"), ("Dubai","AE"), ("Delhi","IN"), ("Mumbai","IN")]

def ow_fetch(endpoint: str, params: dict):
    base = "https://api.openweathermap.org/data/2.5"
    q = {**params, "appid": API_KEY, "units": "metric"}
    r = requests.get(f"{base}/{endpoint}", params=q, timeout=15)
    r.raise_for_status()
    return r.json()


# In[4]:


current_rows = []

for city,country in CITIES:
    j = ow_fetch("weather", {"q": f"{city},{country}"})
    row = {
        "city" : j["name"],
        "country" : j['sys']['country'],
        "lat" : j['coord']['lat'],
        "lon" : j['coord']['lon'],
        'temp' : j['main']['temp'],
        "feels_like" : j['main']['feels_like'],
        "max_temp" : j['main']['temp_max'],
        "min_temp": j['main']['temp_min'],
        "humidity" : j['main']['humidity'],
        "pressure" : j['main']['pressure'],
        "wind_speed" : j['wind']['speed'],
        "visibility" : j['visibility'],
        "sunrise" : j['sys']['sunrise'],
        "sunset"  : j['sys']['sunset'],
        "date" : j['dt']
        
    }
    current_rows.append(row)
    
df_current  = pd.DataFrame(current_rows)


# In[5]:


df_current['date'] = pd.to_datetime(df_current['date'], unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")
df_current['sunrise'] = pd.to_datetime(df_current['sunrise'], unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")
df_current['sunset']  = pd.to_datetime(df_current['sunset'], unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")


# In[6]:


df_current['visibility'] = df_current['visibility'] / 1000


# In[7]:


df_current=df_current.rename(columns={'visibility':'visibility_km'})


# In[8]:


df_current['wind_speed'] = df_current['wind_speed'] * 3.6


# In[9]:


df_current=df_current.rename(columns={'wind_speed':'wind_speed_km'})


# In[10]:


forecast_rows = []
for city, country in CITIES:
    f = ow_fetch("forecast", {"q": f"{city},{country}"})
    for it in f["list"]:
        forecast_rows.append({
            "city":     f["city"]["name"],
            "country":  f["city"]["country"],
            "dt_txt":   it["dt_txt"],
            "temp":     it["main"]["temp"],
            "feels_like": it["main"]["feels_like"],
            "pressure": it["main"]["pressure"],
            "humidity": it["main"]["humidity"],
            "wind_speed": it["wind"]["speed"],
            "clouds_pct": it["clouds"]["all"],
            "pop":      it.get("pop"),
            "rain_3h":  (it.get("rain") or {}).get("3h", 0),
            "weather":  it["weather"][0]["main"],
            "weather_desc": it["weather"][0]["description"],
        })

df_forecast = pd.DataFrame(forecast_rows)


# In[11]:


df_forecast['wind_speed'] = df_forecast['wind_speed'] * 3.6


# In[12]:


df_forecast = df_forecast.rename(columns={'wind_speed':'wind_speed_km'})


# In[13]:


df_forecast['dt_txt'] = pd.to_datetime(df_forecast['dt_txt'])


#  

# In[14]:


df_forecast['date'] = df_forecast['dt_txt'].dt.date

df_forecast['time'] = df_forecast['dt_txt'].dt.time


df_forecast['hour'] = df_forecast['dt_txt'].dt.hour


df_forecast = df_forecast[[
    'city', 'country',"dt_txt", 'date', 'time', 'hour',
    'temp', 'feels_like', 'pressure', 'humidity',
    'wind_speed_km', 'clouds_pct', 'pop', 'rain_3h',
    'weather', 'weather_desc'
]]


# In[ ]:





# In[ ]:




