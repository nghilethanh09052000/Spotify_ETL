import sqlalchemy
import pandas as pd
import json
from sqlalchemy.orm import sessionmaker
import requests
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = 'sqlite:///recently_played_tracks.sqlite'
USER_ID = '31zchtm5m5ueleb2lqmzlnsbh3bu'
TOKEN = 'BQDliPkSAhdcBWIaUUvZWxU8BA_OoKXG0Sv5gZeeVASYNYW3PjhdJxM3QF5DAHfBI4jAHXgJmoseyuXBpjmV3pjtxKMoWGBFa80aXJcN7B1ExtbG-ifyQ4y_fxelUKbOOA-PQj8bmi4XOCzOk6NIANHPIdkdbONC1VqDH4eZA24'

def data_validation(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")
    #check for nulls
    if df.isnull().values.any():
        raise Exception("Null value found")
    #check that all timestamps are of yesterday's date
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
    #
    # timetamps = df['timestamp'].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
    #         raise Exception("At least one of the returned songs does not come from within the last 24 hours")
    # return True


if __name__ == "__main__":
    headers = {
        "Accept":"application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    today = datetime.datetime.now()
    yesterday = today-datetime.timedelta(days=10)
    yesterday_unit_timestamp = int(yesterday.timestamp()) * 1000
    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unit_timestamp),headers=headers)
    data = r.json()
    # print(data)
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    popularities = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])
        popularities.append(song['track']['popularity'])

    song_di = {
    'song_name': song_names,
    'artist_name': artist_names,
    'played_at': played_at_list,
    'timestamp': timestamps,
    'popularity': popularities
    }

    song_df = pd.DataFrame(song_di)
    # print(song_df)

#validate
if data_validation(song_df):
    print("Data valid, proceed to Load Stage")

#Load Stage
engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('recently_played_tracks.sqlite')
cur = conn.cursor()
sql_script = 'drop table if exists my_played_tracks;'
sql_script2 = """
create table my_played_tracks (
song_name varchar(200),
artist_name varchar(200),
played_at varchar(200) primary key,
timestamp varchar(200),
popularity integer
)"""

cur.execute(sql_script)
cur.execute(sql_script2)
print("Enter database successfully")

try:
    song_df.to_sql('recently_played_tracks.sqlite', engine, index = False, if_exists = 'append')
except:
    print('Data already exists in the database')


conn.close()
print('Close database successfully')
