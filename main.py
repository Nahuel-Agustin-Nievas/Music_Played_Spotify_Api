import sqlalchemy
import pandas as pd
import requests
from datetime import datetime
import datetime
import sqlite3



DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "" # your Spotify username 
TOKEN = "" # your Spotify API token

# Generate your token here:  https://developer.spotify.com/console/get-recently-played/
# Note: You need a Spotify account (can be easily created for free)

def check_data(df: pd.DataFrame) -> bool:
    if df.empty:
        #Check if dataframe is empty
        print("No songs downloaded. Finish execution")
        return False
    
    #Primary Key Check in order to avoid duplicated data
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception ("Primary Key Check is violated")
    #Check for nulls
    if df.isnull().values.any():
        raise Exception("Null valued found")
    
    return True

    
    


if __name__ == "__main__":

    headers = { 
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token= TOKEN)

    }
    today = datetime.datetime.now()
    yesterday =  today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    # print(data)

    song_names = []
    artist_name = []
    played_at_list = []
    timestamps = []


    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_name.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])


    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_name,
        "played_at" : played_at_list,
        "timestamp": timestamps


    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    #Validate
    if check_data(song_df):
        print("Data ok")

    print(song_df)


    #Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully") 
