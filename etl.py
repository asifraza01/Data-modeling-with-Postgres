import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, typ='series')
    #print(df)

    # insert song record
    
    #artist_id, duration,song_id, title ,year                 
    song_data=df[['artist_id', 'duration', 'song_id','title', 'year' ]]
    
    
    cur.execute(song_table_insert,song_data)
    
    
    #artist_id,artist_latitude,artist_location,artist_longitude,artist_name
    artist_data =df[['artist_id','artist_latitude','artist_location','artist_longitude','artist_name']]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] ==  "NextSong"]
    

    # convert timestamp column to datetime
    #t = 
    
    # insert time data records
    #time_data = 
    #column_labels = 
    #time_df = 
    
    list_timestamp = []
    list_year = []
    list_month = []
    list_date = []
    list_hour = []
    list_minute = []
    list_second = []
    list_weekofyear = []
    list_weekday = []

    for td in df.ts:
        timestamp = (datetime.utcfromtimestamp(td/1000).strftime('%Y-%m-%d %H:%M:%S'))
        year = (datetime.utcfromtimestamp(td/1000).strftime('%Y'))
        month = (datetime.utcfromtimestamp(td/1000).strftime('%m'))
        date = (datetime.utcfromtimestamp(td/1000).strftime('%d'))
        hour = (datetime.utcfromtimestamp(td/1000).strftime('%H'))
        minute = (datetime.utcfromtimestamp(td/1000).strftime('%M'))
        second = (datetime.utcfromtimestamp(td/1000).strftime('%S'))
        weekofyear = (datetime.utcfromtimestamp(td/1000).strftime('%U'))
        weekday = (datetime.utcfromtimestamp(td/1000).strftime('%A'))
    
        list_timestamp.append(timestamp)
        list_year.append(year)
        list_month.append(month)
        list_date.append(date)
        list_hour.append(hour)
        list_minute.append(minute)
        list_second.append(second)
        list_weekofyear.append(weekofyear)
        list_weekday.append(weekday)
    df["normal_ts"] = list_timestamp
    df["year"] = list_year
    df["month"] = list_month
    df["date"] = list_date
    df["hour"] = list_hour
    df["min"] = list_minute
    df["sec"] = list_second
    df["weekofyear"] = list_weekofyear
    df["weekdayname"] = list_weekday

                
    dftemp = df.drop(["artist","auth","firstName","gender","itemInSession","lastName","length","level","location","method","page","registration","sessionId","song","status","userAgent","userId"], axis=1)
            
    time_df = dftemp.drop(["normal_ts","min","sec"], axis=1)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    
    user_df =df[['firstName','gender', 'lastName','level', 'userId']]
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, 
                     row.userId, 
                     row.level, 
                     songid, 
                     artistid, 
                     row.sessionId, 
                     row.location, 
                     row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()