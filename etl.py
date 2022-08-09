import os
import glob
import psycopg2
import pandas as pd
import datetime
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
from sql_queries import *


def process_song_file(cur, filepath):
    """ Processes song file, assigns data to three separate Pandas dataframes
            df: a direct read of the JSON file
            song_data: 'song_id', 'title', 'artist_id', 'year', 'duration'
            artist_data: 'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'
        
        Excutes song_table_insert and artist_table_insert to store data in respective Postgres tables.
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ Processes log file, assigns data to four separate Pandas dataframes
            df: a direct read of the JSON file, filtered by "NextSong" action
            time_df: 'time','hour','day','week','month','year','weekday_name'
            user_df: 'userId','firstName','lastName','gender','level'
            songplay_data: 'timestamp','userId','level','songid','artistid','sessionId','location','userAgent'
            
        Excutes time_table_insert, user_table_insert, and songplay_table_insert to store data in respective Postgres tables.

    """ 
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday_name)
    column_labels = ("time","hour","day","week","month","year","weekday_name")
    time_dict = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]].drop_duplicates("userId")

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
        songplay_data = ((pd.to_datetime(row['ts'], unit='ms'),int(row['userId']),row['level'],songid,artistid,row['sessionId'],row['location'],row['userAgent']))
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Appends all JSON files in a given directory to a list. Each filepath in the list
    is then passed as an argument to the provided function that will extract the
    appropriate data.
    
    Arguments:
        cur: a cursor to perform the database operations
        conn: database connection
        filepath: path to the data directory
        func: function to process the data file
    """
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
    """
    Calls process_data function for both main data directories.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
