import psycopg2
import pandas as pd
import os
from sql_queries import insert_table_artists, insert_table_songplays, insert_table_songs,\
     insert_table_time, insert_table_users, create_table_queries, drop_table_queries, check_song_id

def insert_records(cur, conn, insert_query, df):
    for ind in range(len(df)):
        cur.execute(insert_query, df.values[ind].tolist())
        conn.commit()

def load_data(log_path = 'data/log_data/2018/11/', song_path = 'data/song_data/A/'):
    log_data = pd.read_json(log_path+os.listdir(log_path)[0], lines=True)
    for i in os.listdir(log_path)[1:]:
        log_data = pd.concat([log_data, pd.read_json(log_path+i, lines=True)]) 
    
    path = ['A', 'B']
    path_2 = ['A', 'B', 'C']
    all_path = []
    for i in path:
        for j in path_2:
            for p in os.listdir(f'{song_path}{i}/{j}/'):
                all_path.append(f'{song_path}{i}/{j}/{p}')
    song_data = pd.read_json(all_path[0], lines=True)
    for i in all_path[1:]:
        song_data = pd.concat([song_data, pd.read_json(i, lines=True)])

    return log_data, song_data

def connect_database():
    conn = psycopg2.connect(database="sparkifydb", user='postgres', password='0908', host='127.0.0.1', port= '5432')
    cursor = conn.cursor()

    return conn, cursor

def insert_data(log_data, song_data, conn, cursor):
    user_df = log_data[['userId','firstName','lastName','gender','level']]
    user_df = user_df[user_df['userId']!='']
    user_list = [user_df['userId'].astype('int').to_list()]+[user_df[i].to_list() for i in ['firstName','lastName','gender','level']]
    user_df = pd.DataFrame({'userId':user_list[0],'firstName':user_list[1],'lastName':user_list[2],'gender':user_list[3],'level':user_list[4]})

    song_df = song_data[['song_id','title','artist_id','year','duration']]
    song_list = [song_df[i].to_list() for i in ['song_id','title','artist_id']] \
                + [song_df['year'].astype('int').to_list(), song_df['duration'].astype('int').to_list()]
    song_df = pd.DataFrame({'song_id':song_list[0],'title':song_list[1],'artist_id':song_list[2],'year':song_list[3],'duration':song_list[4]})
            
    artist_df = song_data[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]

    time_df = pd.DataFrame({'start_time': log_data['ts']})
    time_df['year'] = pd.to_datetime(log_data['ts'], unit='ms').dt.year.astype('int')
    time_df['month'] = pd.to_datetime(log_data['ts'], unit='ms').dt.month.astype('int')
    time_df['week'] = pd.to_datetime(log_data['ts'], unit='ms').dt.isocalendar().week.astype('int')
    time_df['day'] = pd.to_datetime(log_data['ts'], unit='ms').dt.day.astype('int')
    time_df['hour'] = pd.to_datetime(log_data['ts'], unit='ms').dt.hour.astype('int')
    time_df['weekday'] = pd.to_datetime(log_data['ts'], unit='ms').dt.weekday.astype('int')
    time_df = time_df[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]

    insert_records(cursor, conn, insert_table_artists, artist_df)
    insert_records(cursor, conn, insert_table_songs, song_df)
    insert_records(cursor, conn, insert_table_time, time_df)
    insert_records(cursor, conn, insert_table_users, user_df)

    log_data = log_data[log_data['userId']!='']
    songplay_list = []
    for i in ['userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent', 'length']:
        if log_data[i].dtype == 'int64':
            songplay_list.append(log_data[i].astype('int').to_list())
        else:
            songplay_list.append(log_data[i].to_list())

    songplay_df = pd.DataFrame({'ts': log_data['ts'].to_list(), 'userId':songplay_list[0], 'level':songplay_list[1], 'song':songplay_list[2], \
        'artist':songplay_list[3], 'sessionId':songplay_list[4], 'location':songplay_list[5], 'userAgent':songplay_list[6],\
             'length':songplay_list[7]})

    for i in range(len(songplay_df)):
        title = songplay_df['song'].to_list()[i]
        artist_name = songplay_df['artist'].to_list()[i]
        duration = songplay_df['length'].to_list()[i]

        results = cursor.execute(check_song_id, (title, artist_name, duration))
        song_id, artist_id = results if results else None, None

        cursor.execute(insert_table_songplays, songplay_df[['ts']].iloc[i].astype('int64').to_list() \
        + songplay_df[['userId']].iloc[i].astype('int').to_list() \
        + songplay_df[['level']].iloc[i].to_list() \
        + [song_id, artist_id]\
        + songplay_df[['sessionId']].iloc[i].astype('int').to_list() \
        + songplay_df[['location', 'userAgent']].iloc[i].to_list())
        conn.commit()

def main():
    conn, cursor = connect_database()
    log_data, song_data = load_data(log_path = 'data/log_data/2018/11/', song_path = 'data/song_data/A/')
    insert_data(log_data, song_data, conn, cursor)
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()