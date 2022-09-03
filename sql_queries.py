create_table_queries = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            location VARCHAR(255),
            latitude DECIMAL,
            longitude DECIMAL
            
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR(255) PRIMARY KEY,
            title TEXT,
            artist_id VARCHAR(255),
            year INT,
            duration NUMERIC
            
        )
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INT PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            gender VARCHAR(255),
            level VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id SERIAL PRIMARY KEY,
            start_time BIGINT NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            level VARCHAR(255),
            song_id VARCHAR(255),
            artist_id VARCHAR(255),
            session_id INT,
            location VARCHAR(255),
            user_agent VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS time (
            start_time BIGINT PRIMARY KEY,
            hour INT,
            day INT,
            week INT,
            month INT,
            year INT,
            weekday INT
        )
        """
        )

drop_table_queries = ("DROP TABLE IF EXISTS artists",
                    "DROP TABLE IF EXISTS songs",
                    "DROP TABLE IF EXISTS users",
                    "DROP TABLE IF EXISTS fact_play_song",
                    "DROP TABLE IF EXISTS time")

insert_table_songplays = (""" INSERT INTO songplays
                    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """)

insert_table_artists = (""" INSERT INTO artists
                    (artist_id, name, location, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (artist_id) DO NOTHING""")

insert_table_songs = (""" INSERT INTO songs
                    (song_id, title, artist_id, year, duration)
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (song_id) DO NOTHING""")

insert_table_users = (""" INSERT INTO users
                    (user_id, first_name, last_name, gender, level)
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (user_id) DO NOTHING""")

insert_table_time = (""" INSERT INTO time
                    (start_time, hour, day, week, month, year, weekday)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) 
                    ON CONFLICT (start_time) DO NOTHING""")                    

check_song_id = """select song_id, artists.artist_id from songs 
    JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.artist_id = artists.artist_id 
    and title = %s
    and artists.name = %s 
    and songs.duration = %s"""