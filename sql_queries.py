import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_event
                                     (event_id INTEGER IDENTITY(0,1),
                                      artist_name VARCHAR,
                                      auth VARCHAR,
                                      user_first_name VARCHAR,
                                      user_gender VARCHAR,
                                      item_in_session INTEGER,
                                      user_last_name VARCHAR,
                                      song_length DOUBLE PRECISION, 
                                      user_level VARCHAR,
                                      location VARCHAR,	
                                      method VARCHAR,
                                      page VARCHAR,	
                                      registration VARCHAR,	
                                      session_id VARCHAR,
                                      song_title VARCHAR,
                                      status INTEGER, 
                                      ts VARCHAR,
                                      user_agent VARCHAR,	
                                      user_id VARCHAR,
                                      PRIMARY KEY (event_id))
                                """)

staging_songs_table_create = ("""CREATE TABLE staging_song
                                    (song_id VARCHAR,
                                     num_songs INTEGER,
                                     artist_id VARCHAR,
                                     artist_latitude DOUBLE PRECISION,
                                     artist_longitude DOUBLE PRECISION,
                                     artist_location VARCHAR,
                                     artist_name VARCHAR,
                                     title VARCHAR,
                                     duration DOUBLE PRECISION,
                                     year INTEGER,
                                     PRIMARY KEY (song_id))
                                """)

songplay_table_create = ("""CREATE TABLE songplay 
                               (songplay_id INTEGER IDENTITY(0,1), 
                                start_time TIMESTAMP, 
                                user_id VARCHAR, 
                                level VARCHAR, 
                                song_id VARCHAR, 
                                artist_id VARCHAR, 
                                session_id VARCHAR, 
                                location VARCHAR,
                                user_agent VARCHAR,
                                PRIMARY KEY (songplay_id)) 
                        """)

user_table_create = ("""CREATE TABLE users
                            (user_id VARCHAR,
                             first_name VARCHAR, 
                             last_name VARCHAR, 
                             gender VARCHAR, 
                             level VARCHAR, 
                             PRIMARY KEY (user_id))
                        """)

song_table_create = ("""CREATE TABLE song
                            (song_id VARCHAR, 
                             title VARCHAR, 
                             artist_id VARCHAR, 
                             year INTEGER, 
                             duration DOUBLE PRECISION, 
                             PRIMARY KEY (song_id))
                        """)

artist_table_create = ("""CREATE TABLE artist
                              (artist_id VARCHAR, 
                               name VARCHAR, 
                               location VARCHAR, 
                               latitude DOUBLE PRECISION, 
                               longitude DOUBLE PRECISION,
                               PRIMARY KEY (artist_id))
                        """)

time_table_create = ("""CREATE TABLE time
                             (start_time TIMESTAMP,
                              hour INTEGER, 
                              day INTEGER, 
                              week INTEGER, 
                              month INTEGER, 
                              year INTEGER, 
                              weekday INTEGER,
                              PRIMARY KEY (start_time))
                        """)

# STAGING TABLES

staging_events_copy = ("""copy staging_event from {}
                          iam_role {}
                          region 'us-west-2'
                          json {}
                          compupdate off;
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_song from {}
                          iam_role {}
                          region 'us-west-2'
                          json 'auto'
                          compupdate off;
""").format(config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time, 
                                   user_id, user_level, song_id, artist_id, session_id, artist_location, user_agent
                            FROM staging_event e 
                            JOIN staging_song s ON e.song_title = s.title
                            WHERE e.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id, user_first_name, user_last_name, user_gender, user_level
                        FROM staging_event
                        WHERE page = 'NestSong';
""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_song;
""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_song;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT start_time,
                               EXTRACT(hour FROM start_time) AS hour,
                               EXTRACT(day FROM start_time) AS day,
                               EXTRACT(week FROM start_time) AS week,
                               EXTRACT(month FROM start_time) AS month,
                               EXTRACT(year FROM start_time) AS year,
                               EXTRACT(weekday from start_time) AS weekday 
                        FROM (
                            SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time 
                            FROM staging_event);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
