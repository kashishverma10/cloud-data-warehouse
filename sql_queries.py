import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table "
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table "
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table "
user_table_drop = "DROP TABLE IF EXISTS user_table "
song_table_drop = "DROP TABLE IF EXISTS song_table "
artist_table_drop = "DROP TABLE IF EXISTS artist_table "
time_table_drop = "DROP TABLE IF EXISTS time_table "

# CREATE TABLES

staging_events_table_create= (""" 

CREATE TABLE IF NOT EXISTS staging_events_table (

    index INT IDENTITY(0,1)   PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    iteminSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT ,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts timestamp,
    userAgent VARCHAR,
    userId INT
);
""")

staging_songs_table_create = (""" 

CREATE TABLE IF NOT EXISTS staging_songs_table (

    index INT IDENTITY(0,1)   PRIMARY KEY,
    num_songs INT  ,
    artist_id VARCHAR  ,
    artist_latitude FLOAT  ,
    artist_longitude FLOAT  ,
    artist_location VARCHAR  ,
    artist_name VARCHAR  ,
    song_id VARCHAR  ,
    title VARCHAR  ,
    duration FLOAT  ,
    year INT  
);
""")

songplay_table_create = (""" 

CREATE TABLE IF NOT EXISTS songplay_table  (

    songplay_id INT IDENTITY(0,1)   PRIMARY KEY,
    start_time TIMESTAMP  ,
    user_id INT NOT NULL,
    level VARCHAR  ,
    song_id VARCHAR  ,
    artist_id VARCHAR  ,
    session_id INT  ,
    location VARCHAR  ,
    user_agent VARCHAR  


);
""")

user_table_create = (""" 

CREATE TABLE IF NOT EXISTS user_table (
    
    user_id INT  PRIMARY KEY NOT NULL,
    first_name VARCHAR  ,
    last_name VARCHAR  ,
    gender VARCHAR  ,
    level VARCHAR  
    
);
""")

song_table_create = (""" 

CREATE TABLE IF NOT EXISTS song_table(

    song_id VARCHAR   PRIMARY KEY,
    title VARCHAR  ,
    artist_id VARCHAR  ,
    year INT  ,
    duration FLOAT  

);
""")

artist_table_create = (""" 

CREATE TABLE IF NOT EXISTS artist_table(

    artist_id VARCHAR   PRIMARY KEY,
    name VARCHAR  ,
    location VARCHAR  ,
    lattitude FLOAT  ,
    longitude FLOAT  

);
""")

time_table_create = (""" 

CREATE TABLE IF NOT EXISTS time_table (

    start_time TIMESTAMP   PRIMARY KEY,
    hour FLOAT  ,
    day VARCHAR  ,
    week VARCHAR  ,
    month VARCHAR  ,
    year INT  ,
    weekday VARCHAR  

);
""")

# STAGING TABLES

staging_events_copy = (""" 
    
    COPY staging_events_table FROM {}
    iam_role {}
    FORMAT AS JSON {}
    COMPUPDATE OFF
    TIMEFORMAT as 'epochmillisecs'
    region 'us-east-1'
    ;  
    
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH']
            )

staging_songs_copy = ("""

    COPY staging_songs_table FROM {}
    iam_role {}
    json 'auto' 
    region 'us-east-1';

""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""

INSERT INTO songplay_table (

    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
)

SELECT DISTINCT 
    
    se.ts as start_time, 
    se.userid,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionid,
    se.location,
    se.useragent

FROM 
    staging_events_table se

JOIN 
    staging_songs_table ss

ON 
    se.artist = ss.artist_name AND se.song = ss.title

WHERE se.page ='NextSong';

""")

user_table_insert = ("""

INSERT INTO user_table (

    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
)

SELECT DISTINCT 
    se.userid, 
    se.firstname, 
    se.lastname, 
    se.gender,
    se.level

FROM staging_events_table se

WHERE se.page ='NextSong' AND se.userid IS NOT NULL;

""")

song_table_insert = ("""

INSERT INTO song_table (

   song_id, 
   title, 
   artist_id, 
   year, 
   duration
)

SELECT 

    song_id, 
    title, 
    artist_id, 
    year, 
    duration

FROM staging_songs_table ss;

""")

artist_table_insert = ("""

INSERT INTO artist_table (

    artist_id, 
    name, 
    location, 
    lattitude, 
    longitude
)

SELECT

    artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude

FROM staging_songs_table;

""")

time_table_insert = ("""

INSERT INTO time_table (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
)

SELECT DISTINCT
    ts as start_time, 
    EXTRACT(HOUR FROM ts) as hour,
    EXTRACT(DAY FROM ts) as day,
    EXTRACT(WEEK FROM ts) as week,
    EXTRACT(MONTH FROM ts) as month,
    EXTRACT(YEAR FROM ts) as year,
    EXTRACT(WEEKDAY FROM ts) as weekday

FROM staging_events_table


""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
