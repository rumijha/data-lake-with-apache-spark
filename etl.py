import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import Window
import pyspark.sql.functions as F


# reading configuration details 
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Reading song_data file that resides in S3
    - Selecting necessary infoformation from song_data to create and insert records into songs_table and artists_table
    - Writing data of songs_table and artists_table back to provided s3 destination path
    """
    # fetching filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # reading song data file
    df = spark.read.json(song_data)
    # df.head()
        
    
    # extracting necessary columns from song_data to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates(["song_id"])
    #songs_table.createOrReplaceTempView("songs_table")
    
    
    # writing songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.save(path=output_data, source='parquet', mode='overwrite').partitionBy("year","artist_id")
    #dataFrame.write.mode(SaveMode.Overwrite).partitionBy("eventdate", "hour", "processtime").parquet(path)

    
    # extracting necessary columns from song_data to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).dropDuplicates(["artist_id"])
    #artists_table.createOrReplaceTempView("artists_table")

    # writing artists table to parquet files
    artists_table = artists_table.write.save(path=output_data, source='parquet', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    - Reading log_data file that resides in S3
    - Selecting necessary infoformation from log_data to create and insert records into users_table, time_table 
    - Reading both song_data and log_data and selecting necessary details to create and insert data into songplays_table
    - Writing data of users_table, time_table and songplays_table back to provided s3 destination path
    """
    # fetching filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # reading log data file
    df = spark.read.json(log_data)
    
    # filtering log data for song plays by providing condition as page = "NextSong"
    df = df.filter(df.page == "NextSong")

    # extracting necessary columns from lod_data to create for users table    
    users_table = df.select(["userid", "firstName", "lastName", "gender", "level"])
    # users_table.show()
    
    # writing users table to parquet files
    users_table = users_table.write.save(path=output_data, source='parquet', mode='overwrite')
    
    # creating timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
  
    # creating datetime column from original timestamp column
    df = df.withColumn("datetime", F.date_format((df.timestamp),"yyyy-MM-dd"))\
           .withColumn("hour", F.hour(df.timestamp))\
           .withColumn("day", F.dayofweek(df.timestamp))\
           .withColumn("week", F.weekofyear(df.timestamp))\
           .withColumn("month", F.month(df.timestamp))\
           .withColumn("year", F.hour(df.timestamp))\
           .withColumn("weekday", F.dayofweek(df.timestamp))\
   
    # extracting necessary columns from log_data to create time table
    time_table = df.select(["datetime", "hour", "day", "week", "month", "year", "weekday"])
    #time_table = df.select(["datetime as start_time", "hour", "day", "week", "month", "year", "weekday"])
    
    # writing time table to parquet files partitioned by year and month
    time_table = time_table.write.save(path=output_data, source='parquet', mode='overwrite').partitionBy("year","month")
   
    

    # reading in song data and creating temporary table for song and log data which is used for creating songplays table
    song_df = input_data + "song_data/*/*/*/*.json"
    sdf = spark.read.json(song_df)
    
    sdf.createOrReplaceTempView("songTable")
    df.createOrReplaceTempView("logTable")
    

    # extracting columns by joining song and log datasets to create songplays table 
    songplays_table = spark.sql("""select l.ts, 
                                     l.userid as user_id, 
                                     l.level, 
                                     s.song_id as song_id, 
                                     s.artist_id, 
                                     l.sessionId as session_id, 
                                     l.location, 
                                     l.userAgent as user_agent
                                     from songTable s
                                     join logTable l on l.artist = s.artist_name and l.song = s.title
                                  """)
                                  
           
    # adding column to log_data table
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id()) 
    

    # writing songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.save(path=output_data, source='parquet', mode='overwrite').partitionBy("year","month")


def main():
    """
    - Fetching the connection details     
    - Establishes connection with aws and creating a cursor
    - Processing, creating and writing data to s3 by calling process_song_data and process_log_data function   
    - Finally, closing the connection. 
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://udacitybucketnew/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
