## Data Lake with Apache Spark


#### Introduction


#### Project Description
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, and JSON metadata on the songs in their app. Here I have built an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. The processed data will allow the analytics team to find insights into what songs their users are listening to most frequently


### Datasets
Datasets used in this project resides in pulic access S3 bucket\
The datasets are JSON files namely song_data and log_data\
song_data holds the details of the song and its artists\
log_data hold the details of users which tell what songs is being heard and others\
These datasets will be processed in Redshift and stored back in S3


### Database Schema
**Fact Table**\
*songplay* It has details of the even that is specific to page="NextSong"

**Dimension Tables**\
*users* - Details of the users using the app\
*songs* - Details of the song\
*artists* - Details of the artists of their respective songs\
*time*


### Data Warehouse Configurations and Setup
Create an IAM user into you aws account\
Create an S3 bucket that holds the json files song_data and log_data\
Create redshift cluster\
Create an IAM roles that allows redshift cluster to access S3 bucket\
Provide Access Key on the configuration files


### ETL Process
Extracting the data from S3\
Fetching necessary details from song_data and Log_data json file to insert data into dimention and fact tables\
Writing the tables data back to S3


### How To Run the Project
**Step1:** Provide necessary aws details on dl.cfg file\
**Step2:** Implement etl.py file which will read song_data and Log_data json file transform it and write it back to provided S3 destination path

