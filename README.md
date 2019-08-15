**Instructions**

1. Business case
Sparkify company  is moving to the cloud to align its processes with the growth of the business. It's data is stored in Amazon S3 in the form of JSON log files containing it's users' activities as well as it's songs' metadata. The company needs an ETL pipeline developed which will extract data from the json files in S3 and load it into Redshift database so that the analytics team can unlock insights into what songs their users are listening to.

2. Data extraction , transformation and loading
A python script file etl.py has been developed to extract, transform and load user activity information from S3 into Redshift database

3. Execute ETL pipeline
i. Open the Execute.ipynb notebook
ii. Run the first line of code  which runs the create_table.py script to drop any existing tables and create new tables in Redshift
iii. Run the second line of code to execute the etl.py script which reads data from the json log files and songs metadata in S3 into Redshift
iv. Open the test.ipynb notebook and run the lines of codes to verify  that data has been imported into the database

4. Sample songs analysis

To get the top 5 most listened-to songs you need to execute the line of code below in your test.ipynb notebook

%sql SELECT song_id, sum(duration) FROM songs GROUP BY songs.song_id ORDER BY sum(duration) DESC LIMIT 5

5. Database design

**Fact table**

**i. songplays table : play records from the log data**

columns:

songplay_id bigint identity(0,1) PRIMARY KEY
start_time timestamp NOT NULL -> foreign key to reference the time table which contains specific date and time units
user_id int NOT NULL
level varchar NOT NULL
song_id varchar
artist_id varchar
session_id int NOT NULL
location varchar 
user_agent varchar 


**Dimension tables**

**i. users : record of users in the app **

columns:

user_id int PRIMARY KEY
first_name varchar 
last_name varchar 
gender char
level varchar


**ii.  songs : records of available songs in the music database **

columns: 

song_id varchar PRIMARY KEY
title varchar 
artist_id varchar    -> foreign key to reference the artists table based on the artist_id
year int
duration numeric 


**iii. artists : artists in the music database**

columns:

artist_id varchar PRIMARY KEY
name varchar NOT NULL
location varchar
latitude numeric
longitude numeric

**iv: time : timestamps of records in songplays broken down into specific units**


columns:

start_time timestamp   PRIMARY KEY
hour int NOT NULL
day int NOT NULL
week int NOT NULL
month int NOT NULL
year int NOT NULL
weekday int NOT NULL
