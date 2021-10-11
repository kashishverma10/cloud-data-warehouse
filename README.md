
# **Summary**

The purpose of this project is to create an ETL pipeline that helps extracting song and user activity log data residing in a S3 bucket in JSON format. The data in then staged in AWS Redshift Cluster. From the staging table the data is then transformed into a set of fact and dimensional table. Having a dataset in form of a star-schema database tables will allow Sparkify analytics team to analyze their users’ activity on their app and to get data reports relevant to their business initiatives

### **ETL Process:**

The schema of all the tables is defined in the create table queries in the sql_queries.py file. Also it has the drop table queries that are needed to reset our tables.
Every time we run the create_table.py, the script will run the drop table queries and then the create_table queries to reset our tables.

The ETL process is defined in the etl.py file. The process involves the following steps:

### Loading data to staging tables 
- ETL file first runs the load_staging_table function that runs copy_table queries imported from sql_queries.
- The first query of copy_table_queries extracts data from s3 log_data and copies it into staging_events_table.
- The second query from copy_table_queries, extracts s3 song_data and inserts into the staging_songs_table.

### Inserting data from staging tables to fact and dimensional tables.
- Then second step of etl.py file runs insert_tables function.
- The function runs the insert_table queries that inserts data from the two staging tables defined above to the facts and dimensional tables.
- The data is first inserted into songplay_table which is the fact table of our starscehma database, by joining both the staging tables above.
- Then data is inserted into user_table from staging_events_table.
- Next the data is inserted into song_table from staging_songs_table.
- artist_table takes the data from staging_songs_table.
- At last the data is inserted into time_table from staging_events_table.



# **Project Repository Files**

- ## *sql_queries.py*
This file has all the sql queries needed to define our Staging, fact and dimension tables and inserting data records into those tables.

- ## *create_tables.py*
This file helps us to reset and create our tables in the databse and it imports the drop_table and create_table queries from the sql_queries.py file.

- ## *etl.py*
This file is run to to extract data from S3 buckets and load them into staging tables, and then insert it into fact and dimensional tables.

- ## *dwh.cfg*
This is the configuration file that holds the credentials needed to connect to the Redshift cluster, along with the S3 song and log data paths.




# **_How to Run this Project?_**
#### - Open **dwh.cfg** file and update it with  
         - Redshift host endpoint
         - Database name
         - Database user name
         - Database password
         - Arn of IAM role
#### - Open **Terminal**.
#### - Run *"python create_tables.py"* to reset our tables - clearing any pre-existing data.
#### - Run *"python etl.py"* to extract and load data in our tables from S3.
#### - Open **AWS Redshift cluster** and click on query editor to run queries.





# **Some Sample Queries:** 

## 1. Finding the day of the month with most no. of sessions.

### Query: 

#### SELECT DATE_PART('DAY', start_time), 
####       COUNT(*) total_sessions 
       
#### FROM songplay_table 
#### GROUP BY 1 
#### ORDER BY 2 DESC 
#### LIMIT 1;

### Result:

date_part| total_sessions
---------|----------------
17       |   40

## 2. Finding the most recently active user: 

### Query:
#### SELECT u.first_name, 
####       u.last_name, 
####       s.start_time 

#### FROM songplay_table s
#### JOIN user_table u
#### ON 
#### u.user_id = s.user_id 
#### ORDER by 2 
#### LIMIT 1;

### Result:

first_name|   last_name | start_time 
----------|-------------|------------
Kevin     |    Arellano | 2018-11-06 07:36:46.796

