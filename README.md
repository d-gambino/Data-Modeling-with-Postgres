# Project Summary

Sparkify, a music streaming company, wants to analyze the data they’ve been collecting on songs and user activity logs. They want the database schema to be optimal for running queries on song play analysis.  

This project aims to bring this to fruition by storing the data in a relational database (Postgres), using Python to do the ETL. 

 

# Database Structure


Given that the database is to be optimized for song play analysis, data is organized in a star schema with the fact table describing song plays. Dimension tables include time, users, songs, and artists. 

 

# The files: 

#### sql_queries.py 

Contains SQL queries that create tables and insert data. Also includes a query that searches for artist and song IDs for our fact table. 

#### create_tables.py 

Connects to the database, or creates it if it does not yet exist. Executes the create table queries in `sql_queries.py`. 

#### etl.py 

Processes Sparkify’s raw data (JSON) using a variet of Python libraries and stores it in our Postgres database tables.  


#### etl.ipynb:  

Sandbox notebook. Identical to `etl.py` but only processes a single song and log file. 

#### test.ipynb:  

Contains a variety of queries that tests whether the tables were created,  and data has been inserted. Also contains sanity test queries to verify primary keys exist in all tables, column constraints are appropriate, and datatypes are appropriate. 

 

 

# How to run the scripts: 

#### Creating (or recreating) the empty tables

```python create_tables.py``` 

 

#### Executing the ETL 

```python etl.py``` 

