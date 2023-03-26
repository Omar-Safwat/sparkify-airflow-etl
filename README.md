# Project motivation
A music streaming company, Sparkify, has decided to introduce automation and monitoring to their data warehouse ETL pipelines airflow.


They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.


The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

<img alt="Airflow graph" src="https://video.udacity-data.com/topher/2019/January/5c48a861_example-dag/example-dag.png"/>

# Data
Data used in this project, can be downloaded from the following s3 buckets:

```
s3://udacity-dend/log_data
s3://udacity-dend/song_data
```

# Project structure
* **dags**: ETL dag.
* **plugings**: 
  * **helpers**: Classes for sql queries.
  * **operators**: Custom airflow operators.

# Building the operators
The project contains four custom operators that will stages, transforms, and run data quality checks on the data.


### Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

### Fact and Dimension Operators
Runs SQL queries to load the fact and dimension tables.

The operator should have a parameter that allows switching between insert modes when loading dimensions.

### Data Quality Operator
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.
