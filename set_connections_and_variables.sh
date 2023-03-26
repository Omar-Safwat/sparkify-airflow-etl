#!/bin/bash

# AWS credentials
AWS_CREDENTIALS_URI=<URI>
airflow connections add aws_credentials --conn-uri $AWS_CREDENTIALS_URI

# Redshift connection
airflow connections add redshift --conn-uri <redshift-uri>

# S3 bucket
airflow variables set s3_bucket final-module-ud
airflow variables set s3_prefix data-pipelines