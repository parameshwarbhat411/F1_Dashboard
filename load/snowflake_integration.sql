
CREATE STORAGE INTEGRATION my_f1_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = ''
    STORAGE_ALLOWED_LOCATIONS = ('s3://race-predictor-pro/')
    COMMENT = 'Integration for Snowflake to read data from S3 bucket for F1 analytics'
;

DESC INTEGRATION my_f1_integration;

USE SCHEMA F1_DASHBOARD.PUBLIC;

CREATE OR REPLACE STAGE my_f1_stage
  URL = 's3://race-predictor-pro/f1_data/'
  STORAGE_INTEGRATION = my_f1_integration
  FILE_FORMAT = (TYPE = PARQUET)
  COMMENT = 'Stage for F1 data in S3 using Snowflake integration';

LIST @my_f1_stage;

// Testing
CREATE OR REPLACE TABLE test_laps AS
SELECT * FROM @my_f1_stage LIMIT 10;

