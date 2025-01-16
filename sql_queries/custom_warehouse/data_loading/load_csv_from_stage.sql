-- Use database and schema if not already set

-- Set parameters to change table or stage name to be more appropriate for your use case
set table_name = 'STAFF';

-- Create Stage if it does not already exist
create stage if not exists CSV_LOAD_STAGE;

-- Load example file into Stage
-- This can be done using the Snowflake CLI locally or Snowflake UI

-- Create a CSV file format that will parse the CSV header row, for more configuration options see: https://docs.snowflake.com/en/sql-reference/sql/create-file-format
CREATE or replace FILE FORMAT csv_file_format
  TYPE = csv PARSE_HEADER = true error_on_column_count_mismatch=false;

-- Review inferred schema, see https://docs.snowflake.com/en/sql-reference/functions/infer_schema for more information
SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=> '@CSV_LOAD_STAGE'
      , FILE_FORMAT=> 'csv_file_format'
      , IGNORE_CASE=> true
      )
    );

-- Create table from CSV file inferred schema
CREATE TABLE IF NOT EXISTS identifier($table_name)
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@CSV_LOAD_STAGE'
          , FILE_FORMAT=>'csv_file_format'
          , IGNORE_CASE=> true
        )
      ));
      
-- Add metadata columns
ALTER TABLE identifier($table_name)
ADD COLUMN 
    WAREHOUSE_LOAD_TIMESTAMP TIMESTAMP_NTZ,
    SOURCE_FILE_NAME VARCHAR;

-- Load data from Stage into Table, including useful metadata
COPY INTO identifier($table_name) FROM @LOAD_TEST_STAGE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = csv_file_format
INCLUDE_METADATA = (
    WAREHOUSE_LOAD_TIMESTAMP = METADATA$START_SCAN_TIME, SOURCE_FILE_NAME = METADATA$FILENAME);
    
-- Review loaded data
select * from identifier($table_name) limit 100;


-- Create Task to load latest files in stage on a daily basis - 0800 GMT
CREATE TASK LOAD_CSV_FILE_DAILY
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 8 * * * Europe/London'
  AS
COPY INTO identifier($table_name) FROM @LOAD_TEST_STAGE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = csv_file_format
INCLUDE_METADATA = (
    WAREHOUSE_LOAD_TIMESTAMP = METADATA$START_SCAN_TIME, SOURCE_FILE_NAME = METADATA$FILENAME);


alter task LOAD_CSV_FILE_DAILY resume;





