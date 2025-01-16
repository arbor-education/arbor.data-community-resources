-- This file contains SQL examples for Undropping Objects in Snowflake
-- Undrop allows for certain objects in Snowflake to be restored, as long as they were dropped within the Time Travel Data retention period
-- This retention period is 24 hours by default

-- Undrop a database
UNDROP DATABASE DEMO_DB;

-- Undrop a schema
UNDROP SCHEMA DEMO_DB.RAW;

-- Undrop a Table
UNDROP TABLE DEMO_DB.RAW.USERS;

-- Further information can be found here: https://docs.snowflake.com/en/sql-reference/sql/undrop
