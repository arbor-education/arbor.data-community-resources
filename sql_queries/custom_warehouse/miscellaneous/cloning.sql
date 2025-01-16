-- This file contains SQL examples for Cloning Objects in Snowflake
-- Cloning allows for creating copies of objects e.g. Databases, Tables
-- This can be useful for creating non-production databases or tables to test against
-- Or for creating an instant back-up

-- Create a Clone of a Database
CREATE DATABASE DEMO_LOAD_DATA_DEV CLONE DEMO_LOAD_DATA;

-- Create a clone of a Table
CREATE TABLE DEMO_LOAD_DATA_DEV.RAW.USERS_BACKUP CLONE DEMO_LOAD_DATA_DEV.RAW.USERS;

-- More information can be found here: https://docs.snowflake.com/en/sql-reference/sql/create-clone
