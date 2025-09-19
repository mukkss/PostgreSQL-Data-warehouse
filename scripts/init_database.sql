-- =============================================================
-- Create Database and Schemas
-- =============================================================
-- Script Purpose:
--     This script creates a new database named 'datawarehouse'.
--     If the database exists, it is dropped and recreated.
--     Then it sets up three schemas: bronze, silver, and gold.
--
-- WARNING:
--     Running this will drop the entire 'datawarehouse' database if it exists.
--     All data will be permanently deleted. Proceed with caution.
-- =============================================================

-- Drop the database if it exists (disconnect users first!)
DROP DATABASE IF EXISTS datawarehouse;

-- Create the database
CREATE DATABASE datawarehouse;

-- IMPORTANT: In pgAdmin, you cannot "USE" inside a script.
-- You must reconnect to the 'datawarehouse' database in a new query tool tab.

-- After reconnecting, run this part:

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;