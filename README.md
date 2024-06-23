## Project
This project consists of an example of how to handle JSON and an implementation flow using Databricks as the tool.

The main factors of any project are:

CI/CD - in this case used to update the job on Databricks.
Testing - Validating transformation or calculation functions is very important.
Environment - what the cloud environment needs to run the project.
Documentation - The more comments and information, the better, right?

# Main.py Overview

This section of the `main.py` file focuses on merging data into specific tables and executing an autoloader process using Spark Streaming.

## Merging Data into Tables

The code snippet begins with merging data into tables:

- `account` 
- `transaction`
- `account_status_change`

- Similarly, if `df_account_status_change` is not empty, it merges the data into the "account_status_change" table, also using "id" as the key and "date_time" as the additional column.

## Autoloader Execution

The `execute_autoloader` function is defined to execute the autoloader process using Spark Streaming. Here's a breakdown of its parameters and functionality:

### Parameters:

- `load_directory`: The directory containing the data files.
- `schema_path`: The path to the schema file.
- `checkpoint_location`: The location to save the checkpoint.
