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

### Pipeline

The project use Databricks and GCP for example

When merge in branch master, the pipelie consists in execute job.py
job.py is a script responsable ti create or update a job in Databricks workspace.