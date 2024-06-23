
# init spark
import argparse
from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql.functions import split, col, explode, regexp_replace, from_json, row_number, to_date
from pyspark.sql.window import Window
# Init Spark Session

def init():
    """
    Initializes and returns a SparkSession object.

    Returns:
        spark (pyspark.sql.SparkSession): The initialized SparkSession object.
    """
    spark = SparkSession.builder \
    .appName("LoadJson") \
    .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") \
    .getOrCreate()

    return spark

def deduplicate(df, primary_keys):
    """
    Deduplicates records in a DataFrame based on primary keys and source_timestamp.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to deduplicate.
        primary_keys (list): A list of column names representing the primary keys.

    Returns:
        pyspark.sql.DataFrame: The deduplicated DataFrame.
    """
    df = df.withColumn(
        "_idx",
        row_number().over(
            Window.partitionBy(*primary_keys).orderBy(col("timestamp").desc())
        )
    )
    df = df.filter(df._idx == 1)
    df = df.drop("_idx")
    return df

def merge_table(df, table_name, primary_keys, partitions_list=None):
    """
    Merge the given DataFrame with a Delta table.

    Args:
        df (DataFrame): The DataFrame to merge with the Delta table.
        table_name (str): The name of the Delta table.
        primary_keys (list): A list of primary key column names.
        partitions_list (list, optional): A list of column names to partition the table by. Defaults to None.

    Returns:
        None
    """
    
    df.drop("domain", "event_type")

    # organize the fields
    df = deduplicate(df, primary_keys)

    delta_table = DeltaTable.forPath(spark, table_name)

    df_schema_fields = set(df.schema.names)
    delta_schema_fields = set(delta_table.toDF().schema.names)

    # Identificar campos novos que estão no DataFrame, mas não na tabela Delta
    new_fields = df_schema_fields - delta_schema_fields

    # Se houver campos novos, altere a tabela Delta para incluir esses campos
    if new_fields:
        for field in new_fields:
            # get definition of table
            field_definition = df.schema[field].dataType.simpleString()
            # alter to add new field
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({field} {field_definition}) SET DEFAULT null;")

    if not spark.catalog.tableExists(table_name):
        if len(partitions_list): # Partition the table
            df.write.format("delta").partitionBy(*partitions_list).saveAsTable(table_name)
        else:
            df.write.format("delta").saveAsTable(table_name)
            # by default use liquidcluster
            spark.sql(f"ALTER TABLE {table_name} CLUSTER BY (timestamp)")            
    else:
        delta_table.alias("T").merge(
            df.alias("S"),
            f"{' and '.join([f'T.{pk} = S.{pk}' for pk in primary_keys])}"
        ).whenMatchedUpdate(
            condition = "S.timestamp > T.timestamp",
            set = {col: "T." + col for col in df.columns}).whenNotMatchedInsertAll().execute()

def organize_df(spark, df):
    """
    Organizes the given DataFrame by performing the following steps:
    1. Splits the 'value' column by the pattern '\}\{' and separates the resulting values into multiple rows.
    2. Converts each row to a JSON object using the provided schema.
    
    Args:
        df (DataFrame): The input DataFrame to be organized.
        
    Returns:
        DataFrame: The organized DataFrame with the 'value' column split and converted to JSON objects.
        root
    """
    # adjusts the file content
    # split the value column by the pattern '}{' and separates the resulting values into multiple rows
    df = df.select(split(regexp_replace(
        col('value'),'\}\{','\}@|@\{'),'@|@').alias('value')
    )
    # convert each row to a JSON object using the provided schema
    df = df.withColumn("str_struct", explode(df["value"]))
    # Here we convert each row to a JSON object
    json_schema = spark.read.json(df.rdd.map(lambda row: row.json_str)).schema
    df = df.withColumn('json', from_json(col('str_struct'), json_schema))
    # organize the strutct
    df = df.withColumn("date_time", to_date(col("json.timestamp")))
    df = df.withColumn("timestamp", col("json.timestamp"))
    df = df.withColumn("domain", col("json.domain"))
    df = df.withColumn("event_type", col("json.event_type"))
    df = df.select("json.data.*", "date_time", "timestamp", "domain", "event_type")

    return df

def split_tables(df, epoch_id):
    """
    Split the data into different tables based on specific conditions.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        epoch_id (int): The epoch ID.

    Returns:
        None
    """
    # Split the data into tables
    df = organize_df(spark, df)

    df_account = df.filter((col("json.domain") == "account" & col("json.event_type") == "creation"))
    df_transaction = df.filter((col("json.domain") == "transaction" & col("json.event_type") == "creation"))
    df_account_status_change = df.filter((col("json.domain") == "account" & col("json.event_type") == "status-change"))

    if not df_account.isEmpty():
        merge_table(df_account, "account", ["id"], ["date_time"])

    if not df_transaction.isEmpty():
        merge_table(df_transaction, "transaction", ["id"], ["date_time"])

    if not df_account_status_change.isEmpty():
        merge_table(df_account_status_change, "account_status_change", ["id"], ["date_time"])

def execute_autoloader(spark, split_tables, load_directory, schema_path, checkpoint_location):
    """
    Executes the autoloader process using Spark Streaming.

    Args:
        spark (pyspark.sql.SparkSession): The Spark session.
        split_tables (function): A function that splits the data into tables.
        load_directory (str): The directory containing the data files.
        schema_path (str): The path to the schema file.
        checkpoint_location (str): The location to save the checkpoint.

    Returns:
        None
    """
    df_stream = (spark
        .readStream   # a streaming dataframe
        .format("cloudFiles")  # tells Spark to use AutoLoader
        .option("cloudFiles.format", "text")  # the actual format of out data files
        .option("cloudFiles.schemaLocation", schema_path) # location of the schema file will saved
        .load(load_directory) # location of the actual data file
        .writeStream
        .format("delta")    # Write into delta lake
        .option("checkpointLocation", checkpoint_location) # location of the checkpoint will saved
        .trigger(once=True) # only process the files once
        .foreachBatch(split_tables) # apply the split_tables function
        .start()
    )

    df_stream.awaitTermination()

if __name__ == "__main__":
    spark = init()
    
    # using argparse to get the arguments
    parser = argparse.ArgumentParser()
    # all the parameters are required
    # All is GSC or S3 Bucket path.
    parser.add_argument("--load_directory", help="The directory where the data files are stored", required=True)
    parser.add_argument("--schema_path", help="The path to the schema file", required=True)
    parser.add_argument("--checkpoint_location", help="The path to the checkpoint location", required=True)
    args = parser.parse_args()
    
    load_directory = args.load_directory
    schema_path = args.schema_path
    checkpoint_location = args.checkpoint_location
    
    execute_autoloader(spark, split_tables, load_directory, schema_path, checkpoint_location)