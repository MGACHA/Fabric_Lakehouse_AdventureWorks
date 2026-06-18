# Welcome to your new notebook
# Type here in the cell editor to add code!
import os, time, datetime 
from pyspark.sql.functions import monotonically_increasing_id, lit, current_timestamp, when, col, regexp_replace, array, concat_ws
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from notebookutils import mssparkutils


# Specify the folder containing CSV files
csv_folder = "Files/csv/csv"

# Get a list of all files in the folder
files = notebookutils.fs.ls(csv_folder)

# Filter for CSV files
csv_files = [file.path for file in files if file.name.endswith(".csv")] # to test only one file, simply write the name before .csv

# Define the schema for the log DataFrame so it has have good column names
log_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("row_amount_file", IntegerType(), True),
    StructField("row_amount_table", IntegerType(), True),
    StructField("row_amount_diff", IntegerType(), True),
    StructField("load_timestamp_utc_bronze", TimestampType(), True)
])

# Define the schema for the malformed records DataFrame so it has good column names
malformed_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("record_number", IntegerType(), True),
    StructField("entire_record", StringType(), True),
    StructField("explanation", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Initialize an empty DataFrame for log entries with the defined schema
df_log = spark.createDataFrame([], schema=log_schema)
df_mal = spark.createDataFrame([], schema=malformed_schema)
df_mal_log = spark.createDataFrame([], schema=malformed_schema)

# Initialize a counter for warnings
warning_count = 0

# count files
csv_files_count = [file for file in files if file.name.endswith('.csv')]
csv_count = len(csv_files_count)
file_processed_count = 0

# Process each CSV file
for csv_file in csv_files:
    file_processed_count += 1
    table_name = os.path.splitext(os.path.basename(csv_file))[0] # Extract table name from the file name (remove ".csv" extension)
    table_name = table_name.replace(".","_") # remove "." for database schema and replace it with "_"

    print(f"{table_name}.csv is being processed. Remaining files to process: {csv_count - file_processed_count}") 

    # Get the number of lines in the CSV file
    lines = spark.read.text(csv_file)
    line_count = lines.count() - 1 # -1 to exclude the header
    print(f"{table_name} has this amount of lines: {line_count:,}".replace(",", "'"))

    # Read the CSV file with PERMISSIVE mode to get all records
    df_all = spark.read.options(
        delimiter=",", 
        header=True,
        encoding="UTF-8", 
        inferSchema=True, 
        multiLine=True,
        escapeQuotes=True, 
        mode="PERMISSIVE"
    ).csv(csv_file)

    # Read the CSV file with DROPMALFORMED mode to get only valid records
    df_well = spark.read.options(
        delimiter=",", 
        header=True,
        encoding="UTF-8", 
        inferSchema=True, 
        multiLine=True,
        escapeQuotes=True, 
        mode="DROPMALFORMED"
    ).csv(csv_file)

    # Identify malformed records by diff it
    df_mal = df_all.subtract(df_well)

    # Add a record number to the malformed records dataFrame
    df_mal = df_mal.withColumn("record_number", monotonically_increasing_id())
    columns = ['record_number'] + [col for col in df_mal.columns if col != 'record_number']
    df_mal = df_mal.select(columns)

    # Show the malformed records with their record numbers
    print(f"The amount of read well formed lines: {df_well.count():,}".replace(",","'"))
    if df_mal.count() == 0:
        print(f"\033[92mThe amount of read mal formed lines:  {df_mal.count():,}\033[0m".replace(",","'"))
    else:    
        print(f"\033[91mThe amount of read mal formed lines:  {df_mal.count():,}\033[0m".replace(",","'")) 
        # display(df_mal)

    # Add id column and put it into the front 
    df_well = df_well.withColumn("id", monotonically_increasing_id())  
    columns_ordered = ["id"] + [col for col in df_well.columns if col != "id"]
    df_well = df_well.select(*columns_ordered)

    # Add load_timestamp
    df_well = df_well.withColumn('load_timestamp_utc_bronze', current_timestamp())

    # Write to Delta table
    df_well.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

    # Check the number of rows in the Delta table
    delta_table = spark.table(table_name)
    delta_count = delta_table.count()
    print(f"{table_name} has this amount of lines in the delta table: {delta_count:,}".replace(",", "'"))

    # Create a dataframe for the current log entry
    df_current_log = spark.createDataFrame([(
        table_name,
        line_count,
        delta_count,
        line_count - delta_count,
        datetime.datetime.now()  # Use current timestamp
    )], schema=log_schema)

    # Append the current log entry to the log DataFrame
    df_log = df_log.union(df_current_log)

    # Append the current malformed dataframe to a log dataframe
    # display(df_mal)
    # df_mal_log = df_mal_log.union(df_mal)

    # Prepare the malformed records for logging
    df_mal_log_entry = df_mal.withColumn("file_name", lit(table_name)) \
                             .withColumn("entire_record", concat_ws("|||", *df_mal.columns)) \
                             .withColumn("explanation", lit("malformed")) \
                             .withColumn("timestamp", current_timestamp()) \
                             .withColumn("record_number", monotonically_increasing_id())

    # Select the columns in the desired order
    df_mal_log_entry = df_mal_log_entry.select("file_name", "record_number", "entire_record", "explanation", "timestamp")

    # Append the current malformed records to the log DataFrame
    df_mal_log = df_mal_log.union(df_mal_log_entry)

    # Compare the counts and print messages
    if line_count == delta_count:
        print(f"\033[92mSUCCESS: {table_name}.csv loaded into Delta table {table_name} with matching line counts\033[0m")
        print(f"\n")
    else:
        print(f"\033[91mERROR: {table_name}.csv line count {line_count:,}".replace(",", "'") + f" does not match Delta table line count {delta_count:,}".replace(",", "'") + f" \033[0m")
        print(f"\n")
        warning_count += 1

# Write log tables to the currently attached Lakehouse
if not df_log.filter(df_log["row_amount_diff"] > 0).rdd.isEmpty():
    df_log.write.format("delta").mode("append").saveAsTable("load_log_bronze")
    print(f"\n\033[93m ALL CSV files LOADED with WARNING into load_log_bronze table.\033[0m")
else:
    print(f"\n\033[92m ALL CSV files SUCCESSFULLY LOADED into load_log_bronze table.\033[0m")

# Write malformed records to the currently attached Lakehouse
if not df_mal_log.rdd.isEmpty():
    df_mal_log.write.format("delta").mode("append").saveAsTable("bad_records")
    print(f"\n\033[93m Malformed records have been written to the bad_records table.\033[0m")

# Print the number of tables that gave warnings
if warning_count == 0:
    print(f"\n\033[92m Number of tables with warnings: NONE - SUCCESS \033[0m")
else:
    print(f"\n\033[91m Number of tables with warnings: {warning_count} → GO CHECK LOG! \033[0m")