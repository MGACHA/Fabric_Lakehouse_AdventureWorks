# loaded tables
spark.sql("SHOW TABLES").show(truncate=False)
# database
spark.sql("SHOW DATABASES").show(truncate=False)
# check bad records table
df = spark.sql("SELECT * FROM LH_AdventureWorks.bad_records LIMIT 1000")
display(df)
# check log bronze table
df1 = spark.sql("SELECT * FROM LH_AdventureWorks.load_log_bronze WHERE row_amount_diff >0 LIMIT 1000")
display(df1)
print(f"Incorrect rows : {df1.count()}")
df2 = spark.sql("SELECT * FROM LH_AdventureWorks.load_log_bronze WHERE row_amount_diff = 0 LIMIT 1000")
display(df2)
print(f"Correct rows: {df2.count()}")