Welcome to Convert CSV to Parquet Using Spark
===========================
 
This is a program which is used to convert csv file to  parquet file based on spark.

Usage
---------------------

```
python convert_csv_parquet.py csv_input_path parquet_out_path --header
```

**csv_input_path** is a CSV file, whose first line defines the column names. **parquet_out_path** is the Parquet output (i.e., directory in which one or more Parquet files are written.) Note that csv2parquet is currently specifically designed to work with CSV files whose first line defines header/column names.

Installation
------------------------

Your system must have:

- Python
- Spark

There is no other dependencies.

When you after installing spark, you should update the **SPARK_HOME** item in config.py where it has been installed.
