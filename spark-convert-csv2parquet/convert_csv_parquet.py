#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
 * Created by kevin on 10/21/16.
"""
import argparse
import os
import sys

from config import SPARK_HOME, SPARK_PYTHON_HOME

HELP = '''
csv_input is a CSV file, whose first line defines the column names.
parquet_output is the Parquet output (i.e., directory in which one or
more Parquet files are written.)
If you use --header, it means CSV file has header.
If you use --no-header, it means CSV file has not header.
column names:
  csv2parquet data.csv data.parquet --header/--no-header
'''.strip()

# Set the path for spark installation
# this is the path where you have built spark using sbt/sbt assembly
os.environ['SPARK_HOME'] = SPARK_HOME
# Append to PYTHONPATH so that pyspark could be found
sys.path.append(SPARK_PYTHON_HOME)

# Now we are ready to import Spark Modules
try:
    from pyspark.sql import SparkSession

except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

spark = SparkSession.builder.appName("CSV2PARQUET").getOrCreate()


def convert_csv_parquet(csv_path, out_parquet_path, header=True):
    """
    convert csv file to parquet file
    :param csv_path:
    :param out_parquet_path:
    :param header:
    :return:
    """
    if not os.path.exists(csv_path):
        raise IOError('CSV file is not exist!')

    if os.path.exists(out_parquet_path):
        raise IOError('Out Parquet has existed,'
                      ' Please remove or delete it or update output filename.')

    df = spark.read.load(csv_path, format="csv", header=header)
    df.write.save(out_parquet_path, format="parquet")


# helper functions
def get_args():
    parser = argparse.ArgumentParser(
        description='',
        epilog=HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('csv_input', help='Path to input CSV file')
    parser.add_argument('parquet_output', help='Path to Parquet output')
    parser.add_argument('--header', dest='header', action='store_true')
    parser.add_argument('--no-header', dest='header', action='store_false')
    parser.set_defaults(header=True)
    return parser.parse_args()


def test_read_converted_parquet():
    convert_csv_parquet('/home/kevin/test.csv', '/home/kevin/test.parquet')
    df = spark.read.load('/home/kevin/test.parquet')
    df.show()


def main():
    try:
        args = get_args()
        convert_csv_parquet(args.csv_input, args.parquet_output, args.header)
    except Exception, e:
        print(e.message)
    print('Successfully Convert CSV to Parquet.')


if __name__ == "__main__":
    main()
