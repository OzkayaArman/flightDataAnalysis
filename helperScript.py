#Run pip install pyspark in the terminal
#Run python helperScript.py
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os



# Source directory of csv files within teaching labs
source_dirs = [
    #"/cs/datasets/CS5052/P1/",
    #"/cs/datasets/CS5052/P1/SupplementaryCSVs"
    "/Users/armanozkaya/Desktop/P1",
    "/Users/armanozkaya/Desktop/P1/SupplementaryCSVs"
]

#Local directory path to work with
#target_dir = "/home/ao85/Documents/FourthYear/DataIntensiveSystem"
target_dir = "/Users/armanozkaya/Desktop/Parquet"

#GAP Reference: https://medium.com/@shuklaprashant9264/convert-a-csv-file-to-a-parquet-58381b588557 for function below
def convert_csv_to_parquet(source_directories, target_directory):
    # Configure Spark settings
    conf = SparkConf() \
        .setAppName("CSV to Parquet") \
        .setMaster("local[*]") \
        .set("spark.driver.memory", "15g") \
        .set("spark.executor.memory", "15g")

   # Create a Spark session
    spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()



    for source_directory in source_directories:
        # Lists all CSVs in the source directory
        for file_name in os.listdir(source_directory):
            if file_name.endswith(".csv"):
                # Define the path to the current CSV file
                csv_file_path = os.path.join(source_directory, file_name)

                #GAP notice: Chatgpt was used to generate relative path, target_subdir, and parquet_file_path
                relative_path = os.path.relpath(source_directory, start=source_directories[0])
                target_subdir = os.path.join(target_directory, relative_path)

                # Define the target path for the Parquet file
                parquet_file_path = os.path.join(target_subdir, file_name.replace(".csv", ".parquet"))

                # Read the CSV file into a DataFrame
                df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

                # Write the DataFrame out as a Parquet file
                df.write.parquet(parquet_file_path)

    spark.stop()

convert_csv_to_parquet(source_dirs, target_dir)

print("Conversion completed")
