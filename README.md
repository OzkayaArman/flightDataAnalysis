Running Instructions:  
1-Please download the dataset from DOT's website https://www.transtats.bts.gov/tables.asp?QO_VQ=EFD&QO_anzr=Nv4yv0r
2- In helperScript.py script, please put the path of airline.csv, and SupplementaryCSVs as a comma seperated value into source_dirs instead of the paths currently present  
3-Please create a folder named Parquet  
4-Please input the Parquet file’s path into target_dir instead of the path currently present within “”  
4- Run helperScript.py with python helperScript.py command in the terminal   
5- Navigate to InFlight.py script  
6- Instead of the path currently present in parquet_file_path, input the path for airline.parquet.   
7- Instead of the path currently present in parquet_airport_metadata_path, input path for L_AIRPORT_ID.parquet  
8- Intead of the path currently present in parquet_airline_metadata_path, input path for  
L_UNIQUE_CARRIERS.parquet  
9-Run inflight.py  
