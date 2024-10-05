from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, monotonically_increasing_id, when
import re
import itertools
import math
import warnings
import matplotlib.pyplot as plt
import plotly.express as px
import statistics
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from collections import Counter


warnings.filterwarnings("ignore")

from pyspark.sql.functions import max,min


# Initialize a SparkSession
spark = SparkSession.builder.appName("Read Parquet File").getOrCreate()

# Path to your Parquet file
parquet_file_path = '/Users/armanozkaya/Desktop/Parquet/airline.parquet'
parquet_airport_metadata_path = '/Users/armanozkaya/Desktop/Parquet/SupplementaryCSVs/L_AIRPORT_ID.parquet'
parquet_airline_metadata_path = '/Users/armanozkaya/Desktop/Parquet/SupplementaryCSVs/L_UNIQUE_CARRIERS.parquet'
# Read the Parquet file
df = spark.read.parquet(parquet_file_path)
airport_info_df = spark.read.parquet(parquet_airport_metadata_path)
airline_info_df = spark.read.parquet(parquet_airline_metadata_path)

# Get the column names
column_names = df.columns

def main():
    flag = True
    flag2 = 0
    while flag:
        if flag2 == 0:
            print("->Welcome to InFlight! A program that allows you query statistics about the air industry")
            flag2 = 1
        else:
            print("->Main Menu!")
        print("* In order to display the total number of flights that took place in a year of your choosing, press 1")
        print("* In order to display the percentage of flights that took off on time, "
              "took off early or took off late in a year of your choosing, press 2")
        print("* In order to display the top reason for cancelled flights in a user specified year, press 3.")
        print("* In order to display the most punctual airports in the years 1987, 1997, 2007 and 2017, press 4")
        print("* In order to display the top three worst performing airline in the 20th century, press 5")
        print("* In order to compare the performance of airports in any number of US states "
              "across the entire dataset, press 6")
        print("* In order to compare the performance of airports accross US states, press 7")
        print("* In order to exit the program, press 0")
        key_input = input()
        key_input = int(key_input)
        if key_input == 0:
            break
        elif key_input == 1:
            return_value = filter_by_years(df)
            if return_value == 0:
                break
        elif key_input == 2:
            return_value = distribution_of_flight_status(df)
            if return_value == 0:
                break
        elif key_input == 3:
            return_value = top_reason_for_flight_cancellation(df)
            if return_value == 0:
                break
        elif key_input == 4:
            return_value = top3_airports(df, airport_info_df)
            if return_value == 0:
                break
        elif key_input == 5:
            return_value = worst_performing_airline(df, airline_info_df)
            if return_value == 0:
                break
        elif key_input == 6:
            return_value = compare_airports(df,airport_info_df)
            if return_value == 0:
                break
        elif key_input == 7:
            return_value = compare_states(df,airport_info_df)
            if return_value == 0:
                break
        else:
            print("Wrong Input")

#Allow the user to search the dataset by year
def filter_by_years(df):

    range_pattern = r'^\s*\d{4}\s*-\s*\d{4}\s*$'
    list_pattern = r'^\d{4}(,\s*\d{4})*$'
    all_years = None
    flag = False

    while flag == False:

        # Get user input for years
        years_input = input(
            "Please enter the years you would like to query for the toal number of flights that took place either by"
            " providing a range of years separated by a dash (1990-2000) or "
            "years listed as a comma seperated value (1990,1991,1993): ").strip()

        # Determine if input is a range or a list
        if re.match(range_pattern, years_input):
            years_input = years_input.replace("-", " ")

            # Finding all numbers in the string
            years_input = re.findall(r'\d+', years_input)

            # Converting numbers to integers
            start_end_years = [int(years) for years in years_input]
            start, end = sorted(start_end_years)
            #Get the list of years specified in the range
            all_years = list(range(start, end + 1))

            flag = True
            print(all_years)

        elif re.match(list_pattern, years_input):
            years_input = years_input.replace(",", " ")

            # Finding all numbers in the string
            years_input = re.findall(r'\d+', years_input)

            # Converting numbers to integers
            all_years = [int(years) for years in years_input]
            flag = True

            print(all_years)

        else:
            print("Wrong input format")

    filtered_df = df.filter(col("Year").isin(all_years))
    result = filtered_df.groupBy("Year").count().orderBy("Year")
    result.show()

    while True:
        key_input = input("In order to return to main menu, press 1, in order to exit the program press 0: ")
        key_input = int(key_input)
        if (key_input == 1) | (key_input == 0):
            return key_input
        else:
            print("Invalid input")

# For any user specified year, display to the user in a neatly formatted fashion, the percentage of flights that (1) took off on time, (2) took off early or (3) took off late
def distribution_of_flight_status(df):
    year_pattern = r'^\s*(\d{4})\s*$'
    flag = False

    while flag == False:
        # Get user input for years
        year_input = input(
            "Please enter the year you would like to query for the distribution of flights"
            "based on flights' status of being on time, early or late ").strip()
        if re.match(year_pattern, year_input):
            flag = True
            year = int(year_input)
            filtered_df_year = df.filter(col("Year") == year)

            #In order to find out the flight's status, we can look at the value of DepDelay column
            overallFlights = filtered_df_year.count()

            early_flight_numbers = filtered_df_year.filter(col("DepDelay") < 0).count()

            on_time_flight_numbers = filtered_df_year.filter(col("DepDelay") == 0).count()

            late_flight_numbers = filtered_df_year.filter(col("DepDelay") > 0).count()

            #Percentage Calculation
            percentage_early_flights = (early_flight_numbers/ overallFlights) * 100
            percentage_on_time = (on_time_flight_numbers / overallFlights) * 100
            percentage_late_flights = (late_flight_numbers / overallFlights) * 100

            print("The percentage of early flights: ", percentage_early_flights)
            print("The percentage of on time flights: ", percentage_on_time)
            print("The percentage of late flights: ", percentage_late_flights)

        else:
            print("Wrong input format")

    while True:
        key_input = input("In order to return to main menu, press 1, in order to exit the program press 0: ")
        key_input = int(key_input)
        if (key_input == 1) | (key_input == 0):
            return key_input
        else:
            print("Invalid input")

#For any user specified year, display the top reason for cancelled flights that year
def top_reason_for_flight_cancellation(df):
# The interpretation of codes was taken from this DOT official site https: // www7.bts.dot.gov / topics / airlines - and -airports / understanding - reporting - causes - flight - delays - and -cancellations
    #Regular expression recognizes a 4 digit year format
    year_pattern = r'^\s*(\d{4})\s*$'
    flag = False

    while flag == False:
        # Get user input for years
        year_input = input(
            "Please enter the year you would like to query for the primary flight cancellation reason").strip()

        if re.match(year_pattern, year_input):
            flag = True
            year = int(year_input)

            #Only select the data filtered by the year that the user inputted
            filtered_df_year = df.filter(col("Year") == year)
            cancelled_df = filtered_df_year.filter(col("Cancelled") == 1)

            #Calculate the number of cancelled flights
            counting = cancelled_df.count()

            # If there were no flights cancelled, return this
            if counting <= 0:
                print("No flights were cancelled this year")
                break
            filtered_df_cancelled = cancelled_df.filter(col("CancellationCode").isNotNull())

            # Calculate the number of rows that recorded the cancellation code
            counting = filtered_df_cancelled.count()

            #If there was no record of a cancellation code, return this
            if counting <= 0:
                print("There were flights cancelled, however cancellation reasons were not specified for this year")
                break

            #Otherwise, if there was cancellation reason specified, calculate the mode and return this
            #GAP Reference for top_reason calculation:https: // www.statology.org / pyspark - mode - of - column /  #:~:text=Note%3A%20In%20both%20examples%2C%20we,count%20to%20get%20the%20mode.
            top_reason = filtered_df_cancelled.groupBy("CancellationCode").count().orderBy('count', ascending=False).first()
            top_reason = top_reason['CancellationCode']
            if top_reason == 'A':
                print("This year the cause of the cancellation was due to circumstances within the airline's control (e.g. maintenance or crew problem)")
            elif top_reason == 'B':
                print("This year the cause of the cancellation was due to significant meteorological conditions (actual or forecasted) that, in the judgment of the carrier, delays or prevents the operation of a flight")
            elif top_reason == 'C':
                print("This year the cause of the cancellation was due to national aviation system that refer to a broad set of conditions, such as non-extreme weather conditions, airport operations...")
            elif top_reason == 'D':
                print("This year the cause of the cancellation was due to security related issues")
            else:
                print(top_reason)

        else:
            print("Wrong input format")

    while True:
        key_input = input("In order to return to main menu, press 1, in order to exit the program press 0: ")
        key_input = int(key_input)
        if (key_input == 1) | (key_input == 0):
            return key_input
        else:
            print("Invalid input")

#In 1987, 1997, 2007 and 2017, display the top 3 airports (and where were they) that recorded the most punctual take-offs?

def top3_airports(df, df_airport_metadata):
    #Every dataframe has the specific year and all flights that departed on time
    df_1987 = df.filter(col("Year") == 1987)
    df_1997 = df.filter(col("Year") == 1997)
    df_2007 = df.filter(col("Year") == 2007)
    df_2017 = df.filter(col("Year") == 2017)

    dataframes = [df_1987, df_1997, df_2007, df_2017]

    airport_ids = []
    airport_names = []
    airport_city_names = []

    #For every year specified in the dataframe list
    for dataframe in dataframes:
        print("*** In the year ", dataframe.select("Year").first()["Year"], " top 3 most punctual airports and their location was as follows: ")
        #Find the flights that have departed on time
        dataframe = dataframe.filter(col("DepDelay") == 0)
        #Group such on time flights by the airport id, count them and return the top 3
        df_grouped = dataframe.groupby("OriginAirportID", 'OriginCityName').count().orderBy('count', ascending=False).limit(3)

        #GAP reference: https: // www.geeksforgeeks.org / how - to - iterate - over - rows - and -columns - in -pyspark - dataframe /
        # Add all the airports that had a flight that year to a list
        for row in df_grouped.select("OriginAirportID").collect():
            airport_ids.append(row)

        #Add all airports city name into a list
        for row in df_grouped.select("OriginCityName").collect():
            airport_city_names.append(row)

        #Get the corresponding name for the airport id from the metadata
        for id in airport_ids:
            df_airport = df_airport_metadata.filter(col("Code") == id['OriginAirportID'])
            description = df_airport.select("Description").first()
            airport_names.append(description)


        #GAP Reference: https://stackoverflow.com/questions/9394803/python-combine-two-for-loops
        #GAP Reference: https://www.geeksforgeeks.org/python-itertools-zip_longest/?ref=lbp
        for row1, row2 in itertools.zip_longest(airport_names, airport_city_names):
            description = row1.Description
            city_name = row2.OriginCityName
            print("Airport Name Is: ", description, " |Airport is located in: ", city_name)

        #Prepare the lists for the next iteration
        airport_ids.clear()
        airport_names.clear()
        airport_city_names.clear()

    while True:
        key_input = input("In order to return to main menu, press 1, in order to exit the program press 0: ")
        key_input = int(key_input)
        if (key_input == 1) | (key_input == 0):
            return key_input
        else:
            print("Invalid input")
'''
->To judge the performance, we will use a point based system, the airline that has the most points at the 
end of the analysis will be the worst performing airline

->Delayed Flights  1 point for every 15 minute delay interval (for instance a flight delayed 15 minutes 
will get 1 point, a flight delayed 32 minutes will get 2 points)

->Cancelled Flights = 20 points

!The implementation relies on all lists defined having the same structure (reporting airlines) as keys, this is a concious implementation

'''

#Examining all the years in the 20th century, display the top three worst performing airlines.
def worst_performing_airline(df, df_airline_metadata):
    #These lists will hold dictionaries
    list_flights_of_airlines = []
    list_number_of_delayed_flights_of_airlines = []
    list_number_of_cancelled_flights_of_airlines = []
    final_list=[]

    #Filter for all flights in the 20th century
    all_years = list(range(1987, 2000))
    filtered_df = df.filter(col("Year").isin(all_years))

    #Query for number of flights of each airline in the 20th century, will be used for data normalization
    flight_numbers = filtered_df.select("Reporting_Airline", "Flight_Number_Reporting_Airline")
    all_flights = flight_numbers.groupby("Reporting_Airline").count()

    for row in all_flights.collect():
        data_all_flights = {"Reporting_Airline": row["Reporting_Airline"], "Count": row["count"]}
        list_flights_of_airlines.append(data_all_flights)


    df_filtered_cancelled = filtered_df.filter((col("Cancelled") == 1))
    cancelled_flights = df_filtered_cancelled.select("Reporting_Airline", "Cancelled")
    cancelled_flights = cancelled_flights.groupBy("Reporting_Airline").count()

    #Add the grouped data into the dictionary after multiplying the value by 20  (one cancellation is worth 20 points)
    for row in cancelled_flights.collect():
        data_cancelled_flights = {"Reporting_Airline": row["Reporting_Airline"], "Count": row["count"] * 20}
        list_number_of_cancelled_flights_of_airlines.append(data_cancelled_flights)


    #Query for delayed flights of each airline
    df_filtered_delayed = filtered_df.filter(col("DepDelay") > 0)
    delayed_flights = df_filtered_delayed.select("Reporting_Airline", "DepDelay")
    delayed_flights = delayed_flights.groupBy("Reporting_Airline").count()


    for row in delayed_flights.collect():
        data_delayed_flights = {"Reporting_Airline": row["Reporting_Airline"], "Count": math.floor(row["count"] / 15) + 1}
        list_number_of_delayed_flights_of_airlines.append(data_delayed_flights)


    for value1, value2, value3 in zip(list_number_of_delayed_flights_of_airlines, list_number_of_cancelled_flights_of_airlines,list_flights_of_airlines):
        key = value1['Reporting_Airline']
        #Add the points for delayed and cancelled flights, then divide by number of flights in order to normalize the data
        new_value = (value1["Count"] + value2["Count"]) / value3["Count"]
        data = {"Reporting_Airline": key, "Point": new_value}
        final_list.append(data)


    #GAP Reference for line 379, 382 https://www.freecodecamp.org/news/sort-dictionary-by-value-in-python/#:~:text=it%20as%20well!-,How%20to%20Sort%20the%20Dictionary%20by%20Value%20in%20Ascending%20or,sorted%20dictionary%20in%20descending%20order.&text=You%20can%20see%20the%20output,to%20the%20sorted()%20method.
    # Sort the list by 'score' in descending order
    sorted_list = sorted(final_list, key=lambda x: x['Point'], reverse=True)

    # Select the top 3 dictionaries
    top_3 = sorted_list[:3]

    print("Worst 3 performing airlines in the 20th century when ratio of delayed or cancelled flights over overall number of flights is computed are:")
    counter = 1
    for row in top_3:
        df_airline = df_airline_metadata.filter(col("Code") == row['Reporting_Airline'])
        description = df_airline.select("Description").first()
        if counter == 1:
            print("Worst Airline:")
        if counter == 2:
            print("Second Worst Airline:")
        if counter == 3:
            print("Third Worst Airline:")

        print(description['Description'])
        counter += 1

    while True:
        key_input = input("In order to return to main menu, press 1, in order to exit the program press 0: ")
        key_input = int(key_input)
        if (key_input == 1) | (key_input == 0):
            return key_input
        else:
            print("Invalid input")

'''
Allow the user to compare the performance of airports in any number of US states
across the entire dataset. Justify how you will compare and present the data and what
metric(s) you select to measure performance.

    We will compare the airports based on their
    1- On Time Performance (departure delay)
    2- Capacity
    3- Reliability(Cancelled Or Not)
'''
def compare_airports(df, df_airport_metadata):
    #Get state names from user and clean any trailing and leading white space
    state_input = input(
        "Please enter the state names of states (seperated by coma) you would like to compare the performance of airports belonging"
        "to each state against each other: ").strip()
    #Clean any whitespace that may exist when states are split by ","
    states = [state.strip() for state in state_input.split(',')]

    filtered_df = df.filter(col("OriginStateName").isin(states))

    #Create an ordering of airports based on the departure delays (lower delay is better)
    delayed_df = filtered_df.groupBy("OriginAirportID").agg(avg("DepDelay").alias("AverageDepDelay"))\
        .orderBy("AverageDepDelay", ascending=True).withColumnRenamed("OriginAirportID", "AirportID")
    delayed_df = delayed_df.withColumn("OrderDelay", monotonically_increasing_id())

    #Create an ordering of airports based on the cancellations (lower cancellation number is better)
    cancelled_df = filtered_df.groupBy("OriginAirportID").agg(avg("Cancelled").alias("CancelledFlights"))\
        .orderBy("CancelledFlights", ascending=True).withColumnRenamed("OriginAirportID", "AirportID")
    cancelled_df = cancelled_df.withColumn("OrderCancelled", monotonically_increasing_id())

    #Create an ordering of airports based on the capacity (Higher capacity is better)
    non_cancelled_df = filtered_df.filter(col("Cancelled") == 0)
    originated_df = non_cancelled_df.groupBy("OriginAirportID").count().withColumnRenamed("OriginAirportID", "AirportID")
    destination_df = non_cancelled_df.groupBy("DestAirportID").count().withColumnRenamed("DestAirportID", "AirportID")

    originated_df = originated_df.withColumnRenamed("count", "DepartureCount")
    destination_df = destination_df.withColumnRenamed("count", "ArrivalCount")

    #Drop joins where there isn't an id present in both dataframes (highly unlikely as that implies there was an airport)
    #That did departures but no arrivals or vice versa
    capacity_df = originated_df.join(destination_df, on="AirportID", how="inner")

    capacity_df = capacity_df.withColumn("TotalFlights", col("DepartureCount") + col("ArrivalCount"))
    capacity_df = capacity_df.drop("DepartureCount", "ArrivalCount")
    capacity_df = capacity_df.orderBy('TotalFlights', ascending=False)

    capacity_df = capacity_df.withColumn("OrderCapacity", monotonically_increasing_id())

    #Join the lists together to achieve a final result
    interim_df = delayed_df.join(cancelled_df, on="AirportID", how="inner")
    final_df = interim_df.join(capacity_df,on="AirportID", how="inner")
    final_df = final_df.withColumn("Performance Index", col("OrderDelay") + col("OrderCancelled")+col("OrderCapacity") + (((((
                                                                                                                      col("OrderDelay") - (
                                                                                                                          (
                                                                                                                                      col("OrderDelay") + col(
                                                                                                                                  "OrderCancelled") + col(
                                                                                                                                  "OrderCapacity")) / 3)) ** 2) + (
                                                                                                                     (
                                                                                                                                 col("OrderCancelled") - (
                                                                                                                                     (
                                                                                                                                                 col("OrderDelay") + col(
                                                                                                                                             "OrderCancelled") + col(
                                                                                                                                             "OrderCapacity")) / 3)) ** 2) + (
                                                                                                                     (
                                                                                                                                 col("OrderCapacity") - (
                                                                                                                                     (
                                                                                                                                                 col("OrderDelay") + col(
                                                                                                                                             "OrderCancelled") + col(
                                                                                                                                             "OrderCapacity")) / 3)) ** 2)) / 3) ** 0.5))

    final_df = final_df.withColumnRenamed("AirportID", "code")
    final_df = final_df.join(df_airport_metadata, on="code", how="inner")
    final_df = final_df.drop("code")
    final_df = final_df.orderBy("Performance Index", ascending=True)
    data_collected = final_df.select("Description", "Performance Index").collect()
    descriptions = []
    performance_indexes = []

    for row in data_collected:
        descriptions.append(row['Description'])

    for row in data_collected:
        performance_indexes.append(row['Performance Index'])

    #Gap Reference for bar chart implementation: https://plotly.com/python/bar-charts/
    fig = px.bar(x=descriptions, y=performance_indexes, labels={'x': 'Airport Names', 'y': 'Inefficiency Index'})
    fig.update_layout(title_text='Inefficiency Index (Lower Is Better)',
                      xaxis={'categoryorder': 'total ascending'})
    fig.show()

    while True:
        key_input = input("In order to return to main menu, press 1, in order to exit the program press 0: ")
        key_input = int(key_input)
        if (key_input == 1) | (key_input == 0):
            return key_input
        else:
            print("Invalid input")

#Chart/explore the performance of the various states’ airports that are located within the continental United States
def compare_states(df, df_airport_metadata):
    #GAP Notice for below max-min: https://www.statology.org/pyspark-max-of-column/
    #Below variables are used for data normalization
    max_DepDelay = df.agg(max("DepDelay")).collect()[0][0]
    min_DepDelay = df.agg(min("DepDelay")).collect()[0][0]
    max_Cancelled = 1
    min_Cancelled = 0
    capacity_count = df.groupBy("OriginStateFips", "Year").count()
    max_Capacity= capacity_count.agg(max("count")).collect()[0][0]
    min_Capacity = capacity_count.agg(min("count")).collect()[0][0]


    # List of state names of continental USA
    state_names = [
        'Alabama', 'Arizona', 'Arkansas', 'California', 'Colorado',
        'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Idaho',
        'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
        'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
        'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
        'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
        'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
        'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
        'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
        'West Virginia', 'Wisconsin', 'Wyoming'
    ]

    # List of fips codes for every region
    Northeast = [9,23,25,33,44,50,34,36,42]
    Midwest = [18,17,26,39,55,19,20,27,29,31,38,46]
    South = [10,11,12,13,24,37,45,51,54,1,21,28,47,5,22,40,48]
    West = [4,8,16,35,30,49,32,56,6,41,53]

    #Filters out records where state name isn't recorded
    filtered_df = df.filter(col("OriginStateName").isin(state_names))

    #Maps fips codes to region names
    fips = filtered_df.withColumn('OriginStateFips', when(filtered_df['OriginStateFips'].isin(Northeast), 'Northeast').otherwise(filtered_df['OriginStateFips']))

    fips = fips.withColumn('OriginStateFips',when(fips['OriginStateFips'].isin(Midwest), 'Midwest').otherwise(fips['OriginStateFips']))

    fips = fips.withColumn('OriginStateFips',when(fips['OriginStateFips'].isin(South), 'South').otherwise(fips['OriginStateFips']))

    fips = fips.withColumn('OriginStateFips',when(fips['OriginStateFips'].isin(West), 'West').otherwise(fips['OriginStateFips']))

    #**************** Region Choropleth Creation
    # Creating a data frame that holds information about the average delay time of every airport per travel
    delayed_df = fips.groupBy("OriginStateFips").agg(avg("DepDelay").alias("AverageDepDelay")) \
        .orderBy("AverageDepDelay", ascending=True)

    delayed_df = delayed_df.withColumn("OrderDelay", monotonically_increasing_id())

    # Creating a data frame that holds information about a flight's cancellation chance per travel for every airport
    cancelled_df = fips.groupBy("OriginStateFips").agg(avg("Cancelled").alias("CancelledFlights")) \
        .orderBy("CancelledFlights", ascending=True)
    cancelled_df = cancelled_df.withColumn("OrderCancelled", monotonically_increasing_id())

    # Creating a data frame that calculates the capacity of every airport by using departing and arriving flights' counts
    # Cancelled flights are excluded from capacity calculation
    non_cancelled_df = fips.filter(col("Cancelled") == 0)

    capacity_df = non_cancelled_df.groupBy("OriginStateFips").count().orderBy("count", ascending=False)
    capacity_df = capacity_df.withColumn("OrderCapacity", monotonically_increasing_id())

    #Joining data frames of different metrics to obtain final data frame with inefficiency scores
    interim_df = delayed_df.join(cancelled_df, on="OriginStateFips", how="inner")
    final_df = interim_df.join(capacity_df, on="OriginStateFips", how="inner")

    #Calculating the inefficiency index with standard deviation calculated manually
    final_df = final_df.withColumn("Inefficiency Index",
                                   col("OrderDelay") + col("OrderCancelled") + col("OrderCapacity") + (((((
                                                                                                                      col("OrderDelay") - (
                                                                                                                          (
                                                                                                                                      col("OrderDelay") + col(
                                                                                                                                  "OrderCancelled") + col(
                                                                                                                                  "OrderCapacity")) / 3)) ** 2) + (
                                                                                                                     (
                                                                                                                                 col("OrderCancelled") - (
                                                                                                                                     (
                                                                                                                                                 col("OrderDelay") + col(
                                                                                                                                             "OrderCancelled") + col(
                                                                                                                                             "OrderCapacity")) / 3)) ** 2) + (
                                                                                                                     (
                                                                                                                                 col("OrderCapacity") - (
                                                                                                                                     (
                                                                                                                                                 col("OrderDelay") + col(
                                                                                                                                             "OrderCancelled") + col(
                                                                                                                                             "OrderCapacity")) / 3)) ** 2)) / 3) ** 0.5))

    final_df = final_df.orderBy("Inefficiency Index", ascending=True)
    data_collected = final_df.select("OriginStateFips", "Inefficiency Index").collect()
    descriptions = []
    performance_indexes = []

    #State Name Abbrevations In The Form Choropleth understands
    west_region_states = ['WA', 'OR', 'CA', 'ID', 'NV', 'UT', 'AZ', 'MT', 'WY', 'CO', 'NM']
    south_region_states = ['AL', 'AR', 'DE', 'FL', 'GA', 'KY', 'LA', 'MD', 'MS', 'NC', 'OK', 'SC', 'TN', 'TX', 'VA',
                           'WV']
    midwest_region_states = ['IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI']

    northeast_region_states = ['CT', 'MA', 'ME', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT']

    descriptions2 = []
    performance_indexes2 = []

    #Put region names into a list
    for row in data_collected:
        descriptions.append(row['OriginStateFips'])

    #Put data for all 4 regions into a list
    for row in data_collected:
        performance_indexes.append(row['Inefficiency Index'])

    #For every region, map region names back to state abbrevations and add them to another list to be used for data plotting again
    for row in descriptions:
        if row == 'West':
            for x in west_region_states:
                descriptions2.append(x)
                performance_indexes2.append(performance_indexes[0])
        if row == 'South':
            for x in south_region_states:
                descriptions2.append(x)
                performance_indexes2.append(performance_indexes[1])
        if row == 'Midwest':
            for x in midwest_region_states:
                descriptions2.append(x)
                performance_indexes2.append(performance_indexes[2])
        if row == 'Northeast':
            for x in northeast_region_states:
                descriptions2.append(x)
                performance_indexes2.append(performance_indexes[3])

    #Choropleth GAP Reference: https://plotly.com/python/choropleth-maps/
    fig = px.choropleth(
        locations=descriptions2,
        locationmode='USA-states',
        color=performance_indexes2,
        scope="usa",
        color_continuous_scale="temps",
        labels={'Value': 'Inefficiency Index'}
    )

    fig.update_layout(
        title_text='Comparison Of Different States\' Inefficiency Index',coloraxis_colorbar=dict(title='Inefficiency Index')
    )


    fig.show()

    #**************** State Choropleth Creation

    # Creating a data frame that holds information about the average delay time of every airport per travel
    delayed_df = filtered_df.groupBy("OriginState").agg(avg("DepDelay").alias("AverageDepDelay")) \
        .orderBy("AverageDepDelay", ascending=True)

    delayed_df = delayed_df.withColumn("OrderDelay", monotonically_increasing_id())

    # Creating a data frame that holds information about a flight's cancellation chance per travel for every airport
    cancelled_df = filtered_df.groupBy("OriginState").agg(avg("Cancelled").alias("CancelledFlights")) \
        .orderBy("CancelledFlights", ascending=True)
    cancelled_df = cancelled_df.withColumn("OrderCancelled", monotonically_increasing_id())

    # Creating a data frame that calculates the capacity of every airport by using departing and arriving flights' counts
    # Cancelled flights are excluded from capacity calculation
    non_cancelled_df = filtered_df.filter(col("Cancelled") == 0)

    capacity_df = non_cancelled_df.groupBy("OriginState").count().orderBy("count", ascending=False)
    capacity_df = capacity_df.withColumn("OrderCapacity", monotonically_increasing_id())

    interim_df = delayed_df.join(cancelled_df, on="OriginState", how="inner")
    final_df = interim_df.join(capacity_df, on="OriginState", how="inner")

    #Calculating the inefficiency index with standard deviation calculated manually
    final_df = final_df.withColumn("Inefficiency Index",
                                   col("OrderDelay") + col("OrderCancelled") + col("OrderCapacity") + (((((
                                                                                                                      col("OrderDelay") - (
                                                                                                                          (
                                                                                                                                      col("OrderDelay") + col(
                                                                                                                                  "OrderCancelled") + col(
                                                                                                                                  "OrderCapacity")) / 3)) ** 2) + (
                                                                                                                     (
                                                                                                                                 col("OrderCancelled") - (
                                                                                                                                     (
                                                                                                                                                 col("OrderDelay") + col(
                                                                                                                                             "OrderCancelled") + col(
                                                                                                                                             "OrderCapacity")) / 3)) ** 2) + (
                                                                                                                     (
                                                                                                                                 col("OrderCapacity") - (
                                                                                                                                     (
                                                                                                                                                 col("OrderDelay") + col(
                                                                                                                                             "OrderCancelled") + col(
                                                                                                                                             "OrderCapacity")) / 3)) ** 2)) / 3) ** 0.5))

    final_df = final_df.orderBy("Inefficiency Index", ascending=True)
    data_collected = final_df.select("OriginState", "Inefficiency Index").collect()
    descriptions = []
    performance_indexes = []

    for row in data_collected:
        descriptions.append(row['OriginState'])

    for row in data_collected:
        performance_indexes.append(row['Inefficiency Index'])

    #Choropleth GAP Reference: https://plotly.com/python/choropleth-maps/
    fig = px.choropleth(
        locations=descriptions,
        locationmode='USA-states',
        color=performance_indexes,
        scope="usa",
        color_continuous_scale="temps",
        labels={'Value': 'Inefficiency Index'}
    )

    fig.update_layout(
        title_text='Comparison Of Different States\' Inefficiency Index',coloraxis_colorbar=dict(title='Inefficiency Index')
    )

    fig.show()

    #**************** Line Chart Creation For Different Regions' Departure Delay Over Time

    delayed_fips = fips.groupBy("OriginStateFips","Year").agg(avg("DepDelay").alias("AverageDepDelay")).orderBy("Year", "AverageDepDelay", ascending=True)

    delayed_fips = delayed_fips.withColumn("NormalizedDepDelay",(F.col("AverageDepDelay") - min_DepDelay) / (max_DepDelay - min_DepDelay) )

    #GAP Notice: the 2 lines of code below that uses Window was taken from chat gpt to get know how of ordering that resets
    windowSpec = Window.partitionBy("Year").orderBy("AverageDepDelay")
    delayed_fips = delayed_fips.withColumn("OrderDelay", F.row_number().over(windowSpec))

    data_collected = delayed_fips.select("OriginStateFips","Year","AverageDepDelay").collect()
    west_descriptions = []
    west_depDelay = []
    south_descriptions = []
    south_depDelay = []
    midwest_descriptions = []
    midwest_depDelay = []
    Northeast_descriptions = []
    Northeast_depDelay = []


    for row in data_collected:
        if str(row['OriginStateFips']) == "West":
            west_depDelay.append(row['AverageDepDelay'])
            west_descriptions.append(row['Year'])
        elif str(row['OriginStateFips']) == "South":
            south_depDelay.append(row['AverageDepDelay'])
            south_descriptions.append(row['Year'])
        elif str(row['OriginStateFips']) == "Midwest":
            midwest_depDelay.append(row['AverageDepDelay'])
            midwest_descriptions.append(row['Year'])
        elif str(row['OriginStateFips']) == "Northeast":
            Northeast_depDelay.append(row['AverageDepDelay'])
            Northeast_descriptions.append(row['Year'])

    # Plotting West
    fig1 = px.scatter(x=west_descriptions, y=west_depDelay, title = "Average Departure Delay Over Time For Region: West",labels={'x': 'Region West Over Time', 'y': 'AverageDepDelay'})
    fig1.update_traces(mode='lines+markers')
    average_depDelay = np.mean(west_depDelay)
    fig1.add_hline(y=average_depDelay, line_dash="solid", line_color="red", annotation_text="Average",
                   annotation_position="bottom right")
    fig1.show()

    # Plotting South
    fig2 = px.scatter(x=south_descriptions, y=south_depDelay, title = "Average Departure Delay Over Time For Region: South",labels={'x': 'Region South Over Time', 'y': 'AverageDepDelay'})
    fig2.update_traces(mode='lines+markers')
    average_depDelay = np.mean(south_depDelay)
    fig2.add_hline(y=average_depDelay, line_dash="solid", line_color="red", annotation_text="Average",
                   annotation_position="bottom right")
    fig2.show()

    # Plotting Midwest
    fig3 = px.scatter(x=midwest_descriptions, y=midwest_depDelay, title = "Average Departure Delay Over Time For Region: Midwest",labels={'x': 'Region Midwest Over Time', 'y': 'AverageDepDelay'})
    fig3.update_traces(mode='lines+markers')
    average_depDelay = np.mean(midwest_depDelay)
    fig3.add_hline(y=average_depDelay, line_dash="solid", line_color="red", annotation_text="Average",
                   annotation_position="bottom right")
    fig3.show()

    # Plotting Northeast
    fig4 = px.scatter(x=Northeast_descriptions, y=Northeast_depDelay, title = "Average Departure Delay Over Time For Region: NorthEast",labels={'x': 'Region Northeast Over Time', 'y': 'AverageDepDelay'})
    fig4.update_traces(mode='lines+markers')
    average_depDelay = np.mean(Northeast_depDelay)
    fig4.add_hline(y=average_depDelay, line_dash="solid", line_color="red", annotation_text="Average",
                   annotation_position="bottom right")
    fig4.show()


    #**************** Calculating Cancelled Flight (Reliability) Metric for Pie Chart Later On
    cancelled_fips = fips.groupBy("OriginStateFips", "Year").agg(
        avg("Cancelled").alias("CancelledFlights")).orderBy("Year", "CancelledFlights", ascending=True)
    cancelled_fips = cancelled_fips.withColumn("NormalizedCancelledFlights",(F.col("CancelledFlights") - min_Cancelled) / (max_Cancelled - min_Cancelled) )
    #cancelled_fips.show()

    # GAP Notice: the 2 lines of code below that uses Windowwas taken from chat gpt to get "know how" of ordering that resets based on year
    windowSpec = Window.partitionBy("Year").orderBy("CancelledFlights")
    cancelled_fips = cancelled_fips.withColumn("OrderCancelled", F.row_number().over(windowSpec))

    # **************** Line Chart Creation For Different Regions' Capacity Over Time
    non_cancelled_fips = fips.filter(col("Cancelled") == 0)
    non_cancelled_fips = non_cancelled_fips.groupBy("OriginStateFips", "Year").count().orderBy("count", "year", ascending=False)
    non_cancelled_fips = non_cancelled_fips.withColumn("NormalizedCapacity",1- ((F.col("count") - min_Capacity) / (max_Capacity - min_Capacity)) )

    # GAP Notice: the 2 lines of code below that uses Windowwas taken from chat gpt to get "know how" of ordering that resets based on year
    windowSpec = Window.partitionBy("Year").orderBy(F.col("count").desc())
    capacity_fips = non_cancelled_fips.withColumn("OrderCapacity", F.row_number().over(windowSpec))

    data_collected2 = capacity_fips.select("OriginStateFips","Year","count").collect()
    west_desc = []
    west_cap = []
    south_desc = []
    south_cap = []
    midwest_desc = []
    midwest_cap = []
    Northeast_desc = []
    Northeast_cap = []

    for row in data_collected2:
        if str(row['OriginStateFips']) == "West":
            west_cap.append(row['count'])
            west_desc.append(row['Year'])
        elif str(row['OriginStateFips']) == "South":
            south_cap.append(row['count'])
            south_desc.append(row['Year'])
        elif str(row['OriginStateFips']) == "Midwest":
            midwest_cap.append(row['count'])
            midwest_desc.append(row['Year'])
        elif str(row['OriginStateFips']) == "Northeast":
            Northeast_cap.append(row['count'])
            Northeast_desc.append(row['Year'])

    # Plotting West
    fig1 = px.scatter(x=west_desc, y=west_cap, title = "Average Capacity Over Time For Region: West",labels={'x': 'Region West Over Time', 'y': 'Capacity'})
    fig1.update_traces(mode='lines+markers')
    average_cap = np.mean(west_cap)
    fig1.add_hline(y=average_cap, line_dash="solid", line_color="red", annotation_text="Average",
                   annotation_position="bottom right")
    fig1.show()

    # Plotting South
    fig2 = px.scatter(x=south_desc, y=south_cap, title = "Average Capacity Over Time For Region: South",labels={'x': 'Region South Over Time', 'y': 'Capacity'})
    fig2.update_traces(mode='lines+markers')
    average_cap = np.mean(south_cap)
    fig2.add_hline(y=average_cap, line_dash="solid", line_color="red", annotation_text="Average",
                   annotation_position="bottom right")
    fig2.show()

    # Plotting Midwest
    fig3 = px.scatter(x=midwest_desc, y=midwest_cap, title = "Average Capacity Over Time For Region: Midwest",labels={'x': 'Region Midwest Over Time', 'y': 'Capacity'})
    fig3.update_traces(mode='lines+markers')
    average_cap = np.mean(midwest_cap)
    fig3.add_hline(y=average_cap, line_dash="solid", line_color="red", annotation_text="Average",
                   annotation_position="bottom right")
    fig3.show()

    # Plotting Northeast
    fig4 = px.scatter(x=Northeast_desc, y=Northeast_cap, title = "Average Capacity Over Time For Region: Northeast",labels={'x': 'Region Northeast Over Time', 'y': 'Capacity'})
    fig4.update_traces(mode='lines+markers')
    average_cap = np.mean(Northeast_cap)
    fig4.add_hline(y=average_cap, line_dash="solid", line_color="red", annotation_text="Average",
                   annotation_position="bottom right")
    fig4.show()


    interim_fips = delayed_fips.join(cancelled_fips, on=["OriginStateFips", "Year"], how="inner")
    final_fips = interim_fips.join(capacity_fips, on=["OriginStateFips", "Year"], how="inner")
    final_fips = final_fips.withColumn("Inefficiency Index", col("NormalizedDepDelay") + col("NormalizedCancelledFlights") + col("NormalizedCapacity") +
                                   col("OrderDelay") + col("OrderCancelled") + col("OrderCapacity") + (((((
                                                                                                                      col("OrderDelay") - (
                                                                                                                          (
                                                                                                                                      col("OrderDelay") + col(
                                                                                                                                  "OrderCancelled") + col(
                                                                                                                                  "OrderCapacity")) / 3)) ** 2) + (
                                                                                                                     (
                                                                                                                                 col("OrderCancelled") - (
                                                                                                                                     (
                                                                                                                                                 col("OrderDelay") + col(
                                                                                                                                             "OrderCancelled") + col(
                                                                                                                                             "OrderCapacity")) / 3)) ** 2) + (
                                                                                                                     (
                                                                                                                                 col("OrderCapacity") - (
                                                                                                                                     (
                                                                                                                                                 col("OrderDelay") + col(
                                                                                                                                             "OrderCancelled") + col(
                                                                                                                                             "OrderCapacity")) / 3)) ** 2)) / 3) ** 0.5))

    final_fips = final_fips.orderBy("Year","Inefficiency Index", ascending=True)
    # GAP Notice: the 2 lines of code below that uses Windowwas taken from chat gpt to get "know how" of ordering that resets based on year
    windowSpec = Window.partitionBy("Year").orderBy(F.col("Inefficiency Index"))
    final_fips = final_fips.withColumn("OrderOverall", F.row_number().over(windowSpec))

    data_collected = final_fips.select("OriginStateFips", "OrderOverall").collect()
    best = []
    worst = []

    year = 1
    for row in data_collected:
        if row["OrderOverall"] == 1:
            best.append(str(row['OriginStateFips']))
        elif row ["OrderOverall"] == 4:
            worst.append(str(row['OriginStateFips']))

    best_counts = Counter(best)
    worst_counts = Counter(worst)

    # Pie chart for best regions over time
    fig_best = px.pie(names=list(best_counts.keys()), values=list(best_counts.values()),
                      title='Best Regions Distribution')
    fig_best.update_traces(textinfo='percent+label')
    fig_best.show()

    # Pie chart for worst regions over time
    fig_worst = px.pie(names=list(worst_counts.keys()), values=list(worst_counts.values()),
                       title='Worst Regions Distribution')
    fig_worst.update_traces(textinfo='percent+label')
    fig_worst.show()

    while True:
        key_input = input("In order to return to main menu, press 1, in order to exit the program press 0: ")
        key_input = int(key_input)
        if (key_input == 1) | (key_input == 0):
            return key_input
        else:
            print("Invalid input")

main()
spark.stop()