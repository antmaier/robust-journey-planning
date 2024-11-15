# %load_ext sparkmagic.magics

# %spark cleanup

# %spark add -l python -s groupAD -u http://iccluster044.iccluster.epfl.ch:8998 -k

# %spark info

# + language="spark"
# from pyspark.sql import DataFrame
# import pyspark.sql.functions as F
# from pyspark.sql.functions import col, date_format
# from pyspark.sql.types import DoubleType, FloatType
# import pyspark.sql.types as T
# from pyspark.sql.window import Window
#
# import datetime
# import graphframes as gf
#
# from math import sin, cos, sqrt, atan2, radians
# -

# ## Preprcoessing istdaten: To get the data of the actual times at which the mode of transport arrived or departured along with their scheduled times
# * Rename columns from the german names to their english form
# * Filter out rows that have empty time measurement status for both arrival and departure as they are of no use to us
# * Filter out rows that contain data about weekends
# * Take only datapoints for the year 2022 since we only want the latest data and timetables may change over different years
# * Convert the columns containing datest into datformate and time data into timestamps

# + language="spark"
# df_istdaten = spark.read.orc("/data/sbb/part_orc/istdaten/")
# df_stops_near = spark.read.orc("/user/anmaier/df_stops_near.orc").select('stop_name')
# df_network = spark.read.orc("/user/anmaier/schedule_network.orc")
#
# # specify the year in consideration
# year = "2022"
# df_istdaten_near = (df_istdaten
#                    # Remove rows where the transport did not stop
#                     .filter(F.col('DURCHFAHRT_TF') == F.lit('false'))
#                     # remove the empty measurements and statuses for arrival and departure
#                     .filter((F.col('ab_prognose_status')!='')& (F.col('an_prognose_status')!=''))
#                     .filter((F.col('ab_prognose')!='')& (F.col('an_prognose')!=''))
#                     # Rename columns to match timetable nomenclature
#                     .withColumn('date', F.to_date(col('BETRIEBSTAG'), 'dd.MM.yyyy'))
#                     # Remove the dates that are weekends
#                     .filter(F.dayofweek(col('date')).between(2, 6))
#                     # Filter for the year
#                     .filter(F.col("year")==year)
#                     .withColumnRenamed('HALTESTELLEN_NAME', 'stop_name')
#                     .withColumnRenamed('FAHRT_BEZEICHNER', 'trip_id')
#                     # Store the time string data as timestamps
#                     .withColumn('departure_time', F.to_timestamp(col('AB_PROGNOSE'), 'dd.MM.yyyy HH:mm:ss'))
#                     .withColumn('arrival_time', F.to_timestamp(col('AN_PROGNOSE'), 'dd.MM.yyyy HH:mm:ss'))
#                     .withColumn('scheduled_departure_time', F.to_timestamp(col('ABFAHRTSZEIT'), 'dd.MM.yyyy HH:mm'))
#                     .withColumn('scheduled_arrival_time', F.to_timestamp(col('ANKUNFTSZEIT'), 'dd.MM.yyyy HH:mm'))
#                     # Filter out stops that are not near zurich HB
#                     .join(df_stops_near, on='stop_name', how='inner')
#                     .select('stop_name', 'trip_id',
#                            'date',
#                            'scheduled_arrival_time', 'arrival_time', 
#                            'scheduled_departure_time', 'departure_time')).sort(F.asc('arrival_time'))
#
# df_istdaten_near.cache()
# -

# ## Calculate the mean and standar deviation values (in mins from 00hr) for the actual arrival and departure times from the pre-processed istdaten dataset

# + language="spark"
#
# # load the pre-processed data made as shown above
# df_istdaten_near = spark.read.orc("/user/amenon/df_istdaten_prob.orc")
#
# @F.udf(returnType=T.DoubleType())
# def time_to_minutes(dt):
#     # Define the reference time as 00hr
#     reference_time = datetime.time(0, 0, 0)
#     # Calculate the time difference
#     time_diff = datetime.datetime.combine(datetime.date.today(), dt.time()) - datetime.datetime.combine(datetime.date.today(), reference_time)
#     minutes_passed = time_diff.total_seconds()/60.0
#     
#     return minutes_passed
#
# # store the time stamps as minutes from 12 am 
# df_minutes = (df_istdaten_near
#               .withColumn('arrival_minutes', time_to_minutes(df_istdaten_near.arrival_time))
#               .withColumn('departure_minutes', time_to_minutes(df_istdaten_near.departure_time))
#               .withColumn('scheduled_arrival_minutes', time_to_minutes(df_istdaten_near.scheduled_arrival_time))
#               .withColumn('scheduled_departure_minutes', time_to_minutes(df_istdaten_near.scheduled_departure_time))
#               # group by on stop name, trip id, and the schedules values
#               .groupBy(['stop_name', 'trip_id','scheduled_arrival_minutes','scheduled_departure_minutes'])\
#                           .agg(F.mean('arrival_minutes').alias('mean_arrival_time'), 
#                                F.stddev('arrival_minutes').alias('std_arrival_time'),
#                                F.count('arrival_minutes').alias('arr_values_count'),
#                                F.mean('departure_minutes').alias('mean_departure_time'), 
#                                F.stddev('departure_minutes').alias('std_departure_time'),
#                                F.count('departure_minutes').alias('dep_values_count'),)\
#               # For 1 data point estimates fill std dev to be a small value 0.001 to mimic an indicator function as the distribution
#               .fillna(0.001, subset=['std_arrival_time','std_departure_time'])
#               .sort(F.asc('stop_name')))
#
# df_minutes = df_minutes.withColumn('hour', F.floor(df_minutes["scheduled_departure_minutes"] / 60))
# df_minutes.write.partitionBy('hour').orc("/user/amenon/df_minutes_partitioned.orc", mode="overwrite")
# df_minutes.cache()
# -

# ## Compute the probabilities for the walking edges present in the temporal network using the arrival and departure distributions

# + language="spark"
#
# # load the minutes and the temporal network
# df_network = spark.read.orc("/user/anmaier/schedule_network.orc")
# df_minutes = spark.read.orc("/user/amenon/df_minutes_partitioned.orc")
#
#
# # Print the average value counts that were used to compute the estimates of the arrival and departure time distributions
# print("Mean value count for the distributiion estimation = ", df_minutes.agg(F.mean('arr_values_count')).collect()[0][0])

# + language="spark"
#
# # create the dataframes containing the arrival and depature distributions in order to join it to the network
# df_minutes_arr = df_minutes.select('src_stop_name','trip_id','mean_arrival_time','std_arrival_time','arr_values_count')
# df_minutes_dep = df_minutes.select('dst_stop_name','trip_id','mean_departure_time','std_departure_time','dep_values_count')
#
# df_network_probs = (df_network
#                     # Join using stop name and trip id
#                     .join(df_minutes_arr, on=['src_stop_name','src_trip_id'], how = "inner")
#                     .join(df_minutes_dep, on=['dst_stop_name','dst_trip_id'], how = "inner"))

# + language="spark"
#
# # to print the different types of edges in the network
# df_network_probs.select('route_desc').distinct().show()

# + language="spark"
#
# # load the network with the distribution data 
# df_network_probs = spark.read.format("parquet").load("/user/amenon/df_network_probs.parquet")
# df_network_probs.cache()
# -

# ## Computing the probabilities assuming that arrival and departure time are normally distributed and that delays for different trips are independent

# + language="spark"
# import scipy.stats
# import numpy as np
#
# # Function to calculate probability
# @F.udf(returnType=T.FloatType())
# def calc_probability(duration, arr_mean, arr_std, dep_mean, dep_std):
#     
#     diff_std = np.sqrt(arr_std**2 + dep_std**2)
#     diff_mean = dep_mean - arr_mean
#     p = scipy.stats.norm.cdf(duration, loc=diff_mean, scale=diff_std)
#     
#     # Probability of successful transfer = 1 - CDF(transit duration)
#     return 1 - float(p)
#
# # load the network with the distribution parameters for the edge stops  on which the transfer is being made
# df_network_probs = spark.read.format("parquet").load("/user/amenon/df_network_probs.parquet")
#
# # Compute the probabilities for each edge using the gaussian assumption
# distribution_df = df_network_probs.withColumn('probability', 
#                                               # specify wbether walking duration or duration
#                                               calc_probability(df_network_probs.walking_duration, 
#                                                                df_network_probs.mean_arrival_time, 
#                                                                df_network_probs.std_arrival_time,
#                                                                df_network_probs.mean_departure_time,
#                                                                df_network_probs.std_departure_time ))
# # write the distibution df without partition
# distribution_df.write.mode('overwrite').parquet("/user/amenon/df_network_with_probabilities.parquet")
# distribution_df.show()
# -
# ## Cell to get statistics about the walking edges

# + language="spark"
#
# distribution_df = spark.read.format("parquet").load("/user/amenon/df_network_with_probabilities.parquet")
# walking_edges_df = distribution_df.filter(F.col('route_desc')=="walking").na.drop(subset=['probability'])
#
# # compare duration and walking duration means
# print("Mean Duration value = ", walking_edges_df.agg(F.mean(distribution_df['duration'])).collect()[0][0])
# print("Mean Walking Duration value = ", walking_edges_df.agg(F.mean(distribution_df['walking_duration'])).collect()[0][0])
#
# # jus
# print("Total rwalking edges = ",walking_edges_df.count())
# print("Total nan values for probabilities = ", walking_edges_df.filter(F.isnan(distribution_df['probability'])).count())
# print("Mean probability value = ", walking_edges_df.na.drop(subset=['probability']).agg(F.mean(distribution_df['probability'])).collect()[0][0])
# -

# ## Store the network with the probabilities partitioned by hour

# + language="spark"
# # Partition by hour
# # drop all the nan probability values 
# distribution_df = distribution_df.na.drop(subset=['probability'])
#
# store = True
# if store:
#     (distribution_df
#      .write.partitionBy('hour').parquet("/user/amenon/df_network_pp", mode="overwrite"))
# -

# !pip install pyarrow

# +
import pandas as pd
from hdfs3 import HDFileSystem

hdfs = HDFileSystem(user='ebouille')
def read_hdfs(path, fmt='parquet'):
    files = hdfs.glob(path)
    df = pd.DataFrame()
    for file in files:
        with hdfs.open(file) as f:
            if fmt == 'orc':
                if not file.endswith('.orc'):
                    continue
                df = pd.concat([df, pd.read_orc(f)])
            else:
                df = pd.concat([df, pd.read_parquet(f)])
    return df

df_stops = read_hdfs("/user/amenon/df_minutes_partitioned.orc/hour=12","orc")
df_stops.count()

# + language="spark"
# @F.udf(returnType=T.DoubleType())
# def time_to_minutes(dt):
#     # Define the reference time as 00hr
#     reference_time = datetime.time(0, 0, 0)
#     # Calculate the time difference
#     time_diff = datetime.datetime.combine(datetime.date.today(), dt.time()) - datetime.datetime.combine(datetime.date.today(), reference_time)
#     minutes_passed = time_diff.total_seconds()/60.0
#     return minutes_passed
#
# df_actual = spark.read.orc("/user/amenon/df_istdaten_prob.orc")
# df_actual.cache()
# df_mins = (df_actual
#               .withColumn('arrival_minutes', time_to_minutes(df_actual.arrival_time))
#               .withColumn('departure_minutes', time_to_minutes(df_actual.departure_time))
#               .withColumn('scheduled_arrival_minutes', time_to_minutes(df_actual.scheduled_arrival_time))
#               .withColumn('scheduled_departure_minutes', time_to_minutes(df_actual.scheduled_departure_time))
#               # fill the single sample std dev to be a small value 0.001 to indicate that value is almost sure
#               .sort(F.asc('stop_name')))
# df_mins.cache()
# -

# ## Plot histogram of the actual arrival times to visualize the distribution for a specific stop and trip id

# + language="spark"
#
# df_mins.filter(F.col('stop_name')=='Zürich, Schiffbau').distinct().select('trip_id','scheduled_arrival_minutes').show()

# + language="spark"
#
# arrival = datetime.datetime(2022, 4, 27, 22, 47,0)
# arrival_mins = 1206.0
# print(arrival_mins)
#
# stop_name = 'Zürich, Schiffbau'
# trip_id = '85:849:62329-02033-1'
#
# df = df_mins.filter((F.col('trip_id')==trip_id) & (F.col('stop_name')==stop_name) & (F.col('scheduled_arrival_minutes')==arrival_mins))
#
# df.cache()

# + magic_args="-o pandas_df" language="spark"
#
# pandas_df = df.select('arrival_minutes')
#
# pandas_df.head()

# +
import matplotlib.pyplot as plt 
# Extract the values and x_value columns as arrays
values = pandas_df['arrival_minutes'].values
x_value = pandas_df['arrival_minutes'].values[0]  # Assuming x_value is the same for all rows

# Plot the histogram
plt.hist(values, bins=10)

arrival_mins = 1206.0
# Add a vertical line for the scheduled arrival time
plt.axvline(x=arrival_mins, color='r', linestyle='--')

# Set plot labels and title
plt.xlabel('Values')
plt.ylabel('Frequency')
plt.title('Histogram of the actual arrival time')

# Show the plot
plt.show()
