import math
from pyspark.sql.functions import udf, array
from pyspark.sql.types import *
from pyspark.sql import functions as F
from math import pi
import random
import folium
from pyspark.sql import SparkSession

# File location and type
data_file_location = "/tmp/data/DataSample.csv"
poi_file_location = "/tmp/data/POIList.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","


def load_csv(file_location, options=None):
  """
  Function to load and get a dataframe from a CSV file
  
  Parameters
  ----------
  file_location : location of input CSv file
  options : load options
  
  Returns
  -------
  A dataframe with the columns imported from csv file : dataframe
  """
  df = spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .option("header", first_row_is_header) \
            .option("sep", delimiter) \
            .load(file_location)
  return df


def cleanup_data(df):
  """
  We consider records that have identical geoinfo and timest as suspicious. 
  A function to clean up the sample dataset by filtering out those suspicious request records.
  
  Parameters
  ----------
  df : Input dataframe
  
  Returns
  -------
  A dataframe with the suspicious records removed : dataframe
  """
  df_dummy = df.groupBy([' TimeSt', 'Latitude', 'Longitude']) \
               .count().where("count = 1").drop('count')

  df_cleaned = df.join(df_dummy, [' TimeSt', 'Latitude', 'Longitude'], "inner") \
                 .select(data_df.columns)
  
  return df_cleaned


def distance(origin, destination):
    """
    Calculate the Haversine distance.

    Parameters
    ----------
    origin : tuple of float
        (lat, long)
    destination : tuple of float
        (lat, long)

    Returns
    -------
    distance_in_km : float

    Examples
    --------
    >>> origin = (48.1372, 11.5756)  # Munich
    >>> destination = (52.5186, 13.4083)  # Berlin
    >>> round(distance(origin, destination), 1)
    504.2
    """
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371  # km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return round(d,2)


def getrows(df, rownums=None):
  """
  Function to get rows from a dataframe
  
  Parameters
  ----------
  df : Input dataframe
  rownums : A list of rows to extract
  
  Returns
  -------
  A dataframe with the selected rows : dataframe
  """
  return df.rdd.zipWithIndex().filter(lambda x: x[1] in rownums).map(lambda x: x[0])  


def add_poi(df, poi_df):
  """
  Function to add distance of each record from POI location
  
  Parameters
  ----------
  df : Input dataframe
  poi_df : A dataframe of POI location
  
  Returns
  -------
  A dataframe with added columns refelecting the distance from each POI : dataframe
  """
  for i in range(poi_df.count()):
    destination = getrows(poi_df, rownums=[i]).collect()[0]
    poi = destination[0]
    poi_geo = destination[1:]
    poi_udf = udf(lambda x: distance(x, poi_geo), FloatType())
    df = df.withColumn(poi, poi_udf(array("Latitude", "Longitude")))
  return df  


def find_min_index(arr):
  """
  Function to find the index of minimum value in an array.
  In case more than one index has the minimum value, the index is randomly assinged to one of them.
  
  Parameters
  ----------
  arr : Input array of numbers reflecting the distance of each record from the POIs.
  
  Returns
  -------
  Index of the closest POI  : string
  """
  mymin = min(arr)
  min_positions = [i for i, x in enumerate(arr) if x == mymin]
  
  if len(min_positions) != 1:
    poi_index = random.choice(min_positions)
  else:
    poi_index = min_positions[0]
    
  return 'POI' + str(poi_index + 1)


def poi_aggregate(df, poi_df):
  """
  Function to assign each record to the closest POI,
  Also to aggregate the distance of each record from POI location based on POIID and calculate some metrics.
  Mean and Standard deviation (Std) of distances of all assigned records,
  Radius of the circle should reflect the distance between each POI and its furthest encompassed request,
  Maximum distance is used for this metric (Radius).
  Density (i.e. request count/circle area) for each POI location is also calculated.
  Count the number of records assigned to each POI.
  
  Parameters
  ----------
  df : Input dataframe incdluing the columns reflecting the distance of each record from the POIs.
  poi_df : A dataframe of POI location
  
  Returns
  -------
  A dataframe with added columns refelecting the mean and std and maximum of distances from each POI, count, density : dataframe
  """
  min_cols_in = udf(lambda arr: find_min_index(arr), StringType())
  min_cols = udf(lambda arr: min(arr), FloatType())
  density_cols = udf(lambda arr: float(arr[0]) / (pi * float(arr[1])**2), FloatType()) 
  
  df_min = df.withColumn('POIID', min_cols_in(array(df.columns[7:]))) \
             .withColumn('Min_DisPOI', min_cols(array(df.columns[7:])))

  df_metric = df_min.groupBy('POIID').agg(F.round(F.mean('Min_DisPOI'),2).alias('Mean'),
                                          F.round(F.stddev('Min_DisPOI'),2).alias('Std'),
                                          F.round(F.max('Min_DisPOI'),2).alias('Max'),
                                          F.count('Min_DisPOI').alias('Count')
                                         ) \
                                     .withColumn('Density', density_cols(array('Count','Max')))

  df_agg = poi_df.join(df_metric, "POIID", "left") \
                 .orderBy("POIID") \
                 .na.fill(0)
  return df_agg


def count_normalize(count, max, count_mean):
  """
  A function to normalize the count number into (-10,10) and consider extreme cases.
  As the density is directly proportional to count of assigned records, here we use it for normalizing.
  Take consideration about extreme case/outliers, i.e., 
  POIs without a records assigned to them (count = 0) or all records assinged to one POI (count = max).
  Tanh(x) function is chosen as it is a symmetric monotonic fucntion in range of (-1,1) with sesitivity around its mean (zero).

  
  Parameters
  ----------
  count : number of assigned records to a POI 
  max : Total number of records
  mean : Average number of records count assigned to POIs.
  """
  if count == 0:
    result = -10
  elif count == max:
    result = 10
  else:
    result = round(10*math.tanh((count - count_mean)/count_mean),1)
  return result  


def density_map(data_df, df):
  """
  A function to map the density of POIs into a scale from -10 to 10.
  Take consideration about extreme case/outliers, i.e., 
  POIs without a records assigned to them or all records assinged to one POI. 
  The mapping aim to be more sensitive around mean average to give maximum visual differentiability.
    records_count : total number of records after cleaning
    records_mean : the average of number of records assigned to all POIs. (rounded up)
  
  Parameters
  ----------
  data_df : Input dataframe including the all the records to get the total count.
  df : A dataframe of POI location and aggregated metrics.
  
  """
  records_count = data_df.count()
  records_mean = math.ceil(df_agg.agg({"Count": "avg"}).collect()[0][0])
  map_density = udf(lambda x: count_normalize(x, records_count, records_mean), FloatType())
  df_final = df.withColumn('Popularity_Map', map_density('Count'))
  
  return df_final 


def draw_circle(df):
  """
  Function to draw a circle for each record of POI locations
  
  Parameters
  ----------
  df : Input dataframe having the mean, std and count of records assigned to each POI
       
       location=[45.69, -78.04] is coordinates of Ontario, CA used for centerizing the map
  
  Returns
  -------
  Draw circle on the map based on geo location of each POI and number of assigned records : html
  """
  POI_map = folium.Map(location=[45.69, -78.04], zoom_start=4, tiles="Mapbox Bright")
  for i in range(df.count()):
    record = getrows(df, rownums=[i]).collect()[0]
    poi = record[0]
    poi_geo = record[1:3]
    poi_max = record[-4]
    count = record[-3]
    popularity = record[-1]
    folium.CircleMarker(location=poi_geo, 
                        radius=poi_max/100, 
                        fill=True, 
                        weight=1,
                        popup=str(poi) + "; popularity: " + str(round(popularity,1))
                       ).add_to(POI_map)
  POI_map.save('./POI_map.html')
  
  return 0
  
if __name__ == "__main__":
  """
      Usage: poi.py
  """
  spark = SparkSession\
      .builder\
      .appName("PythonPOIanalysis")\
      .getOrCreate()

  # Load the sample data and show them
  data_df = load_csv(data_file_location)
  poi_df = load_csv(poi_file_location)

  # Cleanup the sample data
  df_cleaned = cleanup_data(data_df)

  # Label the records, in case the minimum distance to POIs are the same for more than one of
  # the POIs, randomly one of them is selected. This could be treated as a load balancer when
  # more that one POI is assgined to a location.   
  df_with_pois = add_poi(df_cleaned, poi_df)

  # Analysis of the data to aggregate and calculate some metrics
  df_agg = poi_aggregate(df_with_pois, poi_df)

  # Mathematical modeling for mapping the density values (tanh)
  df_final = density_map(df_cleaned, df_agg)

  # Draw the map with circles and embedded info, an html file will be created on the current folder
  # clicking on the circles would pop up the info associated with that circle.
  # If two circles are centered as the same spot, only the last one would be shown (this scenario needs more work to improve.)
  draw_circle(df_final)
  
  # show the results of each step
  data_df.orderBy(data_df[' TimeSt']).show(truncate=False)
  poi_df.show(truncate=False)
  df_cleaned.orderBy(df_cleaned[' TimeSt']).show(truncate=False)
  df_with_pois.orderBy(df_with_pois[' TimeSt']).show(truncate=False)
  df_final.orderBy(df_final['POIID']).show(truncate=False)
  
  spark.stop()
  
