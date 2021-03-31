import os
import sys
import time
import logging
from pyspark.sql import SparkSession

from pyspark.sql.functions import col,trim,upper,substring, length, expr,regexp_replace

from pyspark.sql import Window


"""
Setup Logger
"""
logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s]   %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
logger.propagate = False

"""
Setup Spark
"""
logger.info('Setting Spark Session object')
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Paytm") \
    .getOrCreate()

test = spark.read.format("csv").option("header", "true").load("../data/test.csv")

logger.info(f'Show test, {test}')
#test.show(15)

#### Test Spark SQL
test.createOrReplaceTempView('test')

a = spark.sql(f"""
select * from test limit 10
""")

logger.info(f'Show test, {a}')
#a.show()

"""
Step 1 - Setting Up the Data

"""
logger.info('====================================================')
logger.info(f'1. Load the global weather data into your big data technology of choice.')
logger.info('====================================================')
weather = spark.read.option("header", "true").csv("../data/2019/*")
#weather = spark.read.option("header", "true").csv("C:\\Users\\ab6101571\\Downloads\\paytm_pyspark\\data\\2019\\*")
weather.show()
weather = weather.withColumnRenamed("STN---", "STN_NO")
weather = weather.withColumn("STN_NO", trim(weather.STN_NO))
print(weather.count())
weather.printSchema()

"""
Clean weather files
"""

logger.info('====================================================')
logger.info(f'Clean MAX, MIN - removal of * in the fields and Clean PRCP - removal of G at the end')
logger.info('====================================================')

weather = weather.\
    withColumn('MAX', regexp_replace('MAX', '\\*', '')).\
    withColumn('MIN', regexp_replace('MIN', '\\*', '')).\
    withColumn('PRCP', regexp_replace('PRCP', 'G', ''))


"""
Import other files
"""

logger.info('====================================================')
logger.info(f'Reading stationlist data')
logger.info('====================================================')

stationlist = spark.read.option("header", "true").csv("../data/stationlist.csv")
#stationlist = spark.read.option("header", "true").csv("C:\\Users\\ab6101571\\Downloads\\paytm_pyspark\\data\\stationlist.csv")
stationlist = stationlist.withColumn("STN_NO", trim(stationlist.STN_NO)).withColumn("COUNTRY_ABBR", trim(upper(stationlist.COUNTRY_ABBR)))

stationlist.show()

logger.info('====================================================')
logger.info(f'Reading countrylist data')
logger.info('====================================================')

countrylist = spark.read.option("header", "true").csv("../data/countrylist.csv")
#countrylist = spark.read.option("header", "true").csv("C:\\Users\\ab6101571\\Downloads\\paytm_pyspark\\data\\countrylist.csv")
countrylist = countrylist.withColumn("COUNTRY_ABBR", trim(upper(countrylist.COUNTRY_ABBR))).withColumn("COUNTRY_FULL", trim(upper(countrylist.COUNTRY_FULL)))
countrylist.show()

logger.info('====================================================')
logger.info(f'Reading countrylist data')
logger.info('====================================================')

logger.info('====================================================')
logger.info('================== 2. Join the stationlist.csv with the countrylist.csv to get the full country name for each station number.==================================')
logger.info('====================================================')

station_country = stationlist.join(countrylist, on="COUNTRY_ABBR", how="left")
print(stationlist.count())
print(station_country.count())
station_country.show()
station_country.where(col("COUNTRY_FULL").isNull()).show()

nulls=station_country.where(col("COUNTRY_FULL").isNull())
nulls.groupby('COUNTRY_ABBR').count()

## 8 country abbreviations dont have a FULL NAME, and hence show up as nulls in the joined table station_country, total of 97 records with NULL FULL name

logger.info('====================================================')
logger.info('================== 3. Join the global weather data with the full country names by station number.==================================')
logger.info('====================================================')

weather_station_country = weather.join(station_country, on="STN_NO", how="left")
print(weather_station_country.count())
weather_station_country.show()

weather_station_country.createOrReplaceTempView("weather_station_country")

"""
Step 2 - Questions
Using the global weather data, answer the following:
1. Which country had the hottest average mean temperature over the year?
2. Which country had the most consecutive days of tornadoes/funnel cloud
formations?
3. Which country had the second highest average mean wind speed over the year?

"""


logger.info('====================================================')
logger.info('================== STEP 2 : QUESTION 1=========\n===========1. Which country had the hottest average mean temperature over the year?==============')
logger.info('====================================================')

hottest_avg_mean_temp = spark.sql(f"""
select country_abbr, country_full, avg(cast(temp as double)) as avg_mean_temp
from weather_station_country
group by 1,2
order by 3 desc
""")

hottest_avg_mean_temp.show(1)


logger.info('====================================================')
logger.info('================== STEP 2 : QUESTION 2=======\n========2. Which country had the most consecutive days of tornadoes/funnel cloud formations?===================')
logger.info('====================================================')

tor_cloud = spark.sql(f"""

with actual_date as 
(
select country_abbr, country_full ,cast(YEARMODA as int) as YEARMODA,lag(cast(YEARMODA as int)) over(partition by country_abbr order by cast(YEARMODA as int)) as prev_day
from weather_station_country
where right(FRSHTT,1) = '1'
),
previous_date as 
(
select country_abbr, country_full ,cast(YEARMODA as int) as YEARMODA
, cast(left(translate(cast(date_sub(to_date(cast(YEARMODA as string), 'yyyyMMdd'), 1) AS string), '-', ''), 8) AS int) as prev_day
from weather_station_country
where right(FRSHTT,1) = '1'

)
select actual_date.country_abbr,actual_date.country_full,count(distinct actual_date.YEARMODA) as num_consec

from actual_date join previous_date 
on actual_date.country_abbr = previous_date.country_abbr and actual_date.YEARMODA = previous_date.prev_day
group by 1,2
order by 3 desc
""")

tor_cloud.show(1)

logger.info('====================================================')
logger.info('================== QUESTION 3 :=====\n===========1. 3. Which country had the second highest average mean wind speed over the year?==============')
logger.info('====================================================')


temp = spark.sql(f"""
select * 
,row_number() over(order by avg_mean_temp desc) as rank
from
(
select country_abbr, country_full, avg(cast(WDSP as double)) as avg_mean_temp
from weather_station_country
where cast(WDSP as double) !=999.9
group by 1,2
--order by 3 desc
)a
""")

top_2 = temp.where("rank==2")


top_2.show()