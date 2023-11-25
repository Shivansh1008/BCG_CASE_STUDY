import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

if __name__ == '__main__':
    #Creating spark session
    spark = SparkSession.builder.master("spark://localhost:7077").appName("BCG_CASE_STUDY").getOrCreate()


df1 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/Primary_Person_use.csv")
resultt_count=df1.filter("PRSN_INJRY_SEV_ID =='KILLED' and PRSN_GNDR_ID == 'MALE'" ).count()
print(f"The number of crashes (accidents) in which number of persons killed are male : {resultt_count}")



df2 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/Units_use.csv")
result_count=df2.filter("VEH_BODY_STYL_ID =='MOTORCYCLE' or VEH_BODY_STYL_ID =='POLICE MOTORCYCLE'" ).count()
print(f" {result_count}  two wheelers are booked for crashes ")


#df1.filter("PRSN_GNDR_ID == 'FEMALE'").groupby("DRVR_LIC_STATE_ID").agg(count('*').alias("total_count")).orderBy(col("total_count").desc()).limit(1).select("DRVR_LIC_STATE_ID").show()
df1.filter("PRSN_GNDR_ID == 'FEMALE'").groupby("DRVR_LIC_STATE_ID").agg(count('*').alias("total_count")).orderBy(desc("total_count")).limit(1).select("DRVR_LIC_STATE_ID").show()



df_top_15=df1.join(df2, df1.CRASH_ID == df2.CRASH_ID ,"inner").filter("PRSN_INJRY_SEV_ID in ('KILLED' ,'INCAPACITATING INJURY' ,'NON-INCAPACITATING INJURY','POSSIBLE INJURY')").groupBy("VEH_MAKE_ID").count().orderBy(desc("count")).limit(15)
df_top_5=df1.join(df2, df1.CRASH_ID == df2.CRASH_ID ,"inner").filter("PRSN_INJRY_SEV_ID in ('KILLED' ,'INCAPACITATING INJURY' ,'NON-INCAPACITATING INJURY','POSSIBLE INJURY')").groupBy("VEH_MAKE_ID").count().orderBy(desc("count")).limit(5)
df_top_15.subtract(df_top_5).select(df_top_15.VEH_MAKE_ID).show()

df_temp = df1.join(df2, df1.CRASH_ID == df2.CRASH_ID ,"inner").groupBy("PRSN_ETHNICITY_ID","VEH_BODY_STYL_ID").agg(count("PRSN_ETHNICITY_ID").alias("ETHNICITY_count")).orderBy(col("VEH_BODY_STYL_ID"),col("ETHNICITY_count").desc()).select(df2.VEH_BODY_STYL_ID,df1.PRSN_ETHNICITY_ID,"ETHNICITY_count")
df_tempp=df_temp.withColumn("count_rank",rank().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("ETHNICITY_count")))).filter("count_rank == 1 and VEH_BODY_STYL_ID not in ('NA','NOT REPORTED','OTHER  (EXPLAIN IN NARRATIVE)')").select("PRSN_ETHNICITY_ID","VEH_BODY_STYL_ID").show()



df1.filter((col("PRSN_ALC_RSLT_ID") =='Positive') & col("DRVR_ZIP").isNotNull()).groupBy("DRVR_ZIP").agg(count('*').alias("Count")).orderBy(desc("Count")).limit(5).show()


df3 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/Damages_use.csv")
dis_crash=df2.join(df3,df2.CRASH_ID == df3.CRASH_ID ,"inner").filter((col("DAMAGED_PROPERTY") =='NONE') & col("VEH_DMAG_SCL_1_ID").isin('DAMAGED 5','DAMAGED 6','DAMAGED 7 HIGHEST')).select(df2.CRASH_ID).distinct().count()
print(f" {dis_crash} is the Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level  is above 4 and car avails Insurance")


df4= spark.read.option("header",True).option("inferSchema",True).csv("/input_data/Charges_use.csv")
df_lic_state=df2.groupBy("VEH_LIC_STATE_ID").count().orderBy(desc("count")).limit(25).select("VEH_LIC_STATE_ID").rdd.flatMap(lambda x:x).collect()
df_colour_id=df2.groupBy("VEH_COLOR_ID").count().orderBy(desc("count")).limit(10).select("VEH_COLOR_ID").rdd.flatMap(lambda x:x).collect()
df_vehicle_model=df1.join(df4,df1.CRASH_ID == df4.CRASH_ID ,"inner").join(df2,df1.CRASH_ID == df2.CRASH_ID ,"inner").filter(col("CHARGE").like("%CONTROL SPEED%") & col("DRVR_LIC_TYPE_ID").isin("COMMERCIAL DRIVER LIC.","DRIVER LICENSE") & col("VEH_COLOR_ID").isin(df_colour_id) & col("VEH_LIC_STATE_ID").isin(df_lic_state)).groupBy("VEH_MAKE_ID").count().orderBy(desc("count")).limit(5).select(df2.VEH_MAKE_ID).show()

