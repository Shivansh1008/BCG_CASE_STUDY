from pyspark.sql.functions import *
1-----
df1 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/Primary_Person_use.csv")
df1.filter("PRSN_INJRY_SEV_ID =='KILLED' and PRSN_GNDR_ID == 'MALE'" ).count()

2---

df2 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/Units_use.csv")
df2.filter("VEH_BODY_STYL_ID =='MOTORCYCLE' or VEH_BODY_STYL_ID =='POLICE MOTORCYCLE'" ).count()
3---

df1.filter("PRSN_GNDR_ID == 'FEMALE'").groupby("DRVR_LIC_STATE_ID").agg(count('*').alias("total_count")).orderBy(col("total_count").desc()).limit(1).select("DRVR_LIC_STATE_ID").show()
df1.filter("PRSN_GNDR_ID == 'FEMALE'").groupby("DRVR_LIC_STATE_ID").agg(count('*').alias("total_count")).orderBy(desc("total_count")).limit(1).select("DRVR_LIC_STATE_ID").show()

4---

df_top_15=df1.join(df2, df1.CRASH_ID == df2.CRASH_ID ,"inner").filter("PRSN_INJRY_SEV_ID in ('KILLED' ,'INCAPACITATING INJURY' ,'NON-INCAPACITATING INJURY','POSSIBLE INJURY')").groupBy("VEH_MAKE_ID").count().orderBy(desc("count")).limit(15)
df_top_5=df1.join(df2, df1.CRASH_ID == df2.CRASH_ID ,"inner").filter("PRSN_INJRY_SEV_ID in ('KILLED' ,'INCAPACITATING INJURY' ,'NON-INCAPACITATING INJURY','POSSIBLE INJURY')").groupBy("VEH_MAKE_ID").count().orderBy(desc("count")).limit(5)
df_top_15.subtract(df_top_5).select(df_top_15.VEH_MAKE_ID).show()

5---VEH_BODY_STYL_ID
df2.VEH_BODY_STYL_ID,
df_temp = df1.join(df2, df1.CRASH_ID == df2.CRASH_ID ,"inner").groupBy("PRSN_ETHNICITY_ID","VEH_BODY_STYL_ID").agg(count("PRSN_ETHNICITY_ID").alias("ETHNICITY_count")).orderBy(col("VEH_BODY_STYL_ID"),col("ETHNICITY_count").desc()).select(df2.VEH_BODY_STYL_ID,df1.PRSN_ETHNICITY_ID,"ETHNICITY_count").show()
df_temp.groupBy("PRSN_ETHNICITY_ID","VEH_BODY_STYL_ID").max("ETHNICITY_count").orderBy(col("VEH_BODY_STYL_ID")).show()


df1.filter(col("PRSN_ALC_RSLT_ID") =='Positive' & col("DRVR_ZIP").isNotNull()).groupBy("DRVR_ZIP").agg(count('*').alias("Count")).orderBy(desc("Count")).limit(5).show()\

