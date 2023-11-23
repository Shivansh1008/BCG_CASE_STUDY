from pyspark.sql.functions import *
from pyspark.sql.window import * /window
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

5---
df_temp = df1.join(df2, df1.CRASH_ID == df2.CRASH_ID ,"inner").groupBy("PRSN_ETHNICITY_ID","VEH_BODY_STYL_ID").agg(count("PRSN_ETHNICITY_ID").alias("ETHNICITY_count")).orderBy(col("VEH_BODY_STYL_ID"),col("ETHNICITY_count").desc()).select(df2.VEH_BODY_STYL_ID,df1.PRSN_ETHNICITY_ID,"ETHNICITY_count").show()

df_tempp=df_temp.withColumn("count_rank",rank().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("ETHNICITY_count")))).filter("count_rank == 1 and VEH_BODY_STYL_ID in ('NA','NOT REPORTED','OTHER  (EXPLAIN IN NARRATIVE)')").select("PRSN_ETHNICITY_ID","VEH_BODY_STYL_ID").show()

6-----

df1.filter((col("PRSN_ALC_RSLT_ID") =='Positive') & col("DRVR_ZIP").isNotNull()).groupBy("DRVR_ZIP").agg(count('*').alias("Count")).orderBy(desc("Count")).limit(5).show()

7--
df2.join(df3,df2.CRASH_ID == df3.CRASH_ID ,"inner").filter((col("DAMAGED_PROPERTY") =='NONE') & col("VEH_DMAG_SCL_1_ID") in ('DAMAGED 4','DAMAGED 5','DAMAGED 6','DAMAGED 7 HIGHEST') & col("VEH_DMAG_SCL_2_ID") in ('DAMAGED 4','DAMAGED 5','DAMAGED 6','DAMAGED 7 HIGHEST')).show()


df2.join(df3,df2.CRASH_ID == df3.CRASH_ID ,"inner").filter((col("DAMAGED_PROPERTY") =='NONE') & col("VEH_DMAG_SCL_1_ID").isin('DAMAGED 5','DAMAGED 6','DAMAGED 7 HIGHEST') & (df1.CRASH_ID==14870169)).select(df2.VEH_DMAG_SCL_2_ID,df2.CRASH_ID).show() 
