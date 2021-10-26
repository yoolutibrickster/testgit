# Databricks notebook source
# MAGIC %sh telnet github.comcast.com 443

# COMMAND ----------

# MAGIC %sql select * from alcyonia.tp_appreto_prod_flatten where sourceip is not null and FQDN is not null

# COMMAND ----------

# MAGIC %sql select * from alcyonia.tp_appreto_prod_flatten 
# MAGIC --where destinationip is not null and FQDN is not null

# COMMAND ----------

# MAGIC %sql select * from alcyonia.tp_appreto_prod_flatten where y=2021 and m=2 and metadata is not null

# COMMAND ----------

# MAGIC %sql with cte as  (select *, ROW_NUMBER() OVER (PARTITION BY metadata ORDER BY _time) as rn from alcyonia.tp_appreto_prod_flatten where y=2021 and m=2 )
# MAGIC select * from cte where rn=1

# COMMAND ----------

# MAGIC %sql select * from alcyonia.tp_appreto_prod_flatten where y=2020 and m=11 and d=30 and message is not null
# MAGIC --"@cloud:aws:accountid=763126650451"

# COMMAND ----------

# MAGIC %sql select _time, FQDN, operationalStatus from alcyonia.tp_appreto_prod_flatten where m=5  and FQDN='dovcas1-asa-46o.email.comcast.net' order by _time ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC select _time, 
# MAGIC        FQDN, 
# MAGIC        operationalStatus,
# MAGIC        rank
# MAGIC from (select  _time, 
# MAGIC               FQDN, 
# MAGIC               operationalStatus, 
# MAGIC               dense_rank() OVER (PARTITION BY FQDN, operationalStatus ORDER BY _time) as rank
# MAGIC       from alcyonia.tp_appreto_prod_flatten)
# MAGIC where FQDN is not null
# MAGIC -- rank = 1
# MAGIC order by FQDN, _time

# COMMAND ----------

# MAGIC %sql
# MAGIC select _time, 
# MAGIC        FQDN, 
# MAGIC        operationalStatus,
# MAGIC        rank
# MAGIC from (select  _time, 
# MAGIC               FQDN, 
# MAGIC               operationalStatus, 
# MAGIC               dense_rank() OVER (PARTITION BY FQDN, operationalStatus ORDER BY _time) as rank
# MAGIC       from alcyonia.tp_appreto_prod_flatten 
# MAGIC       where y=2020 and m=5 and d=14 and h=21)
# MAGIC where FQDN is not null 
# MAGIC -- rank = 1
# MAGIC order by FQDN, _time

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(FQDN)) from alcyonia.tp_appreto_prod_flatten 
# MAGIC     --  where y=2020 and m=5 and d=14 and h=21 -- 566
# MAGIC      -- and  FQDN is not null

# COMMAND ----------

#select FQDN, create_map(operationalStatus, _time) from alcyonia.tp_appreto_prod_flatten group by FQDN --1323
from pyspark.sql.functions import *

df = spark.table("alcyonia.tp_appreto_prod_flatten").\
        where('FQDN is not null').\
        groupby("FQDN").\
        agg('FQDN', collect_list(create_map('operationalStatus', '_time')))

display(df.where("FQDN in ('2001:558:1046:9ed6:4a0f:cfff:feea:4cb1','2001:558:1046:9ed6:4a0f:cfff:feea:4cc1','2001:558:1046:9ed6:4a0f:cfff:feea:5c11','2001:558:1046:9ed6:4a0f:cfff:fef5:10a1','2001:558:1046:9ed6:7210:6fff:fe9f:edd2','dovcas1-asa-02o.email.comcast.net','dovcas1-asa-03o.email.comcast.net','dovcas1-asa-04o.email.comcast.net','dovcas1-asa-05o.email.comcast.net','dovcas1-asa-06o.email.comcast.net','dovcas1-asa-07o.email.comcast.net','dovcas1-asa-08o.email.comcast.net','dovcas1-asa-09o.email.comcast.net','dovcas1-asa-10o.email.comcast.net','dovcas1-asa-11o.email.comcast.net','dovcas1-asa-12o.email.comcast.net','dovcas1-asa-13o.email.comcast.net','dovcas1-asa-14o.email.comcast.net','dovcas1-asa-15o.email.comcast.net','dovcas1-asa-16o.email.comcast.net','dovcas1-asa-17o.email.comcast.net','dovcas1-asa-18o.email.comcast.net','dovcas1-asa-19o.email.comcast.net','dovcas1-asa-20o.email.comcast.net','dovcas1-asa-21o.email.comcast.net','qa-dovcas-asc-01o.email.comcast.net','qa-dovcas-asc-02o.email.comcast.net','qa-dovcas-asc-03o.email.comcast.net','qa-dovcas-asc-04o.email.comcast.net','qa-dovcas-ch2g-01o.email.comcast.net','qa-dovcas-ch2g-02o.email.comcast.net','qa-dovcas-ch2g-03o.email.comcast.net','qa-dovcas-ch2g-04o.email.comcast.net','qa-dovcas-hoc-01o.email.comcast.net','qa-dovcas-hoc-02o.email.comcast.net','qa-dovcas-hoc-03o.email.comcast.net','qa-dovcas-hoc-04o.email.comcast.net','qa-resiscd-as-01p.email.comcast.net','qa-resiscd-as-02p.email.comcast.net','qa-resiscd-as-03p.email.comcast.net','qa-resiscd-as-07p','qadovhapxy-ch2g-01o.email.comcast.net','qadovhapxy-ch2g-02o.email.comcast.net','qaresuper-as-01v.sys.comcast.net')"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select FQDN, _time, operationalStatus from alcyonia.tp_appreto_prod_flatten where FQDN is not null 
# MAGIC order by 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.*, DATEDIFF(a.max_dt, a.min_dt) as days from
# MAGIC     (select b.FQDN, collect_list(b.details), min(_time) as min_dt, max(_time) as max_dt, count(*) as cnt 
# MAGIC     from (select *, map(_time, operationalStatus) as details from alcyonia.tp_appreto_prod_flatten where FQDN is not null )b
# MAGIC     group by 1)a
# MAGIC where a.cnt < 5
# MAGIC order by days desc
# MAGIC 
# MAGIC --select map(_time, operationalStatus) as details from alcyonia.tp_appreto_prod_flatten where FQDN is not null

# COMMAND ----------

w1 = Window.partitionBy("FQDN").orderBy("_time")


# COMMAND ----------

B1 = (B
    .withColumn(
        "ind", 
        # Compute cumulative sum of the indicator variables over window
        sum(
            # yield 1 if date has changed from the previous row, 0 otherwise
            (lag("operationalStatus", 1).over(w1) != col("operationalStatus")).cast("int")
        ).over(w1))
    # Date has change for the first time when cumulative sum is equal to 1
    )

#display(B1.where("ind !=0"))
display(B1)

# COMMAND ----------

df1 = spark.table("alcyonia.tp_appreto_prod_flatten").where("m=6 and d=8 and FQDN is not null").select('fqdn', '_time', 'destinationIP', 'sourceIP', 'operationalStatus')
#display(df1.groupBy('fqdn').count().select('fqdn', col('count').alias('n')))
display(df1.where("fqdn='qadovpxy-ch2g-02o.email.comcast.net'"))

# COMMAND ----------

df2=df1.withColumn("rank", rank().over(Window.partitionBy('FQDN','operationalStatus').orderBy(asc('_time')))).where("fqdn='qadovpxy-ch2g-02o.email.comcast.net'")
display(df2)

# COMMAND ----------

display(df1
        .withColumn("prevStatus", lag("operationalStatus",1).over(Window.partitionBy('FQDN').orderBy(asc('_time'))))
        .where("fqdn='qadovpxy-ch2g-02o.email.comcast.net'")
       )


# COMMAND ----------

display(df2)
#.groupBy("FQDN",'operationalStatus').agg(F.max("rank").alias("rank"))

# COMMAND ----------

df1.select('FQDN').distinct().count() -- 1324


# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(distinct(FQDN)) from alcyonia.tp_appreto_prod_flatten where FQDN is not null and m=6 and d=8 --363
# MAGIC --select count(distinct(FQDN)) from alcyonia.tp_appreto_prod_flatten where FQDN is not null and m=6 and d=9 --380
# MAGIC 
# MAGIC --select count(distinct(FQDN)) from alcyonia.tp_appreto_prod_flatten where FQDN is not null and m=6 and d=8 and FQDN in (select distinct(FQDN) from alcyonia.tp_appreto_prod_flatten where FQDN is not null and m=6 and d=9) --357
# MAGIC 
# MAGIC select * from alcyonia.tp_appreto_prod_flatten where FQDN is not null 
# MAGIC --and m=6 and d=9 
# MAGIC and sourceIP is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into `alcyonia`.`appreto_tsf_msn_eject` select 'fqdn', '_time', 'destinationip', 'sourceip', 'operationalstatus' from alcyonia.tp_appreto_prod_flatten where FQDN is not null and m=6 and d=8

# COMMAND ----------

display(spark.read.json('s3a://secure-backwaters/comcast.tp.appreto.prod/Year=2020/Month=06/Day=09/Hr=21/ls.s3.c42984ab-2ade-4bb6-be7c-d4837c19b1ac.2020-06-09T21.15.part3.txt.gz'))

# COMMAND ----------

spark.sql("""select 
                FQDN
                ,_time as activity_time
                ,operationalStatus
                ,prevStatus
                from 
                    (SELECT
                      *,
                      lag(operationalStatus) OVER(PARTITION BY FQDN ORDER BY _time) as prevStatus
                     FROM alcyonia.tp_appreto_prod_flatten where FQDN is not null and m=6 and d=8) tmp""").\
    registerTempTable("threat_stg")

win = Window.partitionBy('FQDN').orderBy('_time')
fdf = spark.table(alcyonia.tp_appreto_prod_flatten). \
    where("FQDN is not null and m=6 and d=8"). \
    withColumnRenamed('_time', 'activity_time'). \
    select('FQDN', 'activity_time', 'operationalStatus'). \
    withColumn('prevStatus', lag('operationalStatus').over(win)). \
    filter(col('prevStatus').isNull() | (col('prevStatus') != col('operationalStatus')) ). \
    select('FQDN', 'activity_time', 'operationalStatus', 'prevStatus', )

#.withColumn("prevStatus", lag("operationalStatus",1).over(Window.partitionBy('FQDN').orderBy(asc('_time'))))

# COMMAND ----------

win = Window.partitionBy('FQDN').orderBy('activity_time')
fdf = spark.table('alcyonia.tp_appreto_prod_flatten'). \
    where("FQDN is not null and m=6 and d=8"). \
    withColumnRenamed('_time', 'activity_time'). \
    select('FQDN', 'activity_time', 'operationalStatus'). \
    withColumn('prevStatus', lag('operationalStatus').over(win)). \
    filter(col('prevStatus').isNull() | (col('prevStatus') != col('operationalStatus')) ). \
    select('FQDN', 'activity_time', 'operationalStatus' )
display(fdf.where("fqdn='qadovhapxy-asc-01o.email.comcast.net'"))

# COMMAND ----------

fdf1 = newdf.groupBy('fqdn').count().select('fqdn', col('count').alias('cnt'))
display(fdf1.where("cnt>1"))
#fdf.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from threat_stg where fqdn='qadovpxy-ch2g-02o.email.comcast.net'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from threat_stg where fqdn='qadovpxy-ch2g-02o.email.comcast.net'

# COMMAND ----------

from pyspark.sql.window import Window

win = Window.partitionBy('FQDN').orderBy('activity_time')
fdf = spark.table('alcyonia.tp_appreto_prod_flatten'). \
    where("FQDN is not null and m=6 and d=8"). \
    withColumnRenamed('_time', 'activity_time'). \
    withColumn('prevStatus', lag('operationalStatus').over(win)). \
    withColumn('topic', lit('comcast.tp.appreto.prod')). \
    filter(col('prevStatus').isNull() | (col('prevStatus') != col('operationalStatus')) ). \
    select('FQDN', 'activity_time', 'operationalStatus', 'destinationIP', 'sourceIP', 'lastSyncTime', 'topic', 'y', 'm', 'd', 'h')
 
fdf.write.format('delta').mode('overwrite').saveAsTable("`alcyonia`.`appreto_tsf_msn_eject`")


# COMMAND ----------

# spark.sql("""select FQDN, activity_time, operationalStatus from 
#     (SELECT *, dense_rank() OVER(PARTITION BY FQDN ORDER BY activity_time DESC) as rank
#      FROM alcyonia.appreto_tsf_msn_eject) tmp
#      WHERE tmp.rank = 1 and env='prod'"""). \
#     registerTempTable("eject")

enrich = "alcyonia.appreto_tsf_msn_eject"
win = Window.partitionBy('FQDN').orderBy('activity_time')

spark.table('alcyonia.tp_appreto_prod_flatten'). \
    where("FQDN is not null and m=6 and d=9"). \
    withColumnRenamed('_time', 'activity_time'). \
    withColumn('topic', lit('comcast.tp.appreto.prod')). \
    select('FQDN', 'activity_time', 'operationalStatus', 'destinationIP', 'sourceIP', 'lastSyncTime', 'topic', 'y', 'm', 'd', 'h').\
    union(spark.sql("""select FQDN,
                             activity_time, 
                             operationalStatus,
                             destinationIP,
                             sourceIP,
                             lastSyncTime,
                             topic,
                             y, m, d, h
                             from (
                                 SELECT *, dense_rank() OVER(PARTITION BY FQDN ORDER BY activity_time DESC) as rank
                                 FROM {} where topic = 'comcast.tp.appreto.prod') tmp
                             WHERE tmp.rank = 1""".format(enrich))). \
    withColumn('prevStatus', lag('operationalStatus').over(win)). \
    filter(col('prevStatus').isNull() | (col('prevStatus') != col('operationalStatus')) ). \
    select('FQDN', 'activity_time', 'operationalStatus', 'destinationIP', 'sourceIP', 'lastSyncTime', 'topic', 'y', 'm', 'd', 'h'). \
    registerTempTable("stage")


spark.sql("""
    MERGE INTO alcyonia.appreto_tsf_msn_eject target
    USING stage st
    ON target.fqdn = st.fqdn
    AND target.operationalStatus = st.operationalStatus
    AND target.activity_time = st.activity_time
    AND target.topic = st.topic
    WHEN NOT MATCHED THEN
        INSERT *""")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stage

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from target -- 656
# MAGIC --select count(*) from stage -- 363
# MAGIC select * from alcyonia.appreto_tsf_msn_eject where fqdn in ('qadovpxy-asc-02o.email.comcast.net','qadovpxy-ch2g-02o.email.comcast.net','qadovpxy-asc-01o.email.comcast.net','qadovhapxy-asc-01o.email.comcast.net','qa-resiscd-as-08p','qadovhapxy-asc-02o.email.comcast.net','qadovpxy-ch2g-01o.email.comcast.net','qadovhapxy-ch2g-01o.email.comcast.net','qa-resiscd-as-04p','qa-resiscd-as-06p','stgdovpxy-asc-02o.email.comcast.net','stgdovpxy-ch2g-01o.email.comcast.net','qa-resiscd-ch2-07p','qa-resiscd-ch2-06p','qadovhapxy-ch2g-02o.email.comcast.net','qa-resiscd-as-09p','qa-resiscd-as-05p','qa-resiscd-ch2-04p','qa-resiscd-ch2-09p','stgdovpxy-asc-01o.email.comcast.net','qa-resiscd-as-07p','stgdovback2-asc-01o.email.comcast.net','stgdovpxy-ch2g-02o.email.comcast.net','qa-resiscd-po-09p','qa-resiscd-po-07p','qa-resiscd-ch2-08p','qa-resiscd-po-04p','qa-resiscd-po-05p','stgdovhapxy-ch2g-02o.email.comcast.net','qa-resiscd-po-06p.email.comcast.net','stgdovdir2-ch2g-01o.email.comcast.net','stgdovdir2-asc-01o.email.comcast.net','stgdovback2-ch2g-02o.email.comcast.net','stgdovback2-asc-02o.email.comcast.net','stgdovhapxy-asc-02o.email.comcast.net','stgdovhapxy-ch2g-01o.email.comcast.net','stgdovdir2-ch2g-02o.email.comcast.net','stgdovback2-ch2g-01o.email.comcast.net','stgdovdir2-asc-02o.email.comcast.net','stgdovhapxy-asc-01o.email.comcast.net','qa-resiscd-po-08p','oxbasicqa-asc-01o.email.comcast.net','qadovback1-ch2g-01o.email.comcast.net','qa-dovcasrep-ch2g-01o.email.comcast.net','qadovdir1-asc-02o.email.comcast.net','svdev-asb-01.sys.comcast.net','qadovdir1-ch2g-01o.email.comcast.net','qadovback1-ch2g-02o.email.comcast.net','qadovback1-asc-02o.email.comcast.net','qadovback1-asc-01o.email.comcast.net','qadovdir1-asc-01o.email.comcast.net','qadovdir1-ch2g-02o.email.comcast.net','ltdovback0-ch2f-01o.email.comcast.net','ltdovback0-hoc-01o.email.comcast.net','ltdovdir0-ch2f-02o.email.comcast.net','ip-10-144-38-24.email.comcast.net','ltdovpxy-ch2f-02o.email.comcast.net','ltdovhapxy-ch2f-01o.email.comcast.net','ltdovback0-ch2f-03o.email.comcast.net','ltdovdir0-hoc-02o.email.comcast.net.openstacklocal','ltdovpxy-asc-01o.email.comcast.net','qa-resiscd-ch2-03p.email.comcast.net','ltdovdir0-asc-01o.email.comcast.net','ltdovback0-hoc-02o.email.comcast.net','ltdovhapxy-ch2f-02o.email.comcast.net','ltdovback0-asc-01o.email.comcast.net','ltdovpxy-asc-02o.email.comcast.net','ltdovdir0-ch2f-01o.email.comcast.net','ltdovback0-hoc-03o.email.comcast.net','ltdovdir0-hoc-01o.email.comcast.net.openstacklocal','ip-10-144-38-235.email.comcast.net','ltdovback0-asc-02o.email.comcast.net','ltdovpxy-hoc-02o.email.comcast.net','ip-10-144-38-107.email.comcast.net','ip-10-144-38-110.email.comcast.net','ip-10-144-38-175.email.comcast.net','ltdovpxy-hoc-01o.email.comcast.net','ltdovback0-asc-03o.email.comcast.net','ltdovdir0-asc-02o.email.comcast.net','ltdovback0-ch2f-02o.email.comcast.net','ltdovpxy-ch2f-01o.email.comcast.net') order by fqdn, activity_time asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select fqdn,_time,operationalStatus from alcyonia.tp_appreto_prod_flatten where m=6 and d in (8,9) and fqdn in ('qadovpxy-asc-02o.email.comcast.net','qadovpxy-ch2g-02o.email.comcast.net','qadovpxy-asc-01o.email.comcast.net','qadovhapxy-asc-01o.email.comcast.net','qa-resiscd-as-08p','qadovhapxy-asc-02o.email.comcast.net','qadovpxy-ch2g-01o.email.comcast.net','qadovhapxy-ch2g-01o.email.comcast.net','qa-resiscd-as-04p','qa-resiscd-as-06p','stgdovpxy-asc-02o.email.comcast.net','stgdovpxy-ch2g-01o.email.comcast.net','qa-resiscd-ch2-07p','qa-resiscd-ch2-06p','qadovhapxy-ch2g-02o.email.comcast.net','qa-resiscd-as-09p','qa-resiscd-as-05p','qa-resiscd-ch2-04p','qa-resiscd-ch2-09p','stgdovpxy-asc-01o.email.comcast.net','qa-resiscd-as-07p','stgdovback2-asc-01o.email.comcast.net','stgdovpxy-ch2g-02o.email.comcast.net','qa-resiscd-po-09p','qa-resiscd-po-07p','qa-resiscd-ch2-08p','qa-resiscd-po-04p','qa-resiscd-po-05p','stgdovhapxy-ch2g-02o.email.comcast.net','qa-resiscd-po-06p.email.comcast.net','stgdovdir2-ch2g-01o.email.comcast.net','stgdovdir2-asc-01o.email.comcast.net','stgdovback2-ch2g-02o.email.comcast.net','stgdovback2-asc-02o.email.comcast.net','stgdovhapxy-asc-02o.email.comcast.net','stgdovhapxy-ch2g-01o.email.comcast.net','stgdovdir2-ch2g-02o.email.comcast.net','stgdovback2-ch2g-01o.email.comcast.net','stgdovdir2-asc-02o.email.comcast.net','stgdovhapxy-asc-01o.email.comcast.net','qa-resiscd-po-08p','oxbasicqa-asc-01o.email.comcast.net','qadovback1-ch2g-01o.email.comcast.net','qa-dovcasrep-ch2g-01o.email.comcast.net','qadovdir1-asc-02o.email.comcast.net','svdev-asb-01.sys.comcast.net','qadovdir1-ch2g-01o.email.comcast.net','qadovback1-ch2g-02o.email.comcast.net','qadovback1-asc-02o.email.comcast.net','qadovback1-asc-01o.email.comcast.net','qadovdir1-asc-01o.email.comcast.net','qadovdir1-ch2g-02o.email.comcast.net','ltdovback0-ch2f-01o.email.comcast.net','ltdovback0-hoc-01o.email.comcast.net','ltdovdir0-ch2f-02o.email.comcast.net','ip-10-144-38-24.email.comcast.net','ltdovpxy-ch2f-02o.email.comcast.net','ltdovhapxy-ch2f-01o.email.comcast.net','ltdovback0-ch2f-03o.email.comcast.net','ltdovdir0-hoc-02o.email.comcast.net.openstacklocal','ltdovpxy-asc-01o.email.comcast.net','qa-resiscd-ch2-03p.email.comcast.net','ltdovdir0-asc-01o.email.comcast.net','ltdovback0-hoc-02o.email.comcast.net','ltdovhapxy-ch2f-02o.email.comcast.net','ltdovback0-asc-01o.email.comcast.net','ltdovpxy-asc-02o.email.comcast.net','ltdovdir0-ch2f-01o.email.comcast.net','ltdovback0-hoc-03o.email.comcast.net','ltdovdir0-hoc-01o.email.comcast.net.openstacklocal','ip-10-144-38-235.email.comcast.net','ltdovback0-asc-02o.email.comcast.net','ltdovpxy-hoc-02o.email.comcast.net','ip-10-144-38-107.email.comcast.net','ip-10-144-38-110.email.comcast.net','ip-10-144-38-175.email.comcast.net','ltdovpxy-hoc-01o.email.comcast.net','ltdovback0-asc-03o.email.comcast.net','ltdovdir0-asc-02o.email.comcast.net','ltdovback0-ch2f-02o.email.comcast.net','ltdovpxy-ch2f-01o.email.comcast.net') order by fqdn, _time asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select fqdn,_time,operationalStatus from alcyonia.tp_appreto_prod_flatten where y=2020 and m=6 and d=9 and h=16 and fqdn is not null order by fqdn, _time asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select fqdn,activity_time,operationalStatus from alcyonia.appreto_tsf_msn_eject where y=2020 and m=6 and d=9 and h=16 order by fqdn, activity_time asc

# COMMAND ----------

spark.sql("""select 
                FQDN
                ,operationalStatus
                ,_time as activity_time
                from 
                    (SELECT
                      *,
                      dense_rank() OVER(PARTITION BY FQDN, operationalStatus ORDER BY _time DESC) as rank
                     FROM alcyonia.tp_appreto_prod_flatten where FQDN is not null and m=6 and d=8) tmp
                  WHERE tmp.rank = 1""").\
    registerTempTable("threat_stg")

%sql
merge into `alcyonia`.`appreto_tsf_msn_eject` target
USING threat_stg stage
ON target.fqdn = stage.fqdn
AND target.operationalStatus = stage.operationalStatus
WHEN MATCHED THEN
  UPDATE SET target.activity_time = stage.activity_time
WHEN NOT MATCHED THEN
  INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from
# MAGIC   (select FQDN, operationalStatus, activity_time from 
# MAGIC     (SELECT *, dense_rank() OVER(PARTITION BY FQDN ORDER BY activity_time DESC) as rank
# MAGIC      FROM alcyonia.appreto_tsf_msn_eject) tmp
# MAGIC   WHERE tmp.rank = 1)
# MAGIC --467
# MAGIC -- 478 -411
# MAGIC --select count(*) from alcyonia.tp_appreto_prod_flatten where FQDN is not null and m=6 and d=8 --478

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `alcyonia`.`appreto_tsf_msn_eject` where fqdn='qadovpxy-ch2g-02o.email.comcast.net' order by activity_time asc

# COMMAND ----------

# MAGIC %sql select fqdn, _time, destinationIP, sourceIP, operationalStatus from alcyonia.tp_appreto_prod_flatten where FQDN is not null and m=6 and d=8 and fqdn='qadovpxy-ch2g-02o.email.comcast.net' order by _time asc

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table `alcyonia`.`appreto_tsf_msn_eject`
# MAGIC --alter table `alcyonia`.`appreto_tsf_msn_eject` add columns(namespace string after lastSyncTime);
# MAGIC --ALTER TABLE table_name ADD COLUMNS (col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)
# MAGIC --, policyID string, namespace string

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table `alcyonia`.`appreto_tsf_msn_eject`;
# MAGIC CREATE TABLE `alcyonia`.`appreto_tsf_msn_eject` (`fqdn` STRING, `activity_time` TIMESTAMP, operationalStatus STRING, destinationIP STRING, sourceIP STRING, lastSyncTime TIMESTAMP, topic STRING, y int, m int, d int, h int)
# MAGIC USING delta
# MAGIC OPTIONS (
# MAGIC   path 's3a://tpx-sse-ds/data/analytics/tp_appreto/tsf_msn_eject/'
# MAGIC )

# COMMAND ----------

# MAGIC %fs ls s3a://tpx-sse-ds/data/analytics/microsoft_ata/host_alerts/

# COMMAND ----------

# MAGIC %sql
# MAGIC create table alcyonia.tp_appreto_prod_flatten

# COMMAND ----------

import sys
sys.argv = [
  
  '--script.py',
  '-y', '2020',
  '-m','6',
  '-d','16',
  '--hour','10'
  
]
del sys

# COMMAND ----------

from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import argparse
import os
import sys
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, IntegerType, LongType, MapType, StringType, StructType, TimestampType, BooleanType

sys.path.append('/dbfs/mnt/alcyonia/etl/python/common')
import common_utils

ARGS = argparse.ArgumentParser()
ARGS.add_argument('--raw-dir', dest='raw_dir', default='s3a://secure-backwaters/comcast.tp.appreto.prod', help='raw directory to process')
ARGS.add_argument('--flatten-table', dest='flatten_table', default='alcyonia.tp_appreto_prod_flatten', help='flatten table')
ARGS.add_argument('--enrich-table', dest='enrich_table', default='alcyonia.appreto_tsf_msn_eject', help='enrich table')
ARGS.add_argument('--log-table', dest='log_table', default='alcyonia.job_logs', help='log table')
ARGS.add_argument('-y', '--year', dest='year', required=True, type=int, help='year to process')
ARGS.add_argument('-m', '--month', dest='month', required=True, type=int, help='month to process')
ARGS.add_argument('-d', '--day', dest='day', required=True, type=int, help='day to process')
ARGS.add_argument('--hour', dest='hour', required=True, type=int, help='hour to process')

# defining schema to read comcast.tp.appreto.prod raw data
SCHEMA = StructType(). \
    add('@timestamp', StringType()). \
    add('@version', StringType()). \
    add('FQDN', StringType()). \
    add('ID', StringType()). \
    add('action', StringType()). \
    add('associatedTags', ArrayType(StringType())). \
    add('certificate', StringType()). \
    add('certificateRequest', StringType()). \
    add('claims', ArrayType(StringType())). \
    add('collectInfo', BooleanType()). \
    add('collectedInfo', StructType(). \
        add("cache registry", StringType()). \
        add("cat /proc/net/netfilter/nfnetlink_queue", StringType()). \
        add("diagnostic", StringType()). \
        add("dockerbench output", StringType()). \
        add("enforcer logs: main", StringType()). \
        add("ipset -o save -L", StringType()). \
        add("iptables -t mangle -nvL", StringType()). \
        add("iptables -t nat -nvL", StringType()). \
        add("process information", StringType())). \
    add('createTime', StringType()). \
    add('currentVersion', StringType()). \
    add('data', StringType()). \
    add('date', StringType()). \
    add('description', StringType()). \
    add('destinationID', StringType()). \
    add('destinationIP', StringType()). \
    add('destinationPort', LongType()). \
    add('destinationType', StringType()). \
    add('diff', StringType()). \
    add('dropReason', StringType()). \
    add('encrypted', BooleanType()). \
    add('enforcementStatus', StringType()). \
    add('eventCreationTime', StringType()). \
    add('eventReceiptTime', StringType()). \
    add('lastCollectionTime', StringType()). \
    add('lastSyncTime', StringType()). \
    add('localCA', StringType()). \
    add('logLevel', StringType()). \
    add('logLevelDuration', StringType()). \
    add('machineID', StringType()). \
    add('message', StringType()). \
    add('metadata', ArrayType(StringType())). \
    add('name', StringType()). \
    add('namespace', StringType()). \
    add('normalizedTags', ArrayType(StringType())). \
    add('observed', BooleanType()). \
    add('observedAction', StringType()). \
    add('observedDropReason', StringType()). \
    add('observedEncrypted', BooleanType()). \
    add('observedPolicyID', StringType()). \
    add('observedPolicyNamespace', StringType()). \
    add('operation', StringType()). \
    add('operationalStatus', StringType()). \
    add('originalData', StringType()). \
    add('policyID', StringType()). \
    add('policyNamespace', StringType()). \
    add('protected', BooleanType()). \
    add('protocol', LongType()). \
    add('publicToken', StringType()). \
    add('serviceClaimHash', StringType()). \
    add('serviceID', StringType()). \
    add('serviceNamespace', StringType()). \
    add('serviceType', StringType()). \
    add('serviceURL', StringType()). \
    add('sourceID', StringType()). \
    add('sourceIP', StringType()). \
    add('sourceType', StringType()). \
    add('startTime', StringType()). \
    add('subnets', ArrayType(StringType())). \
    add('tags', ArrayType(StringType())). \
    add('targetIdentity', StringType()). \
    add('timestamp', StringType()). \
    add('unreachable', BooleanType()). \
    add('updateAvailable', BooleanType()). \
    add('updateTime', StringType()). \
    add('value', LongType()). \
    add('zone', LongType())

args = ARGS.parse_args()


def main(raw_path):
    try:
        spark = SparkSession.builder.config("spark.sql.files.ignoreCorruptFiles", "true").getOrCreate()
        print('Processing comcast.tp.appreto.prod in {}'.format(raw_path))
        # apply schema, renaming the columns
        df = spark.read.json(raw_path, schema=SCHEMA). \
            withColumn('input_file_name', input_file_name())
        df2 = df.select('FQDN', 'ID', 'action', 'associatedTags', 'certificate', 'certificateRequest', 'claims', 'collectInfo', 'collectedInfo',
                        'createTime', 'currentVersion', 'data', 'date', 'description', 'destinationID', 'destinationIP', 'destinationPort',
                        'destinationType', 'diff', 'dropReason', 'encrypted', 'enforcementStatus', 'eventCreationTime', 'eventReceiptTime',
                        'lastCollectionTime', 'lastSyncTime', 'localCA', 'logLevel', 'logLevelDuration', 'machineID', 'message', 'metadata',
                        'name', 'namespace', 'normalizedTags', 'observed', 'observedAction', 'observedDropReason', 'observedEncrypted',
                        'observedPolicyID', 'observedPolicyNamespace', 'operation', 'operationalStatus', 'originalData', 'policyID', 'policyNamespace',
                        'protected', 'protocol', 'publicToken', 'serviceClaimHash', 'serviceID', 'serviceNamespace', 'serviceType', 'serviceURL',
                        'sourceID', 'sourceIP', 'sourceType', 'startTime', 'subnets', 'tags', 'targetIdentity', 'timestamp',
                        'unreachable', 'updateAvailable', 'updateTime', 'value', 'zone', 'input_file_name'). \
            withColumn('_time', col('timestamp').cast('timestamp')). \
            withColumn('createTime', col('createTime').cast('timestamp')). \
            withColumn('lastCollectionTime', col('lastCollectionTime').cast('timestamp')). \
            withColumn('lastSyncTime', col('lastSyncTime').cast('timestamp')). \
            withColumn('startTime', col('startTime').cast('timestamp')). \
            withColumn('timestamp', col('timestamp').cast('timestamp')). \
            withColumn('updateTime', col('updateTime').cast('timestamp')). \
            withColumn('eventCreationTime', col('eventCreationTime').cast('timestamp')). \
            withColumn('eventReceiptTime', col('eventReceiptTime').cast('timestamp')). \
            withColumn('date', col('date').cast('timestamp')). \
            withColumn('collectedInfo', to_json(col('collectedInfo')))

        df3 = df2.select('_time', 'FQDN', 'ID', 'action', 'associatedTags', 'certificate', 'certificateRequest', 'claims', 'collectInfo',
                         'collectedInfo', 'createTime', 'currentVersion', 'data', 'date', 'description', 'destinationID', 'destinationIP',
                         'destinationPort', 'destinationType', 'diff', 'dropReason', 'encrypted', 'enforcementStatus', 'eventCreationTime',
                         'eventReceiptTime', 'lastCollectionTime', 'lastSyncTime', 'localCA', 'logLevel', 'logLevelDuration', 'machineID',
                         'message', 'metadata', 'name', 'namespace', 'normalizedTags', 'observed', 'observedAction', 'observedDropReason',
                         'observedEncrypted', 'observedPolicyID', 'observedPolicyNamespace', 'operation', 'operationalStatus', 'originalData',
                         'policyID', 'policyNamespace', 'protected', 'protocol', 'publicToken', 'serviceClaimHash', 'serviceID', 'serviceNamespace',
                         'serviceType', 'serviceURL', 'sourceID', 'sourceIP', 'sourceType', 'startTime', 'subnets', 'tags', 'targetIdentity',
                         'timestamp', 'unreachable', 'updateAvailable', 'updateTime', 'value', 'zone', 'input_file_name'). \
            withColumn('_time', coalesce(col('timestamp'), col('eventCreationTime')))
        optimize_col_list = ['_time']
        
        # invoking write_delta_table function
        common_utils.write_delta_table(spark, df3, args.flatten_table, "overwrite", args.year, args.month, args.day, args.hour, optimize_col_list)
        common_utils.dq_count(spark, df3, args.flatten_table, args.flatten_table, args.year, args.month, args.day, args.hour)
        
        #data enrichment for TSF-MSN Eject use case Splunk alerts
        window = Window.partitionBy('FQDN').orderBy('activity_time')
        
        df3.where("FQDN is not null"). \
            withColumnRenamed('_time', 'activity_time'). \
            withColumn('topic', lit('comcast.tp.appreto.prod')). \
            withColumn('y', lit(args.year)). \
            withColumn('m', lit(args.month)). \
            withColumn('d', lit(args.day)). \
            withColumn('h', lit(args.hour)). \
            select('FQDN', 'activity_time', 'operationalStatus', 'destinationIP', 'sourceIP', 'lastSyncTime', 'topic', 'y', 'm', 'd', 'h').\
            union(spark.sql("""select FQDN,
                                     activity_time, 
                                     operationalStatus,
                                     destinationIP,
                                     sourceIP,
                                     lastSyncTime,
                                     topic,
                                     y, m, d, h
                                     from (
                                         SELECT *, dense_rank() OVER(PARTITION BY FQDN ORDER BY activity_time DESC) as rank
                                         FROM alcyonia.appreto_tsf_msn_eject where topic = 'comcast.tp.appreto.prod') tmp
                                     WHERE tmp.rank = 1""".format(args.enrich_table))). \
            withColumn('prevStatus', lag('operationalStatus').over(window)). \
            filter(col('prevStatus').isNull() | (col('prevStatus') != col('operationalStatus')) ). \
            select('FQDN', 'activity_time', 'operationalStatus', 'destinationIP', 'sourceIP', 'lastSyncTime', 'topic', 'y', 'm', 'd', 'h'). \
            registerTempTable("stage")

        spark.sql("""
            MERGE INTO {} target
            USING stage st
            ON target.fqdn = st.fqdn
            AND target.operationalStatus = st.operationalStatus
            AND target.activity_time = st.activity_time
            AND target.topic = st.topic
            WHEN NOT MATCHED THEN
                INSERT *""".format(args.enrich_table))
        
    except:
        common_utils.exception_logging(spark, args.flatten_table, args.log_table, sys.exc_info()[1], args.year, args.month, args.day, args.hour)
        exit(1)


if __name__ == '__main__':
    raw_path = os.path.join(args.raw_dir, 'Year={}/Month={:02d}/Day={:02d}/Hr={:02d}'.format(args.year, args.month, args.day, args.hour))
    flag = common_utils.raw_path_exists(spark, raw_path, args.flatten_table, args.log_table, args.year, args.month, args.day, args.hour)
if flag == True:
    main(raw_path)

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F

df = spark.sql("select * from alcyonia.appreto_tsf_msn_eject where y=2020 and m=7 and d=16").\
      withColumn("activity_time_30", col("activity_time") - F.expr("Interval 30 minutes"))
df.createOrReplaceTempView('df')
display(df)

#df1 = df.withColumn('birthdaytime_new', df.birthdaytime + F.expr('INTERVAL 50 minutes'))


# COMMAND ----------

# MAGIC %sql
# MAGIC --select FQDN, d, h, count(*) from df where FQDN='qadovpxy-ch2g-02o.email.comcast.net' and d=16 group by 1,2,3 order by 1,2 asc
# MAGIC select FQDN, d, h, count(*) from alcyonia.appreto_tsf_msn_eject where m=7 group by 1,2,3 order by 1,2,3 asc

# COMMAND ----------

# MAGIC %sql select * from alcyonia.appreto_tsf_msn_eject where m=9 and d=3 and h=17

# COMMAND ----------

# MAGIC %sql
# MAGIC --select FQDN, h, count(*) from df group by 1,2
# MAGIC --select * from df where h=6 
# MAGIC --and activity_time between 
# MAGIC --FQDN="qa-swanapp-ch2g-01o.email.comcast.net" and h=6
# MAGIC SELECT *, count(*) OVER (
# MAGIC   PARTITION BY FQDN 
# MAGIC   ORDER BY  activity_time DESC
# MAGIC   RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW
# MAGIC ) AS count FROM df where topic = 'comcast.tp.appreto.prod' and h=11
# MAGIC --and FQDN in (select FQDN from alcyonia.appreto_tsf_msn_eject where y=2020 and m=7 and d=16 and h=11)

# COMMAND ----------

# MAGIC %md ###working scenario###

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F

data = spark.sql("select * from alcyonia.appreto_tsf_msn_eject where y=2020 and m=7 and d=16 and h=11")
window = Window.partitionBy('FQDN').orderBy('activity_time')
ranked = data.\
        withColumn('nextStatus', lead('operationalStatus').over(window)).\
        filter(col('operationalStatus') != F.lit("Connected")).\
        withColumn('flag', F.when((F.col("operationalStatus") == F.lit("Disconnected")) | (F.col("nextStatus") == F.lit("Connected")), 1).otherwise(2))
        
display(ranked)
#df.withColumn('flag', F.when((F.col("a") == F.lit(2)) | (F.col("b") <= F.lit(2)), 1).otherwise(2)).show()

#df2=df1.withColumn("rank", rank().over(Window.partitionBy('FQDN','operationalStatus').orderBy(asc('_time')))).where("fqdn='qadovpxy-ch2g-02o.email.comcast.net'")
#withColumn("rank", dense_rank().over(window)).\
#filter(col('nextStatus').isNotNull()  ) | (col('nextStatus') != col('operationalStatus'))


# COMMAND ----------

# MAGIC %md ###Test scenario###

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as fn

data = spark.sql("select * from alcyonia.appreto_tsf_msn_eject where y=2020 and m=7 and d=14 and h=21")
data1 = data.groupby("FQDN").agg(fn.collect_list(fn.create_map(
                fn.lit('event_time'), fn.date_format(fn.col('activity_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                fn.lit('action'), fn.col('operationalStatus'),
                fn.lit('lastSyncTime'), fn.date_format(fn.col('lastSyncTime'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                fn.lit('ID'), fn.col('ID'),
                fn.lit('policyID'), fn.col('policyID'),
                fn.lit('namespace'), fn.col('namespace'),
                fn.lit('topic'), fn.col('topic'))).alias('events'))


window = Window.partitionBy('FQDN').orderBy('activity_time')
ranked = data.\
        withColumn('nextStatus', fn.lead('operationalStatus').over(window)).\
        withColumn('flag', fn.when(((fn.col("operationalStatus") == fn.lit("Disconnected")) & (fn.col("nextStatus") == fn.lit("Connected"))) | (fn.col("operationalStatus") == fn.lit("Connected")) | (fn.col("operationalStatus") == fn.lit("Registered")), 1).otherwise(2)).\
        where("flag=2").\
        select(
            fn.concat_ws(':', 'FQDN', 'operationalStatus', fn.col('activity_time').cast('bigint')).alias('id'),
            fn.col('activity_time').alias('event_time'),
            fn.array('destinationIP').alias('dest_ip'),
            fn.array('sourceIP').alias('src_ip'),
            fn.array('FQDN').alias('src_host'),
            fn.format_string('%s is %s', 'FQDN', 'operationalStatus').alias('body'),
            fn.lit('low').alias('severity'),
            fn.create_map(
                fn.lit('event_time'), fn.date_format(fn.col('activity_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                fn.lit('action'), fn.col('operationalStatus'),
                fn.lit('FQDN'), fn.col('FQDN'),
                fn.lit('lastSyncTime'), fn.date_format(fn.col('lastSyncTime'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                fn.lit('ID'), fn.col('ID'),
                fn.lit('policyID'), fn.col('policyID'),
                fn.lit('namespace'), fn.col('namespace'),
                fn.lit('topic'), fn.col('topic')
            ).alias('data')
        )
#df1.alias('a').join(df2.alias('b'),col('b.id') == col('a.id')).select([col('a.'+xx) for xx in a.columns] + [col('b.other1'),col('b.other2')])

        
display(ranked.alias('a').join(data1.alias('b'), fn.col('b.FQDN') == fn.col('a.data.FQDN')).drop('FQDN'))
#display(ranked.where("flag=2"))
#where("operationalStatus not in ('Registered','Connected')") .\

# COMMAND ----------

# MAGIC %sql
# MAGIC --select FQDN,d, h, count(*), collect_list(operationalStatus) from alcyonia.appreto_tsf_msn_eject where y=2020 and m=7 group by 1,2,3 order by 4 desc
# MAGIC select * from alcyonia.appreto_tsf_msn_eject where y=2020 and m=7 and d=14 and h=21 and FQDN='qadovpxy-ch2g-02o.email.comcast.net'

# COMMAND ----------

display(ranked.where("FQDN = 'qadovpxy-ch2g-02o.email.comcast.net'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select metadata, d, count(*) from alcyonia.tp_appreto_prod_flatten where m=12 and metadata is not null
# MAGIC group by 1,2

# COMMAND ----------

df = spark.sql("select * from alcyonia.appreto_tsf_msn_eject where y=2020 and m=12 and metadata is not null")
display(df)

# COMMAND ----------

df2 = df.where("topic = 'comcast.tp.appreto.prod'"). \
        withColumn('nextStatus', fn.lead('operationalStatus').over(Window.partitionBy('FQDN').orderBy('activity_time'))).\
        where("operationalStatus not in ('Registered','Connected')") .\
        withColumn('flag', fn.when((fn.col("operationalStatus") == fn.lit("Disconnected")) & (fn.col("nextStatus") == fn.lit("Connected")), 1).otherwise(2)).\
        where("flag=2").\
        select(
            fn.concat_ws(':', 'FQDN', 'operationalStatus', fn.col('activity_time').cast('bigint')).alias('id'),
            fn.col('activity_time').alias('event_time'),
            fn.array('destinationIP').alias('dest_ip'),
            fn.array('sourceIP').alias('src_ip'),
            fn.array('FQDN').alias('src_host'),
            fn.format_string('%s is %s', 'FQDN', 'operationalStatus').alias('body'),
            fn.lit('low').alias('severity'),
            fn.create_map(
                fn.lit('event_time'), fn.date_format(fn.col('activity_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                fn.lit('action'), fn.col('operationalStatus'),
                fn.lit('FQDN'), fn.col('FQDN'),
                fn.lit('ID'), fn.col('ID'),
                fn.lit('policyID'), fn.col('policyID'),
                fn.lit('namespace'), fn.col('namespace'),
                fn.lit('Zone'), fn.split(fn.col('namespace'),'/')[2],
                fn.lit('Tenant'), fn.split(fn.col('namespace'),'/')[3],
                fn.lit('Rail'), fn.split(fn.col('namespace'),'/')[4],
                fn.lit('metadata'), fn.to_json(fn.col('metadata')),
                fn.lit('lastSyncTime'), fn.date_format(fn.col('lastSyncTime'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                fn.lit('topic'), fn.col('topic')
            ).alias('data')
        )

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC select FQDN,
# MAGIC        activity_time, 
# MAGIC        operationalStatus,
# MAGIC        destinationIP,
# MAGIC        sourceIP,
# MAGIC        lastSyncTime,
# MAGIC        ID, 
# MAGIC        policyID, 
# MAGIC        namespace,
# MAGIC        metadata,
# MAGIC        topic,
# MAGIC        y, m, d, h
# MAGIC        from (
# MAGIC            SELECT *, dense_rank() OVER(PARTITION BY FQDN ORDER BY activity_time DESC) as rank
# MAGIC            FROM alcyonia.appreto_tsf_msn_eject where topic = 'comcast.tp.appreto.prod') tmp
# MAGIC        WHERE tmp.rank = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from alcyonia.alerts where y=2021 and m=2 and d=8 and id like '%Aporeto_TSF_MSN%' and h=14

# COMMAND ----------

# MAGIC %sql
# MAGIC select metadata, metadata[0]
# MAGIC --,TRANSFORM(metadata, (e, i) -> e.`@cloud:aws:availabilityzone`) 
# MAGIC from alcyonia.appreto_tsf_msn_eject where y=2021 and m=2 and d=8 and h=14

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC map_from_entries(TRANSFORM(TRANSFORM(metadata, (item, i) -> SPLIT(item, "=", 2)), (item2, i) -> STRUCT(item2[0], item2[1]))) as metadata1,
# MAGIC metadata
# MAGIC from alcyonia.tp_appreto_prod_flatten where y=2021 and m=2 and d=8 and h=14 and metadata is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC map_from_entries(TRANSFORM(TRANSFORM(metadata, (item, i) -> SPLIT(item, "=")), (item2, i) -> STRUCT(item2[0], item2[1]))) as metadata1, metadata
# MAGIC from alcyonia.tp_appreto_prod_flatten where y=2021 and m=2 and d=8 and h=14 and metadata is not null

# COMMAND ----------

display(spark.read.json("s3a://secure-backwaters/comcast.tp.appreto.prod/Year=2021/Month=02/Day=08/Hr=14/ls.s3.bcaa3ae4-75cd-4597-8c7c-576c74c46e90.2021-02-08T14.55.part11.txt.gz").where("metadata is not null"))

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as fn
df2 = spark.sql("select * from alcyonia.appreto_tsf_msn_eject where y=2021 and m=2 and d=8").\
          where("topic = 'comcast.tp.appreto.prod'").\
          groupby("FQDN").\
          agg(fn.collect_list(fn.create_map(
                  fn.lit('event_time'), fn.date_format(fn.col('activity_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                  fn.lit('action'), fn.col('operationalStatus'),
                  fn.lit('ID'), fn.col('ID'),
                  fn.lit('metadata'), fn.to_json(fn.col('metadata')),
                  fn.lit('Region'), TRANSFORM(NetworkAccesses, (e, i) -> e.RemoteAddress),
                  fn.lit('policyID'), fn.col('policyID'),
                  fn.lit('namespace'), fn.col('namespace'),
                  fn.lit('lastSyncTime'), fn.date_format(fn.col('lastSyncTime'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))).alias('events'))
display(df2)

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as fn
df2 = spark.sql("select * from alcyonia.appreto_tsf_msn_eject where y=2021 and m=2 and d=8").\
          where("topic = 'comcast.tp.appreto.prod'").\
          groupby("FQDN").\
          agg(fn.collect_list(fn.create_map(
                  fn.lit('event_time'), fn.date_format(fn.col('activity_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                  fn.lit('action'), fn.col('operationalStatus'),
                  fn.lit('ID'), fn.col('ID'),
                  fn.lit('metadata'), fn.to_json(fn.col('metadata')),
                  fn.lit('policyID'), fn.col('policyID'),
                  fn.lit('namespace'), fn.col('namespace'),
                  fn.lit('lastSyncTime'), fn.date_format(fn.col('lastSyncTime'), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))).alias('events'))
display(df2)