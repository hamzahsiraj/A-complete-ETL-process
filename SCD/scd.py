#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from pyspark.sql import SparkSession, Window
import pandas as pd
import pandas_redshift as pr


# In[2]:


# pip install pandas_redshift
spark = SparkSession.builder.getOrCreate()


# In[3]:


pr.connect_to_redshift(
    host= 'hostname',
    user = 'username',
    port = 'port',
    password = '******',
    dbname='dbname'
)


# In[1]:


sql_statement = 'select * from capstone_hamzah.Driver_dim'
sql_statement1 = 'select * from capstone_hamzah.driver_feed'
sql_statement2 = 'select * from capstone_hamzah.shiftTime_dim'
sql_statement3 = 'select * from capstone_hamzah.Coverage_dim'
sql_statement4 = 'select * from capstone_hamzah.Company_dim'
sql_statement5 = 'select * from capstone_hamzah.pickup_data'


# In[5]:


df = pr.redshift_to_pandas(sql_statement)
df1 = pr.redshift_to_pandas(sql_statement1)
df2 = pr.redshift_to_pandas(sql_statement2)
df3 = pr.redshift_to_pandas(sql_statement3)
df4 = pr.redshift_to_pandas(sql_statement4)
df5 = pr.redshift_to_pandas(sql_statement5)


# In[6]:


import pyspark.sql.functions as F
import pyspark.sql.types as T


# In[7]:


#These are the five dataframes
driverdim = spark.createDataFrame(df)
driverfeed = spark.createDataFrame(df1)
shiftTime = spark.createDataFrame(df2)
coverages = spark.createDataFrame(df3)
companys = spark.createDataFrame(df4)
pickupFact = spark.createDataFrame(df5)


# In[8]:


# driverfeed.show()


# In[9]:


from datetime import datetime, timedelta
maxx = driverdim.agg({"driverkey": "max"}).head(1)[0][0]


# In[10]:


change = driverdim.join(driverfeed, (driverdim.drivername==driverfeed.drivername), how='inner')
change = change.select([driverdim.driverrecordkey, driverdim.driverkey, driverdim.drivername, driverdim.shifttimekey, driverdim.coveragekey, driverdim.companykey, driverdim.hiredate, driverdim.firedate, driverdim.effectivedate, driverdim.loaddate, driverdim.expireddate, driverdim.current_record])
nochange = driverdim.subtract(change)


# In[11]:


# nochange.describe().show(100)


# In[12]:


def removeTrailingAndLeadingSpaces(df):
    for x in df.columns:
         df = df.withColumn(x, F.trim(x))
    return df


# In[13]:


def dateMinusOne(s):
    date = datetime.strptime(s, "%m/%d/%Y")
    modified_date = date - timedelta(days=1)
    return datetime.strftime(modified_date, "%m/%d/%Y")
udfModifyTime = F.udf(dateMinusOne, T.StringType())


# In[14]:


def firedTypeTwo(driverdim, driverfeed):
    drivedimFired = driverdim.join(driverfeed, (driverdim.drivername == driverfeed.drivername) & (driverfeed.action == 'FIRED'), how='inner')
    drivedimFired = drivedimFired.select([driverdim.driverrecordkey, driverdim.driverkey, driverdim.drivername, driverdim.shifttimekey, driverdim.coveragekey, driverdim.companykey, driverdim.hiredate, driverdim.firedate, driverdim.effectivedate, driverdim.loaddate, driverdim.expireddate, driverdim.current_record, driverfeed.effdt, driverfeed.filedt])
    drivedimFired = drivedimFired.withColumn('expireddate', udfModifyTime('effdt'))
#     drivedimFired1 = drivedimFired.withColumn('expireddate', F.date_sub(F.to_date(F.col('effdt'), 'M/d/yyyy'), 1))
#     drivdimFired1 = drivedimFired.withColumn('expireddate', F.date_format('expireddate', 'M/d/yyyy'))
    drivedimFired = drivedimFired.withColumn('current_record', F.when(F.col('current_record') == 1, 0)).persist()
    newRecords = drivedimFired.select(['driverrecordkey','driverkey', 'drivername', 'shifttimekey', 'coveragekey', 'companykey', 'hiredate', 'firedate', 'effectivedate', 'loaddate', 'expireddate', 'current_record', 'effdt', 'filedt'])
    newRecords = newRecords.withColumn('firedate', F.lit(F.col('effdt')))
    newRecords = newRecords.withColumn('effectivedate', F.lit(F.col('effdt')))
    newRecords = newRecords.withColumn('loaddate', F.lit(F.col('filedt')))
    newRecords = newRecords.withColumn('current_record', F.lit(1))
    newRecords = newRecords.withColumn('expireddate', F.lit('12/31/9999'))
    newRecords = newRecords.withColumn('driverrecordkey', F.lit('999'))
#     return drivedimFired.unionAll(newRecords)
    return newRecords
driverFiredAndAdded = firedTypeTwo(driverdim, driverfeed)
driverFiredAndAdded = driverFiredAndAdded.select(['driverrecordkey', 'driverkey','drivername', 'shifttimekey', 'coveragekey','companykey','hiredate', 'firedate', 'effectivedate', 'loaddate', 'expireddate', 'current_record'])


# In[15]:


# lol = firedTypeTwo(driverdim, driverfeed)


# In[16]:


def firedTypeTwo(driverdim, driverfeed, shiftTime, coverages, companys):
    

    drivedimChanged = driverdim.join(driverfeed, (driverdim.drivername == driverfeed.drivername) & (driverfeed.action == 'CHANGE'), how='inner').join(shiftTime, (driverfeed.beginshift==shiftTime.starttime) & (driverfeed.endshift==shiftTime.endtime), how='inner').join(coverages, (driverfeed.coverage==coverages.coverage), how='inner').join(companys, (driverfeed.source==companys.company), how='inner')
    drivedimChanged = drivedimChanged.select([driverdim.driverrecordkey, driverdim.driverkey, driverdim.drivername,shiftTime.shifttimekey, coverages.coveragekey,companys.companykey, driverdim.hiredate, driverdim.firedate, driverdim.effectivedate, driverdim.loaddate, driverdim.expireddate, driverdim.current_record, driverfeed.effdt, driverfeed.filedt])
    drivedimChanged = drivedimChanged.withColumn('expireddate', udfModifyTime('effdt'))   
    drivedimChanged = drivedimChanged.withColumn('current_record', F.when(F.col('current_record') == 1, 0)).persist()
    changedRecords = drivedimChanged.select(['driverrecordkey','driverkey', 'drivername', 'shifttimekey', 'coveragekey', 'companykey', 'hiredate', 'firedate', 'effectivedate', 'loaddate', 'expireddate', 'current_record', 'effdt', 'filedt'])
    changedRecords = changedRecords.withColumn('effectivedate', F.lit(F.col('effdt')))
    changedRecords = changedRecords.withColumn('loaddate', F.lit(F.col('filedt')))
    changedRecords = changedRecords.withColumn('current_record', F.lit(1))
    changedRecords = changedRecords.withColumn('firedate', F.lit('12/31/9999'))
    changedRecords = changedRecords.withColumn('expireddate', F.lit('12/31/9999'))
    return drivedimChanged.unionAll(changedRecords)

driverChangedAndAdded = firedTypeTwo(driverdim, driverfeed, shiftTime, coverages, companys)
driverChangedAndAdded = driverChangedAndAdded.select(['driverrecordkey','driverkey', 'drivername', 'shifttimekey', 'coveragekey', 'companykey', 'hiredate', 'firedate', 'effectivedate', 'loaddate', 'expireddate', 'current_record']).persist()


# In[17]:


# driverChangedAndAdded.show()


# In[18]:


def hiredTypeTwo(driverfeed, shiftTime, coverages, companys):
    drivedimHired = driverfeed.join(shiftTime, (driverfeed.action == 'HIRED') & (driverfeed.beginshift==shiftTime.starttime) & (driverfeed.endshift==shiftTime.endtime), how='inner').join(coverages, (driverfeed.coverage==coverages.coverage), how='inner').join(companys, (driverfeed.source==companys.company), how='inner')
    drivedimHired = drivedimHired.withColumn('key', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    drivedimHired = drivedimHired.withColumn('driverkey', maxx+drivedimHired.key)
    drivedimHired = drivedimHired.withColumn('effectivedate', F.lit(F.col('effdt')))
    drivedimHired = drivedimHired.withColumn('hiredate', F.lit(F.col('effdt')))
    drivedimHired = drivedimHired.withColumn('loaddate', F.lit(F.col('filedt')))
    drivedimHired = drivedimHired.withColumn('firedate', F.lit('12/31/9999'))
    drivedimHired = drivedimHired.withColumn('expireddate', F.lit('12/31/9999'))
    drivedimHired = drivedimHired.withColumn('current_record', F.lit(1))
    drivedimHired = drivedimHired.withColumn('driverrecordkey', F.lit(999))
    drivedimHired = drivedimHired.select('driverrecordkey', 'driverkey', driverfeed.drivername,shiftTime.shifttimekey, coverages.coveragekey,companys.companykey,'hiredate', 'firedate', 'effectivedate', 'loaddate', 'expireddate', 'current_record')
    return drivedimHired
driverHired = hiredTypeTwo(driverfeed, shiftTime, coverages, companys).persist()




# driverHired.count()


# In[20]:


# driverFiredAndAdded.count()


# In[21]:


# driverChangedAndAdded.count()


# In[22]:


# nochange.count()


# In[23]:


final_table = nochange.union(driverFiredAndAdded).union(driverChangedAndAdded).union(driverHired).persist()


# In[24]:


final_table = final_table.orderBy('driverkey', ascending=True)


# In[25]:


final_table = final_table.withColumn('driverrecordkey', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))


# In[61]:


print(pickupFact.show())


# In[54]:


# pickupFactTime.show()


# In[64]:


def standardizeTime(df):
    pickupFactTime = df.withColumn('datetime', F.expr("substring(datetime, 1, length(datetime)-6)"))
    pickupFactTime = removeTrailingAndLeadingSpaces(pickupFactTime)
    pickupFactTime = pickupFactTime.withColumn('datetime', pickupFactTime.datetime.substr(-5, 5))
    pickupFactTime = removeTrailingAndLeadingSpaces(pickupFactTime)
    pickupFactTime = pickupFactTime.withColumn('datetime', F.concat(F.lit('0'),pickupFactTime.datetime))
    pickupFactTime = pickupFactTime.withColumn('datetime', pickupFactTime.datetime.substr(-5, 5))
    
    return pickupFactTime
pickupFactTime = standardizeTime(pickupFact)


# In[65]:


def checkDates(strCheck):
    
    if (strCheck > '00:00') & (strCheck <= '07:59'):
        return 1
    elif (strCheck >= '08:00') & (strCheck <= '15:59'):
        return 2
    elif (strCheck >= '16:00') & (strCheck <= '23:59'):
        return 3
    return -1

udf_time = F.udf(checkDates, T.IntegerType())
pickupFactTime = pickupFactTime.withColumn('SKey', udf_time(F.col('datetime')))


# In[66]:


def checkCoverge(string):
    coverage = {'BRONX': 1, 'BROOKLYN': 2, 'JAMAICA': 3, 'EAST ELMHURST': 4, 'MANHATTAN': 5, 'NEW YORK CITY': 6, 'NEWARK': 7, 'Other':8}
    for key in coverage:
        if key == string:
            return coverage.get(key)
    return -1
udf_coverage = F.udf(checkCoverge, T.IntegerType())
pickupFactTime = pickupFactTime.withColumn('CoveKey', udf_coverage(F.col('city')))


# In[67]:


def checkCompany(string):
    company = {'DIAL7': 1, 'FIRSTCLASS': 2, 'LYFT': 3, 'SKYLINE':4 }
    for key in company:
        if key == string:
            return company.get(key)
        
udf_company = F.udf(checkCompany, T.IntegerType())
pickupFactTime = pickupFactTime.withColumn('CompKey', udf_company(F.col('source')))


# In[68]:


pickupFactTime = pickupFactTime.withColumn("DriverKey", F.lit(1))
pickupFactTime = pickupFactTime.withColumn("DriverRecordKey", F.lit(1))
pickupFactTime = pickupFactTime.join(final_table, (final_table.current_record == 1) &(final_table.shifttimekey==pickupFactTime.SKey)&(final_table.coveragekey==pickupFactTime.CoveKey)&(final_table.companykey==pickupFactTime.CompKey), how='inner')


# In[69]:


pickupFactTime = pickupFactTime.select([final_table.driverrecordkey, final_table.driverkey, 'datetime', 'street_address', 'city', 'state', 'source', 'latitude', 'longitude']).persist()





# In[74]:


pickupFactTime.coalesce(1).write.csv('FctType2_withKeys_hamzah.csv', header=True)


# In[35]:


final_table.coalesce(1).write.csv("driverFinal_dim_Type2.csv")


# In[36]:


pr.close_up_shop()


# In[ ]:




