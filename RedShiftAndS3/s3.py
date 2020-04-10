import csv, ast, psycopg2
import boto3, pathlib
import time
import os
from datetime import datetime

connection = psycopg2.connect(
    host= 'hostname',
    user = 'username',
    port = 'port',
    password = '******',
    dbname='dbname'
)
cur = connection.cursor()
startTime =  datetime.now()
standardization_result = os.system("python Standardization/Skyline_Capstone.py")
# standardization_result
cur.execute("begin;")

def saveThepickDataAndSendToS3():
    s3 = boto3.client('s3', aws_access_key_id='ACCESS_KEY', aws_secret_access_key='SECRET_ACCESS_KEY')
    path = list(pathlib.Path('C:/Users/Hamzah.Quaraish/Desktop/capstone/Standardization/Skyline_Final.csv').glob('*.csv'))[0].name
    path = 'C:/Users/Hamzah.Quaraish/Desktop/capstone/Standardization/Skyline_Final.csv/'+path
    s3.upload_file(path, 's3lab-hamzah', 'pickup_data.csv')
    copy_command = ("""copy capstone_hamzah.pickup_data from 's3://s3lab-hamzah/pickup_data.csv' 
credentials 'aws_access_key_id="ACCESS_KEY";aws_secret_access_key="ACCESS__secret_KEY"'
ignoreheader 1
region 'us-east-2'
delimiter ','
MAXERROR 2;""")
    cur.execute("truncate table capstone_hamzah.pickup_data")
    cur.execute(copy_command)
saveThepickDataAndSendToS3()
endTime = datetime.now()
if 0==standardization_result:
    cur.execute("insert into capstone_hamzah.Audit values (%s, %s, %s, %s)", (1,startTime.strftime("%d/%m/%Y %H:%M:%S"), endTime.strftime("%d/%m/%Y %H:%M:%S"), "TRUE"))
else:
    print("failed")
    cur.execute("insert into capstone_hamzah.Audit values (%s, %s, %s, %s)", (1,startTime.strftime("%d/%m/%Y %H:%M:%S"), endTime.strftime("%d/%m/%Y %H:%M:%S"), "FALSE (Standardization"))
print("THE STANDARDIZATION TOOK: %s"% (endTime - startTime))


# (job_id, job_start_time, job_end_time, job_completed)
tweetStartTime = datetime.now()
tweet_result = os.system("python Tweet/TweetStreaming.py")
# os.system("python Tweet/TweetStreaming.py")
def sendTheTweets():
    s3 = boto3.client('s3', aws_access_key_id='ACCESS_KEY', aws_secret_access_key='SECRET_ACCESS_KEY')
    # df.coalesce(1).write.csv('prod_update', header=True)
    path = list(pathlib.Path('C:/Users/Hamzah.Quaraish/Desktop/capstone/Tweet').glob('*.csv'))[0].name
    path = 'C:/Users/Hamzah.Quaraish/Desktop/capstone/Tweet/'+path
    s3.upload_file(path, 's3lab-hamzah', 'tweet_analysis.csv')
    copy_command = ("""copy capstone_hamzah.Tweet_Analysis from 's3://s3lab-hamzah/tweet_analysis.csv' 
credentials 'aws_access_key_id="ACCESS_KEY";aws_secret_access_key="ACCESS__secret_KEY"'
ignoreheader 1
region 'us-east-2'
delimiter ',';""")
    cur.execute("truncate table capstone_hamzah.Tweet_Analysis")
    cur.execute(copy_command)

sendTheTweets()
tweetEndTime = datetime.now()
if 0==tweet_result:
    cur.execute("insert into capstone_hamzah.Audit values (%s, %s, %s, %s)", (2,tweetStartTime.strftime("%d/%m/%Y %H:%M:%S"), tweetEndTime.strftime("%d/%m/%Y %H:%M:%S"), "TRUE"))
else:
    print("failed")
    cur.execute("insert into capstone_hamzah.Audit values (%s, %s, %s, %s)", (2,tweetStartTime.strftime("%d/%m/%Y %H:%M:%S"), tweetEndTime.strftime("%d/%m/%Y %H:%M:%S"), "FALSE (Tweet)"))

print("THE tweets TOOK: %s"% (tweetEndTime - tweetStartTime))


scdStartTime = datetime.now()
# print("here two")

scd_result = os.system("python SCD/scd.py")
# os.system("python SCD/scd.py")
print("here lol")
def sendPickupFactWithKeys():
    s3 = boto3.client('s3', aws_access_key_id='ACCESS_KEY', aws_secret_access_key='SECRET_ACCESS_KEY')
    # df.coalesce(1).write.csv('prod_update', header=True)
    path = list(pathlib.Path('C:/Users/Hamzah.Quaraish/Desktop/capstone/FctType2_withKeys_hamzah.csv').glob('*.csv'))[0].name
    path = 'C:/Users/Hamzah.Quaraish/Desktop/capstone/FctType2_withKeys_hamzah.csv/'+path
    s3.upload_file(path, 's3lab-hamzah', 'pickup_data_withKeys.csv')
    copy_command = ("""copy capstone_hamzah.pickup_data_withkeys from 's3://s3lab-hamzah/pickup_data_withKeys.csv' 
credentials 'aws_access_key_id="ACCESS_KEY";aws_secret_access_key="ACCESS__secret_KEY"'
ignoreheader 1
region 'us-east-2'
delimiter ',';""")
    cur.execute("truncate table capstone_hamzah.pickup_data_withkeys")
    cur.execute(copy_command)

def sendUpdated():
    s3 = boto3.client('s3', aws_access_key_id='ACCESS_KEY', aws_secret_access_key='SECRET_ACCESS_KEY')
    # df.coalesce(1).write.csv('prod_update', header=True)
    path = list(pathlib.Path('C:/Users/Hamzah.Quaraish/Desktop/capstone/driverFinal_dim_Type2.csv').glob('*.csv'))[0].name
    path = 'C:/Users/Hamzah.Quaraish/Desktop/capstone/driverFinal_dim_Type2.csv/'+path
    s3.upload_file(path, 's3lab-hamzah', 'driver_dim_updated.csv')
    copy_command = ("""copy capstone_hamzah.driver_dim_updated from 's3://s3lab-hamzah/driver_dim_updated.csv' 
credentials 'aws_access_key_id="ACCESS_KEY";aws_secret_access_key="ACCESS__secret_KEY"'
ignoreheader 1
region 'us-east-2'
delimiter ',';""")
    cur.execute("truncate table capstone_hamzah.driver_dim_updated")
    cur.execute(copy_command)
print("here")
sendPickupFactWithKeys()
sendUpdated()
scdEndTime = datetime.now()
if 0==scd_result:
    cur.execute("insert into capstone_hamzah.Audit values (%s, %s, %s, %s)", (3,scdStartTime.strftime("%d/%m/%Y %H:%M:%S"), scdEndTime.strftime("%d/%m/%Y %H:%M:%S"), "TRUE"))
else:
    print("failed")
    cur.execute("insert into capstone_hamzah.Audit values (%s, %s, %s, %s)", (3,scdStartTime.strftime("%d/%m/%Y %H:%M:%S"), scdEndTime.strftime("%d/%m/%Y %H:%M:%S"), "FALSE (SCD)"))




print("THE SCD TYPE 2 TOOK: %s"% (scdEndTime - scdStartTime))

AnotherStartTime = datetime.now()
# os.system("python Standardization/Skyline_Capstone.py")
# subprocess.call("[ACTIVEDASH/*]")
def sendAnotherSourceAndJoin():
    s3 = boto3.client('s3', aws_access_key_id='ACCESS_KEY', aws_secret_access_key='SECRET_ACCESS_KEY')
    # df.coalesce(1).write.csv('prod_update', header=True)
    # path = list(pathlib.Path('C:/Users/Hamzah.Quaraish/Desktop/capstone.csv').glob('*.csv'))[0].name
    path = 'C:/Users/Hamzah.Quaraish/Desktop/capstone/uber.csv'
    s3.upload_file(path, 's3lab-hamzah', 'another_source.csv')
    copy_command = ("""copy capstone_hamzah.pickupData_with_anotherSource from 's3://s3lab-hamzah/another_source.csv' 
credentials 'aws_access_key_id="ACCESS_KEY";aws_secret_access_key="ACCESS__secret_KEY"'
ignoreheader 1
region 'us-east-2'
delimiter ',';""")
    # cur.execute("truncate table capstone_hamzah.driver_dim_updated")
    cur.execute(copy_command)
    cur.execute("INSERT INTO capstone_hamzah.pickupData_with_anotherSource (SELECT * FROM capstone_hamzah.pickup_data WHERE source NOT IN (SELECT source FROM capstone_hamzah.pickupData_with_anotherSource));")
sendAnotherSourceAndJoin()
AnotherEndTime = datetime.now()

cur.execute("insert into capstone_hamzah.Audit values (%s, %s, %s, %s)", (4,AnotherStartTime.strftime("%d/%m/%Y %H:%M:%S"), AnotherEndTime.strftime("%d/%m/%Y %H:%M:%S"), "TRUE"))
print('The joining of another source took:  %s' % (AnotherEndTime - AnotherStartTime))


print("THE ENTIRE TIME TOOK : %s" % (AnotherEndTime - startTime))







cur.execute("commit;")

cur.close()
connection.close()






