#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from geopy.geocoders import Nominatim


# In[2]:


from pyspark.sql import SparkSession


# In[3]:


sparkLab = SparkSession.builder.getOrCreate()


# In[5]:


fileName = "Standardization/SkylineLab.csv"


# In[6]:


def readFile(fileName):
    if not ".csv" in fileName:
        fileName += ".csv"
    return fileName
    


# In[7]:


df = sparkLab.read.csv(fileName, header=True, inferSchema=True)


# In[9]:


import pyspark.sql.functions as F
import pyspark.sql.types as T


# In[10]:


def capitalizeColumnValues(df):
    for col in df.columns:
        df = df.withColumn(col, F.upper(F.col(col)))
#     df = df.withColumn("PICK UP ADDRESS",F.upper(F.col("PICK UP ADDRESS")))
    return df
df = capitalizeColumnValues(df)


# In[11]:


def standardizeDate(df):  #Change the format of the date
    df = df.withColumn("Date", F.regexp_replace(F.col('Date'), "/", "-"))
    return df
            
df = standardizeDate(df)    


# In[12]:


#If the date and time exist separately, join them in one column
def checkIfDateExistsSeparately(df):   
   if 'Date' in df.columns:
       if 'Time' in df.columns:
           df = df.withColumn('Datetime', F.concat(F.col('Date'), F.lit(' '), F.col('Time')))
           columsToDrop = ['Date', 'Time']
           df = df.drop(*columsToDrop)
   else:
       print("The columns date and time don't exist separately")
       return df
   return df
df = checkIfDateExistsSeparately(df)


# In[13]:


def removeTrailingAndLeadingSpaces(df):
    for x in df.columns:
         df = df.withColumn(x, F.trim(x))
    return df
df = removeTrailingAndLeadingSpaces(df)   


# In[14]:


def createColumnsAndSelect(df):
    df = df.withColumn("Source", F.lit("SKYLINE"))
    df = df.withColumn("City", F.lit(" "))
    df = df.withColumn("State", F.lit(" "))
    df = df.select(['Datetime', 'Street_Address', 'City', 'City_State', 'State', 'Source'])
    return df
df = createColumnsAndSelect(df)


# In[16]:


def fixStreetAbbv(df):
    df = df.withColumn("Street_Address", F.regexp_replace(F.col('Street_Address'), " BL$", " BLVD"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " AV$", " AVE"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " AVENUE$", " AVE"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " COURT$", " CT"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " STREET$", " ST"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " LANE$", " LN"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " ROAD$", " RD"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " DRIVE$", " DR"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " HY$", " HWY"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " HY$", " HWY"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " PZ$", " PLZ"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " TP$", " TPKE"))
    
    return df
df = fixStreetAbbv(df)


# In[17]:


def fixTheNonNYAddress(a, b):
    if b[-2:] == 'NJ' or b[-2:] == 'CT' or b[-2:] == 'LI' or b[-2:] == 'PA':
        a =  a + " " +b[:-2]
        
    return a
ny_udf = F.udf(fixTheNonNYAddress, T.StringType())
df = df.withColumn('Street_Address',ny_udf('Street_Address', 'City_State'))


# In[18]:


def separateState(df):
    df = df.withColumn('City', F.substring(F.col('City_State'), -2, 2))
    return df
df = separateState(df)


# In[19]:


df = removeTrailingAndLeadingSpaces(df)


# In[20]:


def defineState(df):
    df = df.withColumn('State', F.when((F.col('City') == ('M')) | (F.col('City') == ('QU')) | (F.col('City') == ('BX')) | (F.col('City') == ('CA')) | (F.col('City') == ('ST')) | (F.col('City') == ('WE')) | (F.col('City') == ('BK')) | (F.col('City') == ('FK')) | (F.col('City') == ('AG')), 'NEW YORK').otherwise(F.col('State')))
    df = df.withColumn('State', F.when(F.col('City') == ('PA'), 'PENNSYLVANIA').otherwise(F.col('State')))
    df = df.withColumn('State', F.when(F.col('City') == ('CT'), 'CONNECTICUT').otherwise(F.col('State')))
    df = df.withColumn('State', F.when(F.col('City') == ('LI'), 'LONG ISLAND').otherwise(F.col('State')))
    df = df.withColumn('State', F.when((F.col('City') == ('NJ')) | (F.col('City') == ('WK')), 'NEW JERSEY').otherwise(F.col('State')))
    
    return df
df = defineState(df)


# In[21]:


def cityName(df):
    df = df.withColumn('City', F.when(F.col('City') == 'NJ', F.regexp_replace(F.col('City'), "NJ", "")).otherwise(F.col('City')))
    df = df.withColumn('City', F.when(F.col('City') == 'LI', F.regexp_replace(F.col('City'), "LI", "")).otherwise(F.col('City')))
    df = df.withColumn('City', F.when(F.col('City') == 'PA', F.regexp_replace(F.col('City'), "PA", "")).otherwise(F.col('City')))
    df = df.withColumn('City', F.when(F.col('City') == 'CT', F.regexp_replace(F.col('City'), "CT", "")).otherwise(F.col('City')))
    df = df.withColumn('City', F.when(F.col('City') == 'WE', F.regexp_replace(F.col('City'), "WE", "")).otherwise(F.col('City')))
    df = df.withColumn('City', F.regexp_replace(F.col('City'), "M", "MANHATTAN"))
    df = df.withColumn('City', F.regexp_replace(F.col('City'), "BK", "BROOKLYN"))
    df = df.withColumn('City', F.regexp_replace(F.col('City'), "BX", "BRONX"))
    df = df.withColumn('City', F.regexp_replace(F.col('City'), "QU", "QUEENS"))
#     df = df.withColumn('City', F.regexp_replace(F.col('City'), "NJ", ""))
    
    
    return df
df = cityName(df)


# In[22]:


def fixTheAirportAddress(df):
    df = df.withColumn('Street_Address', F.when(F.col('City_State') == 'NWK', '3 BREWSTER').otherwise(F.col('Street_Address')))
    df = df.withColumn('Street_Address', F.when(F.col('City_State') == 'JFK', '148-18 134TH ST').otherwise(F.col('Street_Address')))
    df = df.withColumn('Street_Address', F.when(F.col('City_State') == 'LAG', '102-05 DITMARS BLVD').otherwise(F.col('Street_Address')))
    df = df.withColumn('City_State', F.regexp_replace(F.col('City_State'), "NWK", "NEWARK"))
    
    df = df.withColumn('City_State', F.regexp_replace(F.col('City_State'), "JFK", " JAMAICA"))
  
    df = df.withColumn('City_State', F.regexp_replace(F.col('City_State'), "LAG", " EAST ELMHURST"))
    
    df = df.withColumn('City', F.regexp_replace(F.col('City'), "FK", "JAMAICA"))
    df = df.withColumn('City', F.regexp_replace(F.col('City'), "WK", "NEWARK"))
    df = df.withColumn('City', F.regexp_replace(F.col('City'), "AG", "EAST ELMHURST"))  
    return df
df = fixTheAirportAddress(df)


# In[23]:


def fixTheCityWithNonNYState(df):
    df = df.withColumn('City', F.when(F.col('State') == 'NEW JERSEY', '').otherwise(F.col('City')))
    df = df.withColumn('City', F.when(F.col('State') == 'CONNECTICUT', '').otherwise(F.col('City')))
    df = df.withColumn('City', F.when(F.col('State') == 'PENNSYLVANIA', '').otherwise(F.col('City')))
    df = df.withColumn('City', F.when(F.col('State') == 'LONG ISLAND', '').otherwise(F.col('City')))
    
    return df


# In[24]:


df = removeTrailingAndLeadingSpaces(df)


# In[25]:


df = fixTheCityWithNonNYState(df)


# In[26]:


df = df.select(['Datetime', 'Street_Address', 'City', 'State', 'Source'])


# In[27]:


def removeOnth(string):
    list1 = string.split()
    counter = 0
    for items in list1:
        if items == 'ON':
            list1[counter] = ''
            if (counter + 2 < len(list1)):
                    if not (list1[counter+1]).isalpha():
                        list1[counter+1] = ''
                    list1[counter+2] = ''
                    
        counter += 1
    return ' '.join(list1)
onth_udf = F.udf(removeOnth, T.StringType())
df = df.withColumn('Street_Address',onth_udf('Street_Address'))


# In[28]:


def fixStreetName(df):
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "FIRST", "1ST"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "SECOND", "2ND"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "THIRD", "3RD"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "FIFTH", "5TH"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "SIXTH", "6TH"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "SEVENTH", "7TH"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "EIGHTH", "8TH"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "NINTH", "9TH"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "TENTH", "10TH"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "ELEVENTH", "11TH"))
    df = df.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), "TWELFTH", "12TH"))

    return df


# In[29]:


df = fixStreetName(df)


# In[30]:


def capitalizeColulmns(df):
    for col in df.columns:
        df = df.withColumnRenamed(col, col.upper())
    return df

df = capitalizeColulmns(df)


# In[31]:


df = removeTrailingAndLeadingSpaces(df)



# In[2]:


# import findspark
# findspark.init()


# In[3]:


# from pyspark.sql import SparkSession


# In[4]:


sparkFirstLab = SparkSession.builder.getOrCreate()


# In[5]:


fileNameFirstClass = "Standardization/FirstClassLab.csv"


# In[6]:


def readFile(fileName):
    if not ".csv" in fileName:
        fileName += ".csv"
    return fileName
    


# In[7]:


dfFirstClass = sparkFirstLab.read.csv(fileNameFirstClass, header=True, inferSchema=True)


# In[8]:


# import pyspark.sql.functions as F
# import pyspark.sql.types as T


# In[9]:


def capitalizeColulmns(dfFirstClass):
    for col in dfFirstClass.columns:
        dfFirstClass = dfFirstClass.withColumnRenamed(col, col.upper())
    return dfFirstClass
dfFirstClass= capitalizeColulmns(dfFirstClass)


# In[10]:


def capitalizeColumnValues(dfFirstClass):
    for col in dfFirstClass.columns:
        dfFirstClass = dfFirstClass.withColumn(col, F.upper(F.col(col)))
#     dfFirstClass = dfFirstClass.withColumn("PICK UP ADDRESS",F.upper(F.col("PICK UP ADDRESS")))
    return dfFirstClass
dfFirstClass = capitalizeColumnValues(dfFirstClass)


# In[11]:


def standardizeDate(dfFirstClass):
    dfFirstClass = dfFirstClass.withColumn("DATE", F.regexp_replace(F.col('DATE'), "/", "-"))
    return dfFirstClass
dfFirstClass = standardizeDate(dfFirstClass)


# In[12]:


def checkIfDateExistsSeparately(dfFirstClass):
    if 'DATE' in dfFirstClass.columns:
        if 'TIME' in dfFirstClass.columns:
            dfFirstClass = dfFirstClass.withColumn('DATETIME', F.concat(F.col('DATE'), F.lit(' '), F.col('TIME')))
            columsToDrop = ['DATE', 'TIME']
            dfFirstClass = dfFirstClass.drop(*columsToDrop)
    else:
        print("The columns date and time don't exist separately")
        return dfFirstClass
    return dfFirstClass
dfFirstClass = checkIfDateExistsSeparately(dfFirstClass)


# In[13]:


def removeTrailingAndLeadingSpaces(dfFirstClass):
    for x in dfFirstClass.columns:
         dfFirstClass = dfFirstClass.withColumn(x, F.trim(x))
    return dfFirstClass

dfFirstClass = removeTrailingAndLeadingSpaces(dfFirstClass)


# In[14]:


dfFirstClass = dfFirstClass.withColumn("SOURCE", F.lit("FIRSTCLASS"))
dfFirstClass = dfFirstClass.withColumn("CITY", F.lit(" "))
dfFirstClass = dfFirstClass.withColumn("STATE", F.lit("NEW YORK"))
dfFirstClass= dfFirstClass.withColumnRenamed("PICK UP ADDRESS", "ADDRESS")
dfFirstClass = dfFirstClass.select(['DATETIME', 'ADDRESS', 'CITY', 'STATE', 'SOURCE'])


# In[15]:


def separateCity(dfFirstClass):
    dfFirstClass = dfFirstClass.withColumn('CITY', F.substring(F.col('ADDRESS'), -3, 3))
    return dfFirstClass
dfFirstClass = separateCity(dfFirstClass)


# In[16]:


def splitFunction(dfFirstClass):
    split = F.split(dfFirstClass['ADDRESS'], ',')
    dfFirstClass= dfFirstClass.withColumn('ADDRESS', F.when(dfFirstClass.ADDRESS.contains(','), split.getItem(0)).otherwise(F.col('ADDRESS')))
    
    
    return dfFirstClass
dfFirstClass = splitFunction(dfFirstClass)


# In[17]:


def emptyItOut(string):
    return ''
def takeSTuffOutAFterSub(string):
    list1 = string.split()
    flag = 0
    counter = 0
    for items in list1:
        if flag == 0:
            if (items == 'AVE') | (items == 'ST') | (items=='BLVD') | (items=='LANE') | (items=='LN') | (items == 'HWY') | (items=='TPKE') | (items=='TURNPIKE')| (items=='PL') | (items == 'PLAZA') | (items== 'DR'):
                flag = 1
                continue
        elif flag == 1:
            list1[counter+1] = emptyItOut(items)
        counter +=1
            
    return ' '.join(list1)
# takeSTuffOutAFterSub("THIS IS CRAZYAVE HIGHWAY 23 34 lamo yall LANE this is crazy")
subscript_udf = F.udf(takeSTuffOutAFterSub, T.StringType())
dfFirstClass = dfFirstClass.withColumn('ADDRESS', subscript_udf('ADDRESS'))


# In[18]:


def takeOut(string):
    return string.rsplit(' ', 1)[0]
add_udf = F.udf(takeOut, T.StringType())
dfFirstClass = dfFirstClass.withColumn('ADDRESS',add_udf('ADDRESS'))


# In[19]:


#LGA, JFK, EWR
def ifAny(s):
    l = s.split()
    if any('JFK' in s for s in l):
        return 1
    elif any('EWR' in s for s in l):
        return 2
    elif any('LGA' in s for s in l):
        return 3
    
def fixingAirports(string):
    list1 = string.split()
    for items in list1:
        if ifAny(items) == 1:#'JFK' in items:
            return '148-18 134TH ST'
        elif 'EWR' in items:
            return '3 BREWSTER RD'
        elif 'LGA' in items:
            return '102-05 DITMARS BLVD'
        return string
airport_udf = F.udf(fixingAirports, T.StringType())
dfFirstClass = dfFirstClass.withColumn('ADDRESS',airport_udf('ADDRESS'))


# In[20]:


def fixTheCityNames(dfFirstClass):
        dfFirstClass= dfFirstClass.withColumn('ADDRESS', F.regexp_replace(F.col('ADDRESS'), "NEW", " "))
        
        return dfFirstClass
dfFirstClass = fixTheCityNames(dfFirstClass)


# In[21]:


def cityName(dfFirstClass):
    dfFirstClass = dfFirstClass.withColumn('City', F.regexp_replace(F.col('City'), "ONX", "BRONX"))
    dfFirstClass = dfFirstClass.withColumn('City', F.regexp_replace(F.col('City'), "BX", "BRONX"))
    dfFirstClass = dfFirstClass.withColumn('City', F.when((F.col('City') == 'NYC') | (F.col('City') == 'NY') | (F.col('City') == 'ORK'), "NEW YORK CITY").otherwise(F.col('City')))
    dfFirstClass = dfFirstClass.withColumn('City', F.regexp_replace(F.col('City'), "QU", "QUEENS"))
    dfFirstClass = dfFirstClass.withColumn('City', F.regexp_replace(F.col('City'), "NY.", "YONKERS"))
    dfFirstClass = dfFirstClass.withColumn('City', F.regexp_replace(F.col('City'), "LYN", "BROOKLYN"))
    
    return dfFirstClass
dfFirstClass = cityName(dfFirstClass)


# In[22]:


def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)


def letsTrim(address):
    word = ' '
    counter = 0;
    if not hasNumbers(address):
        return address
    for v in address:
        if v.isdigit():
              word +=v
        else:
            if counter == 0:
                continue
            else:
                word +=v
        counter+=1
    if not word:
        return 'null'
        
    return word
my_udf = F.udf(letsTrim, T.StringType())
dfFirstClass = dfFirstClass.withColumn('ADDRESS',my_udf('ADDRESS'))         


# In[23]:


def takeTheNum(num):
    word = str(num)
    if str(num)[-1] == ('1'):
        if len(str(num)) > 1 and str(num)[-2] == '1':
            word +='th'
        else:
            word +='st'
    elif str(num)[-1] == ('2'):
        if len(str(num)) > 1 and str(num)[-2] == '1':
            word +='th'
        else:
            word +='nd'
    elif str(num)[-1] == ('3'):
        if len(str(num)) > 1 and str(num)[-2] == '1':
            word +='th'
        else:
            word +='rd'
            
    else:
        word+='th'
    return word
def addSubscripts(string):
    list1 = string.split()
    counter = 0
    for items in list1:
        if len(items) == 1:
            if (items == 'W') | (items == 'E') | (items == 'S') | (items == 'N'):
                if (counter + 1 < len(list1)):
                    if (list1[counter+1]).isdigit():
                        list1[counter+1] = takeTheNum(list1[counter+1])
#             if items == 'AVE' | items == 'ST'
        counter +=1       
    return ' '.join(list1)
sub_udf = F.udf(addSubscripts, T.StringType())
dfFirstClass = dfFirstClass.withColumn('ADDRESS',sub_udf('ADDRESS'))    


# In[24]:


def capitalizeColulmns(dfFirstClass):
    for col in dfFirstClass.columns:
        dfFirstClass = dfFirstClass.withColumnRenamed(col, col.upper())
    return dfFirstClass

dfFirstClass = capitalizeColulmns(dfFirstClass)


# In[25]:


dfFirstClass = dfFirstClass.withColumnRenamed("ADDRESS", "STREET_ADDRESS")


# In[26]:


dfFirstClass = removeTrailingAndLeadingSpaces(dfFirstClass)






# In[2]:


# import pyspark


# In[3]:


# from pyspark.sql import SparkSession


# In[4]:


dialLab = SparkSession.builder.getOrCreate()


# In[5]:


fileName = 'Standardization/DialLab.csv'


# In[6]:


def readFile(fileName):
    if not ".csv" in fileName:
        fileName += ".csv"
    return fileName
    


# In[7]:


dfDial = dialLab.read.csv(fileName, header=True, inferSchema=True)


# In[8]:


import pyspark.sql.functions as F
import pyspark.sql.types as T


# In[9]:


def capitalizeColulmns(dfDial):
    for col in dfDial.columns:
        dfDial = dfDial.withColumnRenamed(col, col.upper())
    return dfDial
dfDial= capitalizeColulmns(dfDial)


# In[10]:


def capitalizeColumnValues(dfDial):
    for col in dfDial.columns:
        dfDial = dfDial.withColumn(col, F.upper(F.col(col)))
#     dfDial = dfDial.withColumn("PICK UP ADDRESS",F.upper(F.col("PICK UP ADDRESS")))
    return dfDial
dfDial = capitalizeColumnValues(dfDial)


# In[11]:


def addSubscriptToAddress(string):
    #1st, 2nd, 3rd
    #11th, 12th, 13th,
    #21st, 22nd, 23rd
    #if ends from 4-0, then add the th.
    #if ends in 1
    num = ''
    word = ''
    counter = 0
    if not string:
        return string
    for x in string:
        if x.isdigit():
            word +=x
            num +=x
            counter +=1
        else:
            
            if counter > 0:
                if word.endswith('1'):
                    if len(num) > 1 and word[-2] == '1':
                        word +='th '
                        counter=0
                        continue
                    else:
                        word +='st '
                        counter=0
                        continue
                elif word.endswith('2'):
                    if len(num) > 1 and word[-2] == '1':
                        word +='th '
                        counter=0
                        continue
                    else:
                        word +='nd '
                        counter=0
                        continue
                elif word.endswith('3'):
                    if len(num) > 1 and word[-2] == '1':
                        word +='th '
                        counter=0
                        continue
                    else:
                        word +='rd '
                        counter=0
                        continue
                else:
                    word +='th '
                    counter=0
                    continue
            word+=x
                
            
                
    return word
# addSubscriptToAddress("1 lmao")
my_udf = F.udf(addSubscriptToAddress, T.StringType())
dfDial = dfDial.withColumn('STREET',my_udf('STREET')) 


# In[12]:


def standardizeDate(dfDial):
    dfDial = dfDial.withColumn("DATE", F.regexp_replace(F.col('DATE'), "/", "-"))
    return dfDial
dfDial = standardizeDate(dfDial)


# In[13]:


def checkIfDateExistsSeparately(dfDial):
    if 'DATE' in dfDial.columns:
        if 'TIME' in dfDial.columns:
            dfDial = dfDial.withColumn('DATETIME', F.concat(F.col('DATE'), F.lit(' '), F.col('TIME')))
            columsToDrop = ['DATE', 'TIME']
            dfDial = dfDial.drop(*columsToDrop)
    else:
        print("The columns date and time don't exist separately")
        return dfDial
    return dfDial
dfDial = checkIfDateExistsSeparately(dfDial)


# In[14]:


def removeTrailingAndLeadingSpaces(df):
    for x in df.columns:
         df = df.withColumn(x, F.trim(x))
    return df

dfDial = removeTrailingAndLeadingSpaces(dfDial)


# In[15]:


def separateState(df):
    df = df.withColumn('STATETWO', F.substring(F.col('STATE'), 0, 2))
    return df
dfDial = separateState(dfDial)


# In[16]:


def defineState(dfDial):
    dfDial = dfDial.withColumn('STATE', F.when((F.col('STATETWO') == ('NY')), 'NEW YORK').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when((F.col('STATETWO') == ('NJ')), 'NEW JERSEY').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when((F.col('STATETWO') == ('PA')), 'PENNSYLVANIA').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when((F.col('STATETWO') == ('CT')), 'CONNECTICUT').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when((F.col('STATETWO') == ('VA')), 'VIRGINIA').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when((F.col('STATETWO') == ('NJ')), 'NEW JERSEY').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when((F.col('STATETWO') == ('RI')), 'RHODE ISLAND').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when((F.col('STATETWO') == ('M')), 'MANHATTAN').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when((F.col('STATETWO') == ('MA')), 'MASSACHUSETTS').otherwise(F.col('STATE')))
    
    return dfDial
dfDial = defineState(dfDial)


# In[17]:


def defineAirports(dfDial):
    dfDial = dfDial.withColumn('STREET', F.when(F.col('STATETWO') == 'EW', 'BREWSTER RD').otherwise(F.col('STREET')))
    dfDial = dfDial.withColumn('STREET', F.when(F.col('STATETWO') == 'JF', '134TH ST').otherwise(F.col('STREET')))
    dfDial = dfDial.withColumn('STREET', F.when(F.col('STATETWO') == 'LG', 'DITMARS BLVD').otherwise(F.col('STREET')))
    dfDial = dfDial.withColumn('STREET', F.when(F.col('STATETWO') == 'LA', 'DITMARS BLVD').otherwise(F.col('STREET')))
    dfDial = dfDial.withColumn('STREET', F.when(F.col('STATETWO') == 'NW', 'BREWSTER RD').otherwise(F.col('STREET')))
    dfDial = dfDial.withColumn('STREET', F.when(F.col('STATETWO') == 'IS', 'ARRIVAL AVE').otherwise(F.col('STREET')))
    dfDial = dfDial.withColumn('STREET', F.when(F.col('STATETWO') == 'TE', 'INDUSTRIAL AVE').otherwise(F.col('STREET')))
    dfDial = dfDial.withColumn('STREET', F.when(F.col('STATETWO') == 'HP', 'AIRPORT RD').otherwise(F.col('STREET')))
    
    dfDial = dfDial.withColumn('ADDRESS', F.when(F.col('STATETWO') == 'EW', '3').otherwise(F.col('ADDRESS')))
    dfDial = dfDial.withColumn('ADDRESS', F.when(F.col('STATETWO') == 'JF', '148-18').otherwise(F.col('ADDRESS')))
    dfDial = dfDial.withColumn('ADDRESS', F.when(F.col('STATETWO') == 'LG', '102-05').otherwise(F.col('ADDRESS')))
    dfDial = dfDial.withColumn('ADDRESS', F.when(F.col('STATETWO') == 'LA', '102-05').otherwise(F.col('ADDRESS')))
    dfDial = dfDial.withColumn('ADDRESS', F.when(F.col('STATETWO') == 'NW', '3').otherwise(F.col('ADDRESS')))
    dfDial = dfDial.withColumn('ADDRESS', F.when(F.col('STATETWO') == 'IS', '100').otherwise(F.col('ADDRESS')))
    dfDial = dfDial.withColumn('ADDRESS', F.when(F.col('STATETWO') == 'TE', '111').otherwise(F.col('ADDRESS')))
    dfDial = dfDial.withColumn('ADDRESS', F.when(F.col('STATETWO') == 'HP', '240').otherwise(F.col('ADDRESS')))
    
    dfDial = dfDial.withColumn('STATE', F.when(F.col('STATETWO') == 'EW', 'NEW JERSEY').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when(F.col('STATETWO') == 'JF', 'NEW YORK').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when(F.col('STATETWO') == 'LG', 'NEW YORK').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when(F.col('STATETWO') == 'LA', 'NEW YORK').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when(F.col('STATETWO') == 'NW', 'NEW JERSEY').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when(F.col('STATETWO') == 'IS', 'NEW YORK').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when(F.col('STATETWO') == 'TE', 'NEW JERSEY').otherwise(F.col('STATE')))
    dfDial = dfDial.withColumn('STATE', F.when(F.col('STATETWO') == 'HP', 'NEW YORK').otherwise(F.col('STATE')))
    
    dfDial = dfDial.withColumn('PUFROM', F.when(F.col('STATETWO') == 'EW', 'NEWARK').otherwise(F.col('PUFROM')))
    dfDial = dfDial.withColumn('PUFROM', F.when(F.col('STATETWO') == 'JF', 'JAMAICA').otherwise(F.col('PUFROM')))
    dfDial = dfDial.withColumn('PUFROM', F.when(F.col('STATETWO') == 'LG', 'EAST ELMHURST').otherwise(F.col('PUFROM')))
    dfDial = dfDial.withColumn('PUFROM', F.when(F.col('STATETWO') == 'LA', 'EAST ELMHURST').otherwise(F.col('PUFROM')))
    dfDial = dfDial.withColumn('PUFROM', F.when(F.col('STATETWO') == 'NW', 'NEWARK').otherwise(F.col('PUFROM')))
    dfDial = dfDial.withColumn('PUFROM', F.when(F.col('STATETWO') == 'IS', 'RONKONKOMA').otherwise(F.col('PUFROM')))
    dfDial = dfDial.withColumn('PUFROM', F.when(F.col('STATETWO') == 'TE', 'TETERBORO').otherwise(F.col('PUFROM')))
    dfDial = dfDial.withColumn('PUFROM', F.when(F.col('STATETWO') == 'HP', 'WHITE PLAINS').otherwise(F.col('PUFROM')))
    
    return dfDial
dfDial = defineAirports(dfDial)


# In[18]:


dfDial = dfDial.withColumn("SOURCE", F.lit("DIAL7"))
dfDial = dfDial.withColumnRenamed("PUFROM", "CITY")
dfDial = dfDial.withColumn('STREET_ADDRESS', F.concat(F.col('ADDRESS'), F.lit(' '), F.col('STREET')))


# In[19]:


columsToDrop = ['ADDRESS', 'STREET', 'STATETWO']
dfDial = dfDial.drop(*columsToDrop)


# In[20]:


dfDial = dfDial.select(['DATETIME', 'STREET_ADDRESS', 'CITY', 'STATE', 'SOURCE'])


# In[21]:


def fixStreetAbbv(dfDial):
    dfDial = dfDial.withColumn("Street_Address", F.regexp_replace(F.col('Street_Address'), " BL$", " BLVD"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " AV$", " AVE"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " AVENUE$", " AVE"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " COURT$", " CT"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " STREET$", " ST"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " LANE$", " LN"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " ROAD$", " RD"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " DRIVE$", " DR"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " HY$", " HWY"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " HY$", " HWY"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " PZ$", " PLZ"))
    dfDial = dfDial.withColumn('Street_Address', F.regexp_replace(F.col('Street_Address'), " TP$", " TPKE"))
    
    return dfDial
dfDial = fixStreetAbbv(dfDial)


# In[22]:


dfDial.show()



# In[28]:
dfTop100 = df.limit(100)
df = df.subtract(dfTop100)
dfFirstClassTop100 = dfFirstClass.limit(100)
dfFirstClass = dfFirstClass.subtract(dfFirstClassTop100)
dfDialTop100 = dfDial.limit(100)
dfDial = dfDial.subtract(dfDialTop100)
# print(df.show())
# print(dfFirstClass.show())
# df = df.unionAll(dfFirstClass).unionAll(dfDial)



# In[27]:



# df = df.withColumn("ADDRESS",F.concat(F.col("STREET_ADDRESS"), F.lit(" "), F.col("CITY"), F.lit(" "), F.col("STATE")))
# print(df.show())
# from geopy.extra.rate_limiter impo
# @udf
def lat_long(street, city, state):
    address = " ".join([street, city, state])
    locator=  Nominatim()
    location = locator.geocode(address)
    if location:
        lat = location.latitude
        long1 = location.longitude
    else:
        lat = None
        long1 = None

    return (lat,long1)

lat_udf = F.udf(lat_long, T.StructType([T.StructField('LATITUDE', T.FloatType(), True), T.StructField('LONGITUDE', T.FloatType(),True)]))
dfTop100 = dfTop100.withColumn('GEOCODE', lat_udf(F.col('STREET_ADDRESS'), F.col('CITY'), F.col('STATE')))#.select('DATETIME', 'SOURCE', 'STREET_ADDRESS','CITY','STATE','GEOCODE.*'))
dfTop100 = dfTop100.select('DATETIME', 'STREET_ADDRESS', 'CITY', 'STATE','SOURCE','GEOCODE.*')

dfFirstClassTop100 = dfFirstClassTop100.withColumn('GEOCODE', lat_udf(F.col('STREET_ADDRESS'), F.col('CITY'), F.col('STATE')))#.select('DATETIME', 'SOURCE', 'STREET_ADDRESS','CITY','STATE','GEOCODE.*'))
dfFirstClassTop100 = dfFirstClassTop100.select('DATETIME', 'STREET_ADDRESS', 'CITY', 'STATE','SOURCE','GEOCODE.*')

dfDialTop100 = dfDialTop100.withColumn('GEOCODE', lat_udf(F.col('STREET_ADDRESS'), F.col('CITY'), F.col('STATE')))#.select('DATETIME', 'SOURCE', 'STREET_ADDRESS','CITY','STATE','GEOCODE.*'))
dfDialTop100 = dfDialTop100.select('DATETIME', 'STREET_ADDRESS', 'CITY', 'STATE','SOURCE','GEOCODE.*')

df = df.withColumn("LATITUDE", F.lit("null"))
df = df.withColumn("LONGITUDE", F.lit("null"))

dfFirstClass = dfFirstClass.withColumn("LATITUDE", F.lit("null"))
dfFirstClass = dfFirstClass.withColumn("LONGITUDE", F.lit("null"))

dfDial = dfDial.withColumn("LATITUDE", F.lit("null"))
dfDial = dfDial.withColumn("LONGITUDE", F.lit("null"))

df = dfTop100.unionAll(df).unionAll(dfFirstClassTop100).unionAll(dfFirstClass).unionAll(dfDialTop100).unionAll(dfDial)

# print(test.show())
# In[33]:


df.coalesce(1).write.csv("Standardization/Skyline_Final.csv", header=True)


# In[ ]:




