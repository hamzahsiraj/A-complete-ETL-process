#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
import pandas as pd
import time


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# In[3]:


from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover
import pyspark.sql.functions as F
import pyspark.sql.types as T


# In[4]:


df=spark.read.option("inferSchema","true").option("delimiter", "~").schema(
        T.StructType(
            [
                T.StructField("DateTime",T.StringType()), T.StructField("Tweet", T.StringType())
            ]
        )
    )\
    .csv('tweet.txt')


# In[5]:


import string


# In[6]:


df_clean = df.withColumn('Tweet', (F.lower(F.regexp_replace('Tweet', "[^a-zA-Z\\s]", ""))))


# In[7]:


# tokenizer = Tokenizer(inputCol='Tweet', outputCol='words_token')
tokenizer = RegexTokenizer(inputCol='Tweet', outputCol='words_token', pattern='\\W')
df_words_token = tokenizer.transform(df_clean)


# In[8]:


remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean')
df_words_no_stopw = remover.transform(df_words_token)


# In[9]:


from pyspark.ml.feature import CountVectorizer


# In[10]:


cv = CountVectorizer(inputCol='words_clean', outputCol='features')


# In[11]:


model = cv.fit(df_words_no_stopw)


# In[12]:


result  = model.transform(df_words_no_stopw)


# In[13]:


vocab=  model.vocabulary
print(vocab[0:10])


# In[14]:


def weight(vec):
    count = 0
    for item in vec.indices.tolist():
        if item in range(0, 5):
            count +=1
            
    if count > 2:
        return 1
    else:
        return 0
    
ny_udf = F.udf(weight, T.IntegerType())
df1 = result.withColumn('tags',ny_udf(F.col('features')))


# In[15]:


df1.select('features', 'tags').show(100)


# In[16]:


df1.select('Datetime','Tweet','features', 'tags').toPandas().to_csv('finalized.csv', index=False)
time.sleep(15)


