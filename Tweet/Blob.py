
import pandas as pd


# In[2]:


df = pd.read_csv('finalized.csv')


# In[5]:


from textblob import TextBlob



# In[6]:


def detect_polarity(text):
    return TextBlob(text).sentiment.polarity

df['Polarity'] = df.Tweet.apply(detect_polarity)

# print("why here")
# In[7]:


def detect_subjectivity(text):
    return TextBlob(text).sentiment.subjectivity

df['Subjectivity'] = df.Tweet.apply(detect_subjectivity)


# In[8]:




# In[11]:


df1 = df.drop(df.columns[[2]], axis=1)


# In[9]:


pd.set_option('display.max_rows', df.shape[0]+1)


# In[12]:


print(df1.head(100))


# In[13]:


df1.to_csv('Tweet/Tweet_Fact.csv', index=False)


# In[ ]:




