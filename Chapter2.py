#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[ ]:





# In[53]:


import urllib.request


# In[69]:


url = 'http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz'
localfile = '/tmp/kddcup.data_10_percent.gz'
f = urllib.request.urlretrieve(url,localfile)


# In[82]:


sc


# In[71]:


raw_data = sc.textFile("file:///tmp/kddcup.data_10_percent.gz")
raw_data.take(2)


# In[72]:


raw_data


# In[73]:


a = range(100)
a


# In[74]:


list(a)


# In[75]:


list_rdd=sc.parallelize(list(a))
list_rdd


# In[76]:


list_rdd.count()


# In[77]:


list_rdd.take(10)


# In[78]:


list_rdd.reduce(lambda a,b : a+b)


# In[79]:


contains_normal = raw_data.filter(lambda line: "normal." in line )
contains_normal.count()


# In[80]:


split_file = raw_data.map(lambda line: line.split(','))
for i in split_file.take(2):
    print(i)


# In[81]:


raw_data.collect()


# In[ ]:




