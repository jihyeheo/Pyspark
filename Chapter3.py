#!/usr/bin/env python
# coding: utf-8

# ## Big Data Cleaning and Wrangling with Spark Notebooks
# 
# This Chapter.<br>
# - Using Spark Notebooks for quick iteration of ideas<br>
# - Sampling/filtering RDDs to quick out relevant data points<br>
# - Splitting datasets and creating some new combinations<br>

# ### Using Spark Notebooks for quick iteration of ideas

# #### 1) What are Spark Notebooks?
# 
# The Spark Notebook is the open source notebook aimed at enterprise environments, providing Data Scientists and Data Engineers with an interactive web-based editor that can combine Scala code, SQL queries, Markup and JavaScript in a collaborative manner to explore, analyse and learn from massive data sets.
# 
# The Spark Notebook allows performing reproducible analysis with Scala, Apache Spark and the Big Data ecosystem.
# 
# ![image.png](attachment:image.png)

# In[1]:


sc


# We instantly get access to our SparkContect that tells us that Spark is Version 3.0.2, our Master is at local, the AppName is PYSparkShell.<br>
# So,Now we know how we create a Notebook-like environment in Jupyter. In the next section, we will look at sampling and filtering RDDs to pick out relevant data points.

# #### 2) Sampling/filtering RDDs to pick out relevant data points

# In[2]:


# we first import the time library as follows:
from time import time


# In[9]:


# we want to do is look at lines or data points in the KDD database
# Contains the word normal:
raw_data = sc.textFile("file:///tmp/kddcup.data_10_percent.gz")
raw_data


# In[10]:


raw_data.take(2)


# In[12]:


# We're sampling 10% of the data, and we're providing 42 as our 
# random seed:
sampled = raw_data.sample(False,0.1,42)


# In[13]:


contains_normal_sample = sampled.map(lambda x:x.split(',')).filter(lambda x:"normal" in x)


# In[15]:


t0 = time()
num_sampled = contains_normal_sample.count()
duration = time() - t0


# In[16]:


duration


# In[17]:


contains_normal = raw_data.map(lambda x:x.split(",")).filter(lambda x:"normal" in x)
t0 = time()
num_sampled = contains_normal.count()
duration = time() - t0


# In[18]:


duration


# => duration comparison : sampled data > all data

# In[19]:


# how we can use take Sample.
# All we need to do is use the following code :
data_in_memory = raw_data.takeSample(False, 10, 42)
# 10 items
# This method should only be used if the resulting array is 
# expected to be small, as all the data is loaded into the driver’s 
# memory.


# In[20]:


contains_normal_py = [line.split(",") for line in data_in_memory if "normal" in line]
len(contains_normal_py)


# We originally took a sample of 10000 data points, and it crashed the machine. So here, we will take these ten data points to see if it contains the word normal.
# 
# 
# We can see that the calculation is completed in the previous code block, it took longer and used more memory than if we were doing it in PySpark. And that's why we use Spark. because Spark allows us to parallelize any big datasets and operate it on it using a parallel fashion, which means that we can do more with less memory and with less time. In the next section, we're going to talk about splitting datasets and creating new combinations with set operations.

# #### 3) Splitting datasets and creating some new combinations
# 
# we've been looking at lines in the datasets that contain the word normal. Let's try to get all the lines that don't contain the word normal.

# In[21]:


normal_sample = sampled.filter(lambda line:"normal." in line)


# In[22]:


non_normal_sample = sampled.subtract(normal_sample) # minus


# In[24]:


sampled.count()


# In[28]:


# 10% of the dataset
normal_sample.count()


# In[26]:


non_normal_sample.count()


# Cartesian : Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of elements (a, b) where a is in self and b is in other.

# In[29]:


feature_1 = sampled.map(lambda line:line.split(",")).map(lambda features:features[1]).distinct()


# In[30]:


feature_2 = sampled.map(lambda line:line.split(",")).map(lambda features:features[2]).distinct()


# In[31]:


f1 = feature_1.collect()
f2 = feature_2.collect()


# In[32]:


f1


# In[33]:


f2


# In[35]:


#f2 has a lot more values, and we can use the cartesian function 
# to clooect all the combinations between f1 and f2 as follows :
len(feature_1.cartesian(feature_2).collect())


# we looked at Spark Notebooks;<br>
# sampling, filtering, and splitting datasets and creating new combinations with set oprations

# In[ ]:




