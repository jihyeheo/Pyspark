#!/usr/bin/env python
# coding: utf-8

# ## Aggregating and Summarizing Data into Useful Reports
# 
# In this chapter,<br>
# - Calculating averages with map and reduce
# - Faster average computations with aggregate
# - Pivot tabling with key-value paired data points

# ### Calculating averages with map and reduce

# 1) How do we calculate averages, map, reduce?

# In[3]:


# three tuple
rdd = sc.parallelize(['b','a','c'])
sorted(rdd.map(lambda x : (x,1)).collect())


# In[4]:


raw_data = sc.textFile("file:///tmp/kddcup.data_10_percent.gz")
raw_data


# In[7]:


csv = raw_data.map(lambda x:x.split(","))


# In[8]:


normal_data = csv.filter(lambda x:x[41]=="normal.")


# In[9]:


duration = normal_data.map(lambda x:int(x[0]))


# In[10]:


total_duration = duration.reduce(lambda x,y:x+y)


# In[11]:


total_duration


# In[12]:


# to divide total_duration by the counts of the data as follows:
total_duration/(normal_data.count())


# And after a little computation, we would have created two counts using map and reduce.

# ### Faster average computations with aggregate
# 
# We look at faster average computations with aggregate function. You can refer to the documentation mentioned in the previous section.
# 
# aggregate argument
# 
# 1. zero Value<br>
# Aggregate the elements of each partition, and then the results for all the partitions, using a given combine functions and a neutral “zero value.”
# 
# 2. seq0p<br>
# The functions op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object allocation; however, it should not modify t2.
# 
# 3. com0p<br>
# The first function (seqOp) can return a different result type, U, than the type of this RDD. Thus, we need one operation for merging a T into an U and one operation for merging two U
# 
# Example<br>
# seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
# 
# combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
# 
# sc.parallelize([1, 2, 3, 4]).aggregate((0, 0), seqOp, combOp)
# (10, 4)
# 
# sc.parallelize([]).aggregate((0, 0), seqOp, combOp)
# (0, 0)

# In[17]:


duration_count = duration.aggregate(
    (0,0),
     lambda db, new_value: (db[0]+new_value, db[1]+1),
     lambda db1, db2: (db1[0]+db2[0], db1[1]+db2[1])
)


# In[19]:


print("average duration : {}".format(duration_count[0]/duration_count[1]))


# ### Pivot tabling with key-value paired data points

# In[30]:


kv = csv.map(lambda x:(x[41],x))
kv


# In[40]:


kv.take(1)


# In[41]:


kv_duration = csv.map(lambda x: (x[41],float(x[0]))).reduceByKey(lambda x,y: x+y)
kv_duration.collect()


# In[42]:


kv.countByKey()


# http://www.dbaglobe.com/2019/03/pyspark-aggregating-and-summarising-data.html

# In[ ]:




