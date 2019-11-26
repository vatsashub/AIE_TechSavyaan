#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().system('pip install --upgrade "ibm-watson>=4.0.1"')
get_ipython().system('pip install --upgrade "watson_developer_cloud"')
get_ipython().system('pip install simplejson')


# !pip install requests
import simplejson as json

from requests import request
from pandas.io.json import json_normalize
import pandas as pd

import numpy as np


# In[3]:


import json
from ibm_watson import DiscoveryV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator

authenticator = IAMAuthenticator('i2Q8WlW2plku6NK1WuL-LXcO58W7AbUn3WnfO5lzcyMA')
discovery = DiscoveryV1(
    version='2019-04-30',
    authenticator=authenticator)

discovery.set_service_url('https://gateway.watsonplatform.net/discovery/api')
environments = discovery.list_environments().get_result()
print(json.dumps(environments, indent=2))
system_environments = [x for x in environments['environments'] if x['name'] == 'Watson System Environment']
system_environment_id = system_environments[0]['environment_id']
collections = discovery.list_collections(system_environment_id).get_result()
system_collections = [x for x in collections['collections']]
print(json.dumps(system_collections[5], indent=2))


# In[25]:


list_company = ['Arconic','Amphenol','SKF','TAL Manufacturing Solutions','ShinMaywa','FELLFAB',
'Avio Interiors', 
'BAE', 
'CATIC', 
'KAI',
'Precision Castparts',
'Israel aerospace']

# list_oem = ['Boeing','Airbus','Hindustan Aeronautics limited','Safran',
# 'Rolls Royce Holdings','Dassault Aviation']

# list_airline = ['United Airlines','Indigo','Lufthansa','Ryanair','Air China','Delta Airlines','Alliance Airlines']

# list_logistic_comp = ['Yusen logistics','Blue Water Shipping','Global Shipping Services',
#                       'The Aerospace logistics Alliance (TALA)','Alpha KKC Logistics']


list_company_specific = ['Worker strike','Raw material shortage','Supply shortage','Scam','Defaulted','Acquisition','Merger','Price hike','Factory fire',
'Technical failure','Recall']


# In[4]:


# list_company = ['Arconic','Amphenol','SKF USA','TAL Manufacturing Solutions','ShinMaywa Industries Ltd.','FELLFAB Limited',
# 'Avio Interiors', 
# 'BAE Systems', 
# 'China National Aero-Technology Import & Export Corporation', 
# 'Korean Aerospace Industries, Ltd', 
# 'Precision Castparts Corp.',
# 'Israel aerospace Industries',
# 'Triumph Group']



# list_oem = ['Boeing','Airbus','Hindustan Aeronautics limited','Safran',
# 'Rolls Royce Holdings','Dassault Aviation']

# list_airline = ['United Airlines','Indigo','Lufthansa','Ryanair','Air China','Delta Airlines','Alliance Airlines']

# list_logistic_comp = ['Yusen logistics','Blue Water Shipping','Global Shipping Services',
#                       'The Aerospace logistics Alliance (TALA)','Alpha KKC Logistics']


# list_company_specific = ['Labour unrest','Worker strike','Labour strike',
# 'Scam','Defaulted','Acquisition','Merger','Acquired','Price fluctuation','Price hike']


# In[5]:


get_ipython().system('pip install datetime')


# In[6]:


import json
from ibm_watson import DiscoveryV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
import datetime
dt = datetime.datetime

authenticator = IAMAuthenticator('i2Q8WlW2plku6NK1WuL-LXcO58W7AbUn3WnfO5lzcyMA')
discovery = DiscoveryV1(
    version='2018-08-01',
    authenticator=authenticator)
discovery.set_service_url('https://gateway.watsonplatform.net/discovery/api')

environments = discovery.list_environments().get_result()
#print(json.dumps(environments, indent=2))

news_environment_id = 'system'
#print(json.dumps(news_environment_id, indent=2))

collections = discovery.list_collections(news_environment_id).get_result()
news_collections = [x for x in collections['collections']]
#print(json.dumps(collections, indent=2))

configurations = discovery.list_configurations(
    environment_id=news_environment_id).get_result()
#print(json.dumps(configurations, indent=2))
data_comb = pd.DataFrame()

for i in range(len(list_company)): 
    for j in range(len(list_company_specific)):
        
    #list_supp = {,,,,,}
        result = discovery.query(environment_id='system',collection_id='news-en',count = 50, natural_language_query=list_company[i]+" " + list_company_specific[j]).get_result()
    #print(result)
        
        elevations = json.dumps(result)
        data = json.loads(elevations)
        data_new = json_normalize(data['results'])
        #data_new['company'] = list_company[i]
        data_new['company'] = list_company[i]
        data_new['reason'] = list_company_specific[j]
        data_new['publication_date'] = pd.to_datetime(data_new['publication_date'])
        #print(len(data_new['publication_date']))
        date_list=[]
        for k in range(0,len(data_new['publication_date'])):
            day=0
            month=0
            day=str(data_new['publication_date'][k].day)
            month = str(data_new['publication_date'][k].month)
            value = day + "_" + month
            date_list.append(value)
        data_new['date'] = np.array(date_list)
        #print(data_new.head(50))
        #print(len(data_new))
        data_new.drop_duplicates(subset ="date",keep = False, inplace = True) 
        
        #print(len(data_new))
        data_comb=data_comb.append(data_new) 


# In[12]:


data_comb = data_comb.reset_index()
del data_comb['index']
#print(len(data_comb))
neg_data = data_comb[data_comb['enriched_text.sentiment.document.label']=='negative'].reset_index()
del neg_data['index']
final_data = neg_data[['company','url','publication_date','enriched_text.sentiment.document.score','enriched_text.sentiment.document.label','reason']]
#print(len(final_data))
new_data = final_data
new_data.columns =[['company','url','publication_date','Sentiment Score','Sentiment Label','Reason Weight']]
new_data.columns = new_data.columns.get_level_values(0)
# new_data.rename(columns = {'test':'TEST'}, inplace = True)

new_data['Reason'] = new_data['Reason Weight']

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Worker strike':'1'})

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Raw material shortage':'2'})

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Supply shortage':'2'})

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Scam':'1.5'})

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Defaulted':'2.5'})

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Acquisition':'1.5'})

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Merger':'1.5'})

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Factory fire':'2'})


new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Price hike':'1.5'})

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Technical failure':'1'})

new_data['Reason Weight'] = new_data['Reason Weight'].replace({'Recall':'1'})




new_data['Reason Weight'] = new_data['Reason Weight'] .astype(float)

new_data['Weighted Sentiment'] = new_data['Reason Weight']*new_data['Sentiment Score']

print(new_data['Reason Weight'].unique())
#print(len(new_data))
new_data['Frequency'] = new_data.groupby('Reason')['Reason'].transform('count')
table = pd.pivot_table(new_data, values=['Weighted Sentiment','Frequency'], index=['Reason'],aggfunc={'Weighted Sentiment': np.mean,'Frequency': np.mean})                  
table['Interaction Score']  = table['Weighted Sentiment'] * table['Frequency']
table = table.sort_values(by='Interaction Score')
table


# In[17]:


new_sent = new_data.sort_values(by='Weighted Sentiment')
new_sent= new_sent.reset_index()
new_sent
new_data


# In[43]:


new_data['url'][130]


# In[26]:


new_data['url'].head(500)


# In[19]:


new_data.to_csv("new_data.csv")


# In[20]:


import os
os.getcwd()


# In[18]:


new_data['url'][6]


# In[10]:


new_data['Frequency'] = new_data.groupby('company')['company'].transform('count')
table = pd.pivot_table(new_data, values=['Weighted Sentiment','Frequency'], index=['company'],aggfunc={'Weighted Sentiment': np.mean,'Frequency': np.mean})                  
table['Interaction Score']  = table['Weighted Sentiment'] * table['Frequency']
table = table.sort_values(by='Interaction Score')
table


# In[11]:


Company_split = pd.DataFrame(new_data.groupby(['company','Reason'])['Sentiment Score'].agg(['mean', 'count']))
Company_split.columns =[['Average Sentiment','Frequency']]
Company_split = Company_split.reset_index()
Company_split

Company_split['Reason Weight'] = Company_split['Reason']


Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Worker strike':'1'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Raw material shortage':'2'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Supply shortage':'2'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Scam':'1.5'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Defaulted':'2.5'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Acquisition':'1.5'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Merger':'1.5'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Factory fire':'2'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Price hike':'1.5'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Technical failure':'1'})

Company_split['Reason Weight'] = Company_split['Reason Weight'].replace({'Recall':'1'})



Company_split['Reason Weight'] = Company_split['Reason Weight'] .astype(float)

Company_split.columns = Company_split.columns.get_level_values(0)
Company_split['Interaction Score'] = Company_split['Reason Weight']*Company_split['Average Sentiment'] * Company_split['Frequency']
 
Company_split 


