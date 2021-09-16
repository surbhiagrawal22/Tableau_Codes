import tableau_api_lib
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_projects_dataframe,get_sites_dataframe,get_datasources_dataframe
from tableau_api_lib import sample_config
import os
import glob 
import pandas as pd
print(sample_config)


config={'tableau_prod': {'server': 'https://<YOUR_SERVER>.com',
    'api_version': '<YOUR_API_VERSION>', 
    'username': '<YOUR_USERNAME>',
     'password': '<YOUR_PASSWORD>', 
    'site_name': '<YOUR_SITE_NAME>', 
 '  site_url': '<YOUR_SITE_URL>'}}



conn=  TableauServerConnection(config,env='tableau_prod') 
# signing in to the Tableau server    
conn.sign_in()

# get all the sites on Server in a dataframe 
sites_data=get_sites_dataframe(conn)

site_choose=input('Under which site do you want to publish the data source ')
project_choose=input('Under which Project do you want to publish the data source ')

print(sites_data[sites_data['name']==site_choose]['id'])

# Switching to the site which user entered, and want to publish data source
conn.switch_site(str(sites_data[sites_data['name']==site_choose]['id']))
res=conn.query_projects()



# gettign the project site where we want to publish our data sources
project_id=[i['id'] for i in res.json()['projects']['project'] if i['name']==project_choose]
project_id=str(project_id[0])    
print(project_id)
# changing to the directory where all our data sources are saved

# looping over all data sources and publsihing them on server
for i in glob.glob('/Users/surbhiagrawal/Downloads/sample/*.hyper'):
    print(i.split('/')[-1])
    res=conn.publish_data_source(i,datasource_name=i.split('/')[-1],project_id=project_id,datasource_description=i.split('/')[-1])
    print(res.json())

# to get the list of all publsihed data sources
datasources_under_site_project=get_datasources_dataframe(conn)
print(datasources_under_site_project)