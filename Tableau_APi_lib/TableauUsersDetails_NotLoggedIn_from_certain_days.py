from tableau_api_lib import TableauServerConnection
from tableau_api_lib import sample_config
from tableau_api_lib.utils.querying import get_users_dataframe,get_sites_dataframe
import datetime 
from datetime import datetime,timezone
import pandas as pd
import os

'''  The goal is to filter all users for all sites who have not logged in for more N number of days also.
 Use Case :- could be used to inform those users to use Tableau or not to renew License for them  ''' 

os.chdir('/Users/surbhiagrawal/Downloads/Tableau Python Practice')

N=int(input('Plesae enter number of days since user has not logged in ? '))

config={'tableau_prod': {'server': 'https://tableau.staging.workpath.com/',
   'api_version': '<YOUR_API_VERSION>',
   'username': '<YOUR_USERNAME>',
   'password': '<YOUR_PASSWORD>', 
   'site_name': '<YOUR_SITE_NAME>', 
   'site_url': '<YOUR_SITE_URL>'}}

conn=TableauServerConnection(config,env='tableau_staging')   

#Logging into Tableau server
conn.sign_in()

# get a dataframe which has all the sites present, we can access contentUrl from this dataframe to loop over.
sites=get_sites_dataframe(conn)
print(f'Avaiable sites are {sites}')

for i in sites['contentUrl']:
   conn.switch_site(i)   # switching to the site for each site in a list
   print(f'Current site is {i}')  
   # getting a dataframe of all users in that current site
   all_users_data=get_users_dataframe(conn)

# converting last-login columns to a datetime column as currently it is Object type
   all_users_data['lastLogin']= pd.to_datetime(all_users_data['lastLogin'])  

# creating a new column with just now() time, converting to same timezone as lastLogin Column else it will throw TypeError: DatetimeArray subtraction must have the same timezones or no timezones
   all_users_data['later_time'] = datetime.now(tz=timezone.utc)

   all_users_data['duration in days']=(all_users_data['later_time']-all_users_data['lastLogin']).dt.days

# Filtering out all users email address and their names so that we can contact them , where days not logegd is greater than n days
   users_not_logged_in=all_users_data[all_users_data['duration in days']>N][['name','siteRole']]  # we can email field also if email field is present

   # saving the datafrme to set current directory
   users_not_logged_in.to_csv(f'{i}_not_logged.csv')


   

