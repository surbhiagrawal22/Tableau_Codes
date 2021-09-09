from tableau_api_lib import sample_config
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_schedules_dataframe
print(sample_config)

config={'tableau_prod': {'server': 'https://<YOUR_SERVER>.com',
   'api_version': '<YOUR_API_VERSION>',
   'username': '<YOUR_USERNAME>',
   'password': '<YOUR_PASSWORD>', 
   'site_name': '<YOUR_SITE_NAME>', 
   'site_url': '<YOUR_SITE_URL>'}}

schedule_priority= int(input('Please enter a schedule priority to be chnaged ?'))

# establishing a conenction object
conn=TableauServerConnection(config,env='tableau_prod')

# signing to the server
conn.sign_in()

# querying all the avaiable schedules
schedule_id=conn.query_schedules()

# printing the total avaiable schedules accessing the json object
print(schedule_id.json()['pagination']['totalAvailable'])

# create a loop over the number of times of schedules present 
for i in range(int(schedule_id.json()['pagination']['totalAvailable'])):
   # changing the schedule priority if the name of the schedule is equal to a ceratin name 
        if schedule_id.json()['schedules']['schedule'][i]['name']=='Weekday early mornings':
            # printing the value of curremt schedule priority
            print(schedule_id.json()['schedules']['schedule'][i]['priority'])

            # updating the priority of the schedule
            conn.update_schedule(schedule_id.json()['schedules']['schedule'][i]['id'],schedule_priority=schedule_priority)


# accesing the dataframe of all schedules (also another option to access all schedules)

get_schedule_info=get_schedules_dataframe(conn)

"""print(get_schedule_info)
print(get_schedule_info.columns)
print(get_schedule_info.loc[get_schedule_info['name']== 'Weekday early mornings']['priority'])"""

if  int(get_schedule_info.loc[get_schedule_info['name']== 'Weekday early mornings']['priority'])==schedule_priority:
   print(f'Schedule priority Updated to {schedule_priority}')
else:
    print('Schedule priority not updated')     

conn.sign_out()