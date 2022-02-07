import sys
import random
import json
import datetime
import os
MessagesCount=0

if len(sys.argv) >1:
	MessagesCount=int(sys.argv[1])
else:
	MessagesCount=1




baseTemperature_state = {'WA': 48.3, 'DE': 55.3, 'DC': 58.5, 'WI': 43.1, 
		  'WV': 51.8, 'HI': 70.0, 'FL': 70.7, 'WY': 42.0, 
		  'NH': 43.8, 'NJ': 52.7, 'NM': 53.4, 'TX': 64.8, 
		  'LA': 66.4, 'NC': 59.0, 'ND': 40.4, 'NE': 48.8, 
		  'TN': 57.6, 'NY': 45.4, 'PA': 48.8, 'CA': 59.4, 
		  'NV': 49.9, 'VA': 55.1, 'CO': 45.1, 'AK': 26.6, 
		  'AL': 62.8, 'AR': 60.4, 'VT': 42.9, 'IL': 51.8, 
		  'GA': 63.5, 'IN': 51.7, 'IA': 47.8, 'OK': 59.6, 
		  'AZ': 60.3, 'ID': 44.4, 'CT': 49.0, 'ME': 41.0, 
		  'MD': 54.2, 'MA': 47.9, 'OH': 50.7, 'UT': 48.6, 
		  'MO': 54.5, 'MN': 41.2, 'MI': 44.4, 'RI': 50.1, 
		  'KS': 54.3, 'MT': 42.7, 'MS': 63.4, 'SC': 62.4, 
		  'KY': 55.6, 'OR': 48.4, 'SD': 45.2}

#Every Sensor has unique GUID
GUID="StreamingDataAnalytics-"
#Every Sensor sends the data to Same Destination
Destination="Repository"

Letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"

device_SensorList={}
current_RecordedTemperature={}

'''
JSON Data Format
{
	"GUID":"sensor_GUIDValue"
	"Destination":"Destination"
	"State":"Temperature to be recorded for STATE"
	"eventTime":"TimeStamp"
	"Payload":"Actual Data"
	{
		"data":
		{
			"temperature":"RecordedTemperature for the state"
		}
	}
}
'''
if os.path.exists("TemperatureRecorded.txt"):
	os.remove("TemperatureRecorded.txt")

for i in range(MessagesCount):
	#creation of unique GUID for Sensor
	try:
		sensor_id=GUID+str(random.randint(0,9))+random.choice(Letters)
		temperature_uniformWeight=random.uniform(-5,5)
		state=random.choice(list(baseTemperature_state.keys()))
		if(not sensor_id in list(device_SensorList.keys())):
			device_SensorList[sensor_id]=state
			current_RecordedTemperature[state]=baseTemperature_state[state]+temperature_uniformWeight
		else:
			state=device_SensorList[sensor_id]	
		Message_Data={}
		Message_Data["GUID"]=sensor_id
		Message_Data["Destination"]=Destination
		Message_Data["State"]=state
		Message_Data["eventTime"]=str(datetime.datetime.now())
		Message_Data["Payload"]={ "data":{"temperature":current_RecordedTemperature[state]}}
		with open("TemperatureRecorded.txt","a") as fileObject:
			json.dump(Message_Data,fileObject)
			fileObject.write("\n")
	except Exception as e:
		print(e)
