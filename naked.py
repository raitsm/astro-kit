import requests
import json
import datetime
import time
import yaml

from datetime import datetime
from configparser import ConfigParser

print('Asteroid processing service')

# Initiating and reading config values. NB: currently the API URL & key are hard-coded in the source file.
# TODO: move the values to a proper config.
print('Loading configuration from file')

try:
	config = ConfigParser()
	config.read("config.ini")
	nasa_api_key = config.get("nasa", "api_key")
	nasa_api_url = config.get('nasa', "api_url")
except:
    print("Error reading configuration file")		# should be logger.exception("")
print("Configuration successfully loaded")		# logger.info("DONE")

# Getting todays date.
dt = datetime.now()
# convert the components of the date to strings that are left-padded with zeros (use zfill for padding)
request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
print("Generated today's date: " + str(request_date))


print("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key))
# send a request to NASA's API interface URL  using today as request start & end date.
# Authenticate using API key (nasa_api_key)

r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)

# print the raw data received back
print("Response status code: " + str(r.status_code))
print("Response headers: " + str(r.headers))
print("Response content: " + str(r.text))		# actual raw data on asteroids, received as json string

if r.status_code == 200:	# corresponds to  HTML Status OK, meaning request was successful

	json_data = json.loads(r.text)		# parse the JSON string, load it into a dictionary (json_data)

	# create two empty lists to store safe & potentially hazardous asteroid data respectively
	ast_safe = []
	ast_hazardous = []

# consistency check - if the number of items (element_count) is not in the request response, skip to the end
	if 'element_count' in json_data:
		ast_count = int(json_data['element_count'])
		print("Asteroid count today: " + str(ast_count))

		if ast_count > 0:	# only process the dataset if there is at least one asteroid to process 

# loop through the asteroid data (to be super-safe, additional check could be added for the request_date and the date in the dataset
			for val in json_data['near_earth_objects'][request_date]:
# check if several attributes required for the analysis are present in the asteroid record:
				if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
					# capture the name of the asteroid
					tmp_ast_name = val['name'] 
					# capture the URL to the respective record in NASA's small body db
					tmp_ast_nasa_jpl_url = val['nasa_jpl_url']	
					# check if the estimated diameter of the asteroid is provided in diameters
					if 'kilometers' in val['estimated_diameter']:
						# make sure there is min and max estimate, and round it to three decimal digits
						if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']:
							tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)
							tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3)
						else:
							# if the estimates for diameter are not present in the data, set them to "-2" - better to use a meaningfully named constant instead!
							tmp_ast_diam_min = -2
							tmp_ast_diam_max = -2
					else:
						# set estimate value to -1 if the estimate in kilometers is absent. Again, a constant would be easier to understand.
						tmp_ast_diam_min = -1
						tmp_ast_diam_max = -1

					# capture the "potentially hazardous" attribute. Could be still improved with a default value & data existance check
					tmp_ast_hazardous = val['is_potentially_hazardous_asteroid']

					# check if close approach data are present & retrieve the data if they are
					if len(val['close_approach_data']) > 0:
						if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]:
							# calculate approach date/time in ticks (makes it easier to sort the asteroids by the approach date/time)
							tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000)
							tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
							tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')

							# get velocity of asteroid, use -1 if velocity is not present in the data
							if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']:
								tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']))
							else:
								tmp_ast_speed = -1

							if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
								tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
							else:
								tmp_ast_miss_dist = -1
						else:
							# handling the missing approach date & time
							tmp_ast_close_appr_ts = -1
							tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59"
							tmp_ast_close_appr_dt = "1969-12-31 23:59:59"
					else:
						print("No close approach data in message")
						tmp_ast_close_appr_ts = 0
						tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
						tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
						tmp_ast_speed = -1
						tmp_ast_miss_dist = -1

					# print & format asteroid data
					print("------------------------------------------------------- >>")
					print("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
					print("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
					print("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km")
					
					# Add the current asteroid data to a list of hazardoes/safe asteroids array
					if tmp_ast_hazardous == True:
						ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])
					else:
						ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])

		else:
			print("No asteroids are going to hit earth today")

	print("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))

	if len(ast_hazardous) > 0:	# are there any potentially hazardous asteroids?
		# sort hazardoes asteroids by their approach time in ascending order
		ast_hazardous.sort(key = lambda x: x[4], reverse=False)

		print("Today's possible apocalypse (asteroid impact on earth) times:")
		for asteroid in ast_hazardous:
			# print formatted approacch date, asteroid name & link to asteroid record in NASA db
			print(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))

		# now rearrange the list of hazardous asteroids by the passing distance
		ast_hazardous.sort(key = lambda x: x[8], reverse=False)
		# print the name, distanca & NASA db link for the asteroid that will pass the closest to the Earth
		print("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))
	else:
		print("No asteroids close passing earth today")

else:
	# print an error message if failed to get response from NASA
	print("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
