import json
import os

text_file = open("json_data_log.csv", "w")
for filename in os.listdir('/home/training/dataset/json_log_data/'):
	with open('/home/training/dataset/json_log_data/' + filename) as fp:
		for line in fp:
			data = json.loads(line)
			text_file.write(str(data['id']) + "," + str(data['timestamp']) + "," + str(data['channel']) + "," + str(data['userid']) + "," + str(data['action']) + "," + str(data['amount']) + "," + str(data['location']) + "\n")
	
text_file.close()
