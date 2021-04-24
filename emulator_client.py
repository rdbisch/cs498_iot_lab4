# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import datetime
import numpy as np
from threading import Lock 


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 3

#Path to the dataset, modify this
data_path = "data/vehicle{}.csv"

#Path to your certificates, modify this
certificate_formatter = "data/certificates/device_{}/device_{}.certificate.pem"
key_formatter = "data/certificates/device_{}/device_{}.private.pem"

class MQTTClient:
	def __init__(self, device_id, cert, key):
		# For certificate based connection
		self.device_id = str(device_id)
		self.log(2, "creating device cert {} key {}".format(cert, key))
		self.state = 0
		self.client = AWSIoTMQTTClient(self.device_id)
		#TODO 2: modify your broker address
		self.client.configureEndpoint("a9ejcm75pxqdq-ats.iot.us-east-2.amazonaws.com", 8883)
		self.client.configureCredentials("./AmazonRootCA1.pem.txt", key, cert)
		self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
		self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
		self.client.configureConnectDisconnectTimeout(10)  # 10 sec
		self.client.configureMQTTOperationTimeout(5)  # 5 sec
		self.client.onMessage = self.customOnMessage
		self.client.connect()
		
	def disconnect(self):
		"""Disconnect from AWS"""
		self.client.disconnect()
		self.client = None

	def customOnMessage(self,message):
		#TODO3: fill in the function to show your received message
		self.log(2, "received message [TOPIC {}] [PAYLOAD {}]".format(message.topic, message.payload))
		
		#Don't delete this line
		#self.client.disconnectAsync()

	def payload(self):
		"""Generate a payload for testing """
		result = {
			"device_id": self.device_id,
			"state": self.state,
			"data":  "!A man, a plan, a canal. Panama!"
		}
		return json.dumps(result)

	def log(self, level, msg):
		print("[LOG {}] [DEVICE {}] {}".format(level, self.device_id, msg))

	# Suback callback
	def customSubackCallback2(self,client, mid, data):
		"""Per the documentation, client and mid should be ignored and soon to be deprecated."""
		self.log(2, "customSubackCallback2 topic {} payload {}".format(data.topic, data.payload))

	# Suback callback
	def customSubackCallback(self,mid, data):
		self.log(2, "customSubackCallback data {}".format(data))

	# Puback callback
	def customPubackCallback(self,mid):
		self.log(2, "custom PubackCallback with data {}".format(self.device_id, data))

	def publish2(self):
		#TODO4: fill in this function for your publish
		a = self.client.connect()
		b = self.client.subscribe("test", 0, callback=self.customSubackCallback2)
		self.log(2, "publish2 connect return {} subscribe return {}".format(a,b))

	def publish(self, topic, payload, QoS = 0):
		self.client.publishAsync(topic, payload, QoS, ackCallback=self.customPubackCallback)
		self.log(2, "client {} sent to topic {} payload {}".format(self.device_id, topic, payload))

# Don't change the code below
print("wait")
lock = Lock()
data = []
for i in range(5):
	a = pd.read_csv(data_path.format(i))
	data.append(a)

clients = []
for device_id in range(device_st, device_end):
	client = MQTTClient(device_id,certificate_formatter.format(device_id,device_id) ,key_formatter.format(device_id,device_id))
	clients.append(client)

states_for_test = [3, 0, 0, 0, 4, 0, 0, 1, 0, 0, 0, 4, 4, 0, 0, 3, 2, 3, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,\
 0, 0, 0, 4, 0, 4, 3, 0, 0, 3, 0, 2, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0,\
  2, 4, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 0, 4, 1, 4, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0,\
   0, 1, 0, 1, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0,\
    0, 0, 4, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 2, 0, 0, 0, 0, 0, 0, 2, 0, 4, 0, 3, 0,\
     0, 4, 1, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0,\
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 4, 4, 0, 0, 0, 0, 0, 0, 2,\
       0, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 2, 0, 0, 0, 0,\
        0, 1, 2, 1, 0, 0, 4, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 4, 1, 0, 3, 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,\
         0, 0, 4, 4, 0, 0, 0, 4, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 1, 2, 0, 0,\
          0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 3, 0, 0, 4, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 4, 0, 0,\
           0, 4, 1, 1, 0, 0, 0, 1, 3, 2, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,\
            2, 0, 2, 2, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 0, 0, 0, 0, 0, 0, 0, 4]
s1,s2,s3,s4 = [],[],[],[]
for i in range(device_st,device_end):
	if i < 500:
		clients[i].state = states_for_test[i]
		if states_for_test[i] == 1: s1.append(i)
		elif states_for_test[i] == 2: s2.append(i)
		elif states_for_test[i] == 3: s3.append(i)
		elif states_for_test[i] == 4: s4.append(i)

print("Users at state 1: ", s1)
print("Users at state 2: ", s2)
print("Users at state 3: ", s3)
print("Users at state 4: ", s4)

print("send now?")
while (True):
	print("Waiting on input. S = Send.  D = Disconnect...")
	x = input()
	if x == "s":
		# Loop until we're out of data.
		row = [ d.iterrows() for d in data ]
		while True:
			anyOn = False
			for i,c in enumerate(clients):
				try:
					next_row = next(row[c.state])
					data_row = next_row[1].to_dict()
					data_row["state"] = c.state
					data_row["row"] = next_row[0]
					c.publish("test", json.dumps(data_row))
					anyOn = True
				except StopIteration:
					pass
			
			if not anyOn: break
			time.sleep(0.5)

		# print("done")
	elif x == "d":
		for c in clients:
			c.disconnect()
			print("All devices disconnected")
		break
	else:
		print("wrong key pressed")

print("Thank you and have a nice day.")