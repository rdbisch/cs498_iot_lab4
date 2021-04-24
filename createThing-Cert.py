################################################### Connecting to AWS
import boto3
import json
import os
################################################### Create random name for things
import random
import string

################################################### Parameters for Thing
defaultPolicyName = 'lab4_policy'
###################################################

def createThing(thingClient, thingName):
	thingResponse = thingClient.create_thing(
		thingName = thingName
	)
	data = json.loads(json.dumps(thingResponse, sort_keys=False, indent=4))
	for element in data: 
		if element == 'thingArn': thingArn = data['thingArn']
		elif element == 'thingId': thingId = data['thingId']
	return createCertificate(thingClient, thingName)

def createCertificate(thingClient, thingName):
	certResponse = thingClient.create_keys_and_certificate(
			setAsActive = True
	)
	data = json.loads(json.dumps(certResponse, sort_keys=False, indent=4))	
	for element in data: 
			if element == 'certificateArn':
					certificateArn = data['certificateArn']
			elif element == 'keyPair':
					PublicKey = data['keyPair']['PublicKey']
					PrivateKey = data['keyPair']['PrivateKey']
			elif element == 'certificatePem':
					certificatePem = data['certificatePem']
			elif element == 'certificateId':
					certificateId = data['certificateId']

	response = thingClient.attach_policy(
			policyName = defaultPolicyName,
			target = certificateArn
	)
	response = thingClient.attach_thing_principal(
			thingName = thingName,
			principal = certificateArn
	)

	response = thingClient.add_thing_to_thing_group(
		thingGroupName = "lab4_thinggroup",
		thingName = thingName
	)

	return {
		thingName:
			{
				"certificateArn": certificateArn,
				"PublicKey": PublicKey,
				"PrivateKey": PrivateKey,
				"certificatePem": certificatePem
			}
	}


def writeFile(prefix, name, data):
	with open(prefix+name, "w") as outfile:
		outfile.write(data)

thingClient = boto3.client('iot')

for i in range(500):
	thingName = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(15)])
	path = "data/certificates/device_{}/".format(i)
	os.mkdir(path)
	keys = createThing(thingClient, thingName)[thingName]
	writeFile(path, "device.json", json.dumps(keys))
	writeFile(path, "device_{}.certificate.pem".format(i), keys["certificatePem"])
	writeFile(path, "device_{}.private.pem".format(i), keys["PrivateKey"])
