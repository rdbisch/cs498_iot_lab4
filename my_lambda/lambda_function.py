import json
import boto3

max_co2 = {}
client = boto3.client('iot-data', region_name = 'us-east-2')

def lambda_handler(event, context):
    global max_co2
    
    key = event["vehicle_id"]
    CO2 = event["vehicle_CO2"]
    publishChange = False
    r = None

    if key in max_co2:
        if co2 > max_co2[key]: publishChange = True
        else: publishChange = False
    else: publishChange = True

    # Some code from https://stackoverflow.com/questions/37810289/how-can-i-publish-to-a-mqtt-topic-in-a-amazon-aws-lambda-function
    if publishChange:
        max_co2[key] = CO2
        client.publish(
            topic='max_co2',
            qos=0,
            payload=json.dumps({"vehicle_ID": key, "max_CO2": max_co2})
        )

    return {
        'statusCode': 200,
        'event': event,
        'max_co2': max_co2[key]
    }

#f __name__ == "__main__":
#    test_data = pd.read_csv("../data/vehicle0.csv")
#    for i in range(100):
#        print(lambda_handler(json.dumps(test_data.loc[i].to_dict()), None))

