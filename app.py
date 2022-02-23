from kafka import KafkaConsumer 
from kafka import KafkaProducer 
from json import loads 
from json import dumps 
import requests
import threading
import json
import boto3
from botocore.config import Config

my_config = Config(
    region_name='us-west-2',
)
#t = get_secret()
#print(DATABASE_CONFIG)
cloudformation_client = boto3.client('cloudformation', config=my_config)
response = cloudformation_client.describe_stacks(
    StackName='MicroserviceCDKVPC'
)
ParameterKey=''
outputs = response["Stacks"][0]["Outputs"]
for output in outputs:
    keyName = output["OutputKey"]
    if keyName == "mskbootstriapbrokers":
        ParameterKey = output["OutputValue"]

print( ParameterKey )
ssm_client = boto3.client('ssm', config=my_config)
response = ssm_client.get_parameter(
    Name=ParameterKey
)
BOOTSTRAP_SERVERS = response['Parameter']['Value'].split(',')


class OrderManager():
    producer = KafkaProducer(acks=0, compression_type='gzip',bootstrap_servers=BOOTSTRAP_SERVERS, security_protocol="SSL", value_serializer=lambda v: json.dumps(v, sort_keys=True).encode('utf-8'))    
    ret_fin = 0
    ret_message = ''

    def register_kafka_listener(self, topic):
        # Poll kafka
        def poll():
            # Initialize consumer Instance
            consumer = KafkaConsumer(topic,security_protocol="SSL", bootstrap_servers=BOOTSTRAP_SERVERS, auto_offset_reset='earliest', enable_auto_commit=True, 
                                        group_id='my-mc' )

            print("About to start polling for topic:", topic)
            consumer.poll(timeout_ms=6000)
            print("Started Polling for topic:", topic)
            for msg in consumer:
                self.kafka_listener(msg)
        print("About to register listener to topic:", topic)
        t1 = threading.Thread(target=poll)
        t1.start()
        print("started a background thread")

    def on_send_success(self, record_metadata):
     #   print("topic: %s" % record_metadata.topic)
        self.ret_fin = 200
        self.ret_message = "successkafkaorder"

    def on_send_error(self, excp):
        print("error : %s" % excp)
        self.ret_fin = 400
        self.ret_message = "failkafkaorder"

    def sendkafka(self, topic,data, status):
        data['status'] = status
        self.producer.send( topic, value=data).get()#.add_callback(self.on_send_success).add_errback(self.on_send_error) 


    def kafka_listener(self, data):
        json_data = json.loads(data.value.decode("utf-8"))
        status = json_data['status']
    #   print( json_data['id'])
        url= 'http://flask-restapi.orderlist/orderlist/' + str(json_data['id'])
        t_data = {"status": status}
        ret_json = json.dumps(t_data)
        r = requests.patch( url,data=ret_json)
        print(r.status_code)
        if r.status_code == 200:
            if status == "success-kafka-product":
                self.sendkafka("userkafka", json_data, "checkuser")
            elif status == "success-kafka-user":
                self.sendkafka("creditkafka", json_data, "checkcredit")
            elif status == "success-kafka-credit" or status == "recovery-kafka-credit":
                self.sendkafka("deliverykafka", json_data, "checkdelivery") 
            elif status == "fail-reduce-kafka-user" or status == "fail-lack-kafka-user" or status == "fail-kafka-user" or status == "fail-kafka-delivery" or status == "fail-kafka-credit":
                self.sendkafka("recoverykafka", json_data , status )
                                      
        print( r.content, self.ret_fin )


def get_kafka_bootstriap():
    my_config = Config(
        region_name='us-west-2',
    )
    #t = get_secret()
    #print(DATABASE_CONFIG)
    cloudformation_client = boto3.client('cloudformation', config=my_config)
    response = cloudformation_client.describe_stacks(
        StackName='MicroserviceCDKVPC'
    )
    ParameterKey=''
    outputs = response["Stacks"][0]["Outputs"]
    for output in outputs:
        keyName = output["OutputKey"]
        if keyName == "mskbootstriapbrokers":
            ParameterKey = output["OutputValue"]

    print( ParameterKey )
    ssm_client = boto3.client('ssm', config=my_config)
    response = ssm_client.get_parameter(
        Name=ParameterKey
    )
    print(response['Parameter']['Value'].split(','))
    return response['Parameter']['Value'].split(',')

if __name__ == '__main__':
    #    OrderManager.register_kafka_listener('orderkafka')
#   app.run(host="0.0.0.0", port=5052,debug=True)


#    BOOTSTRAP_SERVERS = get_kafka_bootstriap('mskbootstriapbrokers')
    print(BOOTSTRAP_SERVERS)
    ordermanager1 = OrderManager()
    ordermanager1.register_kafka_listener('orderkafka')
    