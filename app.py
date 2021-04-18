from kafka import KafkaConsumer 
from kafka import KafkaProducer 
from json import loads 
from json import dumps 
import requests
import threading
import json
import logging
BOOTSTRAP_SERVERS = ['b-2.microservice-kafka-2.6lxf1h.c6.kafka.us-west-2.amazonaws.com:9094','b-1.microservice-kafka-2.6lxf1h.c6.kafka.us-west-2.amazonaws.com:9094']

class OrderManager():
    producer = KafkaProducer(acks=0,security_protocol="SSL", compression_type='gzip', bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v, sort_keys=True).encode('utf-8'))  
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
        url= 'http://orderlist.flask-restapi/orderlist/' + str(json_data['id'])
        t_data = {"status": status}
        ret_json = json.dumps(t_data)
        r = requests.patch( url,data=ret_json)
        print(r.status_code)
        if r.status_code == 200:
            if status == "success-kafka-product":
                self.sendkafka("userkafka", json_data, "checkuser")
            elif status == "success-kafka-user":
                self.sendkafka("creditkafka", json_data, "checkcredit")
            elif status == "success-kafka-credit":
                self.sendkafka("deliverykafka", json_data, "checkdelivery") 
            elif status == "fail-reduce-kafka-user" or status == "fail-lack-kafka-user" or status == "fail-kafka-user" or status == "fail-kafka-delivery" or status == "fail-kafka-credit":
                self.sendkafka("recoverykafka", json_data , status )
                                      
        print( r.content, self.ret_fin )
         
if __name__ == '__main__':
#    OrderManager.register_kafka_listener('orderkafka')
#   app.run(host="0.0.0.0", port=5052,debug=True)
    ordermanager = OrderManager()
    ordermanager.register_kafka_listener('orderkafka')