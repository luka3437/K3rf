import time

import paho.mqtt.client as mqtt
import json

log = open('mqtt_convert_log.txt', 'w')

convertors=[]

with open("MQTT_setings.json", "r") as read_file:
     MQTT_setings = json.load(read_file)


# Define event callbacks
def on_connect(client, userdata, flags, rc):
    print("rc: " + str(rc))
def on_message(client, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
    for i in convertors:
        if i.topicIn==msg.topic:
            i.convert(msg.payload)
            break
def on_publish(client, obj, mid):
    print("mid: " + str(mid))
 
def on_subscribe(client, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))
 
def on_log(client, obj, level, string):
    print(string)
 
mqttc = mqtt.Client()
# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe
 
# Uncomment to enable debug messages
#mqttc.on_log = on_log
 
 
class Converter():
    topicIn=""
    topicOut=""
    calls=0
    def __init__(self, topic1, topic2):
        self.topicIn = topic1
        mqttc.subscribe(self.topicIn, 0)
        self.topicOut = topic2
        convertors.append(self)
    def convert(message):
        pass

class Type1Convert(Converter):
    def convert(self, message):
        msg=json.loads(message)
        mqttc.publish(self.topicOut, msg["value"])
        log.write(time.strftime('%d.%m.%Y %H:%M', time.localtime()) + " send data " + msg["value"] + " to topic " + self.topicOut)

class Type2Convert(Converter):
    def convert(self, message):
        msg = int(message, 16)
        if msg != 0 and msg !=1:
            msg=msg/100
        mqttc.publish(self.topicOut, msg)
        log.write(time.strftime('%d.%m.%Y %H:%M', time.localtime()) + " send data " + msg + " to topic " + self.topicOut)


# Connect
mqttc.username_pw_set(MQTT_setings["MQTT_user"], MQTT_setings["MQTT_password"])
mqttc.connect(MQTT_setings["MQTT_server"], MQTT_setings["MQTT_port"])
 
#mqttc.subscribe("test", 0)

with open("MQTT_parsers.json", "r") as read_file:
    MQTT_parsers = json.load(read_file)




rc = 0
print(convertors)
while rc == 0:
    rc = mqttc.loop()
print("rc: " + str(rc))
log.close()