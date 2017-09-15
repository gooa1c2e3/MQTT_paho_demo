# coding: utf-8
import sys, os, time
from datetime import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR, MQTT_LOG_DEBUG

### block of function definition
def on_publish(client, userdata, mid):
    print '--- on pub ---'
    print 'userdata:', userdata
    print 'mid:', mid
    print '--- end pub ---'

def on_message(client, userdata, msg):
    print '--- on msg ---'
    print "topic: %s" % msg.topic
    print "payload: %s" % msg.payload
    print "qos: %d" % msg.qos
    print '--- end msg ---'
    
def on_log(client, userdata, level, buf):
    if level == MQTT_LOG_INFO:
        head = 'INFO'
    elif level == MQTT_LOG_NOTICE:
        head = 'NOTICE'
    elif level == MQTT_LOG_WARNING:
        head = 'WARN'
    elif level == MQTT_LOG_ERR:
        head = 'ERR'
    elif level == MQTT_LOG_DEBUG:
        head = 'DEBUG'
    else:
        head = level
    print('%s: %s' % (head, buf))

def on_connect(client, userdata, flag, rc):
    global TOPIC_ROOT, QOS1, TOPIC2, QOS2, TOPIC3, QOS3
    print '--- on connect ---'
    print 'flag=', flag
    if rc == 0:  # rc means response code, in MQTT, 0 is successed
        # subscribe when connected.
        print '--- ask sub ---'
        s_rc = client.subscribe([(TOPIC_ROOT, QOS1),(TOPIC2, QOS2),(TOPIC3, QOS3)])  # you can sub a list like [(topic1, qos1),(topic2, qos2),...]
        if s_rc[0] == 0:
            print 'sub successed'
        else:
            print 'sub failed'
        print '--- end ask sub ---'
    else:
        print 'connect failed'
    print '--- end on connect ---'
    
def on_subscribe(mq, userdata, mid, granted_qos):
    global TOPIC_ROOT, QOS1, TOPIC2, QOS2, TOPIC3, QOS3
    print '--- on sub ---'
    print 'userdata:', userdata
    print 'mid:', mid
    print 'granted_qos:', granted_qos  # granted_qos is a list for qos of every subed topic, eg. (2, 0, 1) in this case
    
    ### check sub state for every topic
    if granted_qos[0] == QOS1:
        print 'sub %s successed by qos %d' % (TOPIC_ROOT, granted_qos[0])
    else:
        print 'sub %s failed' % (TOPIC_ROOT)
    
    if granted_qos[1] == QOS2:
        print 'sub %s successed by qos %d' % (TOPIC2, granted_qos[1])
    else:
        print 'sub %s failed' % (TOPIC2)

    if granted_qos[2] == QOS3:
        print 'sub %s successed by qos %d' % (TOPIC3, granted_qos[2])
    else:
        print 'sub %s failed' % (TOPIC3)
    
    print '--- end on sub ---'
    
def callback_function(client, userdata, msg):
    print '--- callback ---'
    print "topic: %s" % msg.topic
    print "payload: %s" % msg.payload
    print "qos: %d" % msg.qos
    
    terms = msg.payload.split(' ')
    if len(terms) > 0 :
        term = terms[-1]
        print 'term: %s' % term
    print '--- callback end ---'
    
### clinet Authentication
# If broker asks client ID.
# Client id should be uniqe for broker
client_id="puber"

# If broker asks user/password.
user = ""
password = ""

TOPIC_ROOT = "qosfn/test"
QOS1 = 2
payload = "Hi!! mosquitto!!!"

TOPIC2 = "TOPIC/#"
QOS2 = 0

TOPIC3 = 'temp'
QOS3 = 1

if __name__ == '__main__':
    
    ### Create clinet object
    client = mqtt.Mosquitto(client_id=client_id, clean_session=True, userdata='USERDATA')
    #client = mqtt.Client()
    ### If broker asks user/password.
    client.username_pw_set(user, password)
    
    ### Clean & reset session
    #client.reinitialise(client_id=client_id, clean_session=True, userdata='CYpuber')

    ### Default inflight = 20
    client.max_inflight_messages_set(20)

    ### Default retry = 20
    client.message_retry_set(retry=5)

    client.max_queued_messages_set(queue_size=30)
    
    ### If need will; will should be raised for broken connection
    client.will_set(TOPIC_ROOT, payload='Puber offline because of broken connection', qos=2, retain=True)
    
    ### If need to add callback functions; define callback functions just like on_message
    ### client.message_callback_add() & message_callback_remove() just handle a list of key-value pairs, eg. (topic filter, callback_function)
    client.message_callback_add(TOPIC_ROOT, callback_function)
    
    ### Set functions to client
    client.on_publish = on_publish
    client.on_message = on_message
    client.on_log = on_log
    client.on_connect = on_connect   # If not need sub, remark this line
    client.on_subscribe = on_subscribe
    
    ### Get connection; default keepalive = 60 sec
    client.connect("192.168.1.187", '1883', keepalive=10)

    ### start service
    #client.loop_forever()
    client.loop_start()    # Claim MQTT service start; you can use 'client.loop_forever()' instead of 'client.loop_start()'
    for i in xrange(0, 3):
        message = "Hello MQTT!"
        infot = client.publish(TOPIC_ROOT, message, qos=0)
        
        ### wait for broker ack, only for qos == 1 or 2
        infot.wait_for_publish()
        
        time.sleep(7)
    
    ### stop service
    client.loop_stop()
    
    ### normal disconnect is NOT RAISE setted will on broker.
    ### Offline message should be pub before disconnect() by hand
    client.publish(TOPIC_ROOT, payload='Puber offline normally', qos=0)
    client.disconnect()
    print 'connection stop'