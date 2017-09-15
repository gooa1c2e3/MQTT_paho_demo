import paho.mqtt.client as mqtt
import paho.mqtt.publish
import paho.mqtt.subscribe
import signal
import time
import sys
from paho.mqtt.client import MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR, MQTT_LOG_DEBUG
from datetime import datetime

TOPIC_ROOT = "qosfn/test"

client = None
mqtt_looping = False

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

def on_connect(mq, userdata, rc, _):
    # subscribe when connected.
    mq.subscribe(TOPIC_ROOT, 2)

def on_message(mq, userdata, msg):
    global g_msg, g_userdata
    g_msg = msg
    g_userdata = userdata
    print 'on msg:'
    print "topic: %s" % msg.topic
    print "payload: %s" % msg.payload
    print "qos: %d" % msg.qos
    print '-----------'
    
def on_subscribe(mq, userdata, mid, granted_qos):
    print 'on sub:'
    print 'userdata:', userdata
    print 'mid:', mid
    print 'granted_qos:', granted_qos
    print '-----------'
    
def on_publish(mq, userdata, mid):
    print 'on pub:'
    print 'userdata:', userdata
    print 'on pub:', mid
    print '------------'

    
def mqtt_client_thread():
    global client, mqtt_looping
    client_id = "suber" # If broker asks client ID.
    client = mqtt.Mosquitto(client_id=client_id, clean_session=False, userdata='cysuber')

    # If broker asks user/password.
    user = ""
    password = ""
    client.username_pw_set(user, password)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_publish = on_publish
    client.on_log = on_log
    
    try:
        client.connect("192.168.1.187", 1883, keepalive=10)
    except:
        print "MQTT Broker is not online. Connect later."

    mqtt_looping = True
    print "Looping..."

    client.loop_forever()
    cnt = 0
    while mqtt_looping:
        client.loop()

        cnt += 1
        if cnt > 20:
            try:
                client.reconnect() # to avoid 'Broken pipe' error.
            except:
                time.sleep(1)
            cnt = 0 

    print "quit mqtt thread"
    client.disconnect()

def stop_all(*args):
    global mqtt_looping
    mqtt_looping = False
    client.loop_stop()
    client.disconnect()

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, stop_all)
    #signal.signal(signal.SIGQUIT, stop_all)         # Windows OS not support this function, an error will be raised 
    signal.signal(signal.SIGINT,  stop_all)  # Ctrl-C

    mqtt_client_thread()

    print "exit program"
    sys.exit(0)