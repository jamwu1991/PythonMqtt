from websocket_server import WebsocketServer
import json
import logging
import paho.mqtt.client as paho
import ssl
import requests

MqttClient = None
SocketServer = None
SubscribeClientCSharp = None;
ConnectClientCSharp = None
ConnectOperation = None
PythonSocketPort = 55688

# The callback run when a message is pushed by the topic from the broker.
def on_message(client, userdata, msg):
    global SocketServer, SubscribeClientCSharp
    passing_message = "[SUBSCRIBE_RECEIVED_MESSAGE] topic = " + msg.topic + ", payload = " + str(msg.payload.decode())
    SocketServer.send_message(SubscribeClientCSharp, passing_message)

def on_connect(client, userdata, flags, rc):
    returnCodeString = None
    if rc==0:
        returnCodeString = "Connected success & Returned code="
    else:
        returnCodeString = "Connected fail & Returned code="

    connectString = f"[{ConnectOperation.upper()}] {returnCodeString}{rc}"
    SocketServer.send_message(ConnectClientCSharp, connectString)


def PahoMqttConnect(obj, clientFromCsharp, server):
    clientId = obj["clientId"]
    operation = obj["function"]
    broker = obj["broker"]
    port = int(obj["port"])
    tls_CERT_PATH = obj["CaCertPath"]
    tls_CLIENT_PATH = obj["ClientCertPath"]
    tls_CLIENTKEY_PATH = obj["ClientKeyPath"]

    mqttClient = paho.Client(clientId)
    mqttClient.on_connect = on_connect
    if tls_CERT_PATH != None:
        mqttClient.tls_set(ca_certs=tls_CERT_PATH, certfile=tls_CLIENT_PATH,
                       keyfile=tls_CLIENTKEY_PATH, cert_reqs=ssl.CERT_REQUIRED,
                       tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
        mqttClient.tls_insecure_set(False)
    try:
        if mqttClient.is_connected() != True:
            mqttClient.connect(broker, port)
        passingmsg = f"[{operation.upper()}] broker = {broker}, port = {port}, clientId = {clientId} connecting success."
        server.send_message(clientFromCsharp, passingmsg)
    except:
        passingmsg = f"[{operation.upper()}] broker = {broker}, port = {port}, clientId = {clientId} connecting fail."
        server.send_message(clientFromCsharp, passingmsg)
        return None

    return mqttClient

def CsharpClientConnect(client, server):
    passing_message = f"[Socket Connect Check] Operation connecting python project port={PythonSocketPort} "
    server.send_message(client, passing_message)

def MessageReceived(client, server, message):
    global SocketServer, MqttClient, SubscribeClientCSharp, ConnectClientCSharp, ConnectOperation



    if SocketServer == None:
        SocketServer = server

    obj = json.loads(message)
    operation = obj["function"]
    if operation.lower() == "upload":
        print("gogo upload")
        logPath = obj["logPath"]
        url = obj["uploadUrl"]
        headers = obj["headers"]
        verb = obj["verb"]

        headers = headers.lstrip('[').rstrip(']').split(",\"")
        headerDict = {}
        for value in headers:
            value = value.lstrip('\"').rstrip('\"')
            valuelist = value.split(':', 1)
            headerDict[valuelist[0]] = valuelist[1]

        if verb.lower() == "post":
            r = requests.post(url=url,data=open(logPath, "rb"),headers=headerDict)
            print("post = ",r.status_code)
        elif verb.lower() == "put":
            r = requests.put(url=url, data=open(logPath, "rb"), headers=headerDict)
            print("put = ", r.status_code)

    elif operation.lower() == "publish" or operation.lower() == "subscribe":
        topics = obj["topics"]
        payload = obj["payload"]

        if MqttClient == None:
            ConnectClientCSharp = client
            ConnectOperation = operation
            MqttClient = PahoMqttConnect(obj, client, server)

            if MqttClient == None:
                return

        if operation.lower() == "publish":
            for topic in topics:
                MqttClient.publish(topic, payload)
                passing_message = f"[{operation.upper()}] topic = {topic}, payload = {payload}"
                SocketServer.send_message(client, passing_message)

        elif operation.lower() == "subscribe":
            SubscribeClientCSharp = client
            MqttClient.on_message = on_message

            for topic in topics:
                MqttClient.subscribe(topic)
                passing_message = f"[{operation.upper()}] topic = {topic}"
                SocketServer.send_message(client, passing_message)
            MqttClient.loop_forever()

socketServer = WebsocketServer(PythonSocketPort, host='localhost', loglevel=logging.INFO)
socketServer.set_fn_new_client(CsharpClientConnect)
socketServer.set_fn_message_received(MessageReceived)
socketServer.run_forever()