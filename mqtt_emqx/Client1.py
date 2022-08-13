# encoding:utf-8
import json
import paho.mqtt.client as mqtt
import time

import serial


# 连接MQTT服务器
def mqtt_connect():
    client.connect(host, port, 60)
    client.loop_start()


# 订阅主题
def on_subscribe(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    print("Start server!")
    client.subscribe("air-monitor/download")
    client.on_message = on_message


# 接收服务端发送的消息
def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode('utf-8'))
    print(msg.topic, payload["msg"])


# 客户端发布消息
def on_publish(message: str):
    payload = {"msg": "%s" % message}
    client.publish("air-monitor/upload", json.dumps(payload, ensure_ascii=False))
    print("Successful send message:", message)


# 读取串口数据转码
def serialdata():
    data = ''
    i = 0
    ser = serial.Serial('COM3', 9600, timeout=0.5)
    while True:
        s = ser.read().hex()
        if s != '':
            if i != 14:
                data = data + str(s) + ','
            if i == 14:
                data = data + str(s)
                i = 0
                break
            i += 1
    return clientid + '、' + place + '、' + str(data) + '、' + key


def main():
    client.on_connect = on_subscribe
    mqtt_connect()
    while True:
        data = serialdata()
        on_publish(data)
        time.sleep(5)


if __name__ == '__main__':
    host = "106.55.27.102"
    port = 1883
    clientid = "beiliulzx1"
    place = "玉林市"
    key = "yulinlzx1"
    client = mqtt.Client(client_id=clientid)
    # 启动监听
    main()
