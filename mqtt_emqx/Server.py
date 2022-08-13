# encoding:utf-8
import json
import time
import paho.mqtt.client as mqtt
# pip install paho-mqtt
import sys
from Tdengine import connect_tdengine, insert_equipment_subtable, create_equipment_subtable
sys.path.append("..")


# 连接MQTT服务器
def mqtt_connect():
    client.connect(host, port, 60)  # 连接
    client.loop_start()  # 以start方式运行，需要启动一个守护线程，让服务端运行，否则会随主线程死亡
    # client.loop_forever()  # 以forever方式阻塞运行。无限运行


# 订阅主题
def on_subscribe(client, userdata, flags, rc):
    # print("Connected with result code " + str(rc))
    # print("Start server!")
    client.subscribe("air-monitor/#")  # 订阅主题
    client.on_message = on_message  # 消息处理函数


def on_message(client, userdata, msg):
    """
    接收客户端发送的消息
    :param client: 连接信息
    :param userdata:
    :param msg: 客户端返回的消息(---主题(Topic)、负载(payload)---)
    :return:
    {'msg': 'guilinlks1、桂林、01,03,14,00,4f,00,25,00,32,02,71,03,b4,28,8f、990720'}
    """
    print("----> on_message: topic:", msg.topic)
    if msg.topic == "air-monitor/upload":
        payload = json.loads(msg.payload.decode('utf-8'))
        # {'msg': 'guilinlks1、桂林、01,03,14,00,48,00,1b,00,3b,02,7b,01,94,2d,c7、990720'}
        datalist = payload['msg'].split("、")
        data = datalist[2]
        tid = datalist[0]
        address = datalist[1]
        tkey = datalist[3]
        print("----> data: {0}, tid:{1}, address:{2}, tkey:{3}".format(data, tid, address, tkey))
        try:
            if create_equipment_subtable(conn, td, tid, address, tkey) != 1:
                print("%s子表创建失败！！" % tid)
            else:
                if insert_equipment_subtable(conn, td, tid, data) != 1:
                    print("%s数据插入失败！！！" % tid)
        except Exception as err:
            print("!!! Error Occur when doing create subtable or insert data to subtable.")
            conn.close()
            raise err


def on_publish(message: str):
    """
    客户端发布消息
    :param message: 消息主体
    :return:
    """
    payload = {"msg": "%s" % message}
    # publish(主题：Topic; 消息内容)
    client.publish("air-monitor/download", json.dumps(payload, ensure_ascii=False))
    print("Successful send message:", message)


def main():
    client.on_connect = on_subscribe  # 启用订阅模式(补齐4个参数)
    mqtt_connect()
    while True:
        pass


if __name__ == '__main__':
    conn, td = connect_tdengine()  # 连接数据库
    host = "127.0.0.1"
    port = 1883
    client = mqtt.Client(client_id="MainServer")
    # 启动监听
    main()
