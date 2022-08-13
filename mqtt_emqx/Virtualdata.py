# encoding:utf-8
import random


# print(int('1446', 16))   十六进制转10进制
# print(hex(5190))   十进制转16进制
def conversion(string):
    return ',' + string.split('x')[1].zfill(4)[0:2] + ',' + string.split('x')[1].zfill(4)[2:4]


# crc计算
def calc_crc(string):
    data = bytearray.fromhex(string)
    crc = 0xFFFF
    for pos in data:
        crc ^= pos
        for i in range(8):
            if (crc & 1) != 0:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    data = hex(((crc & 0xff) << 8) + (crc >> 8))
    return ',' + data.split('x')[1].zfill(4)[0:2] + ',' + data.split('x')[1].zfill(4)[2:4]


def virtualdata():
    data1 = "01,03,14"
    # 70-80
    pm = hex(random.randint(70, 80))
    # 15-40
    temp = hex(random.randint(15, 40))
    # 20-70
    humidity = hex(random.randint(20, 70))
    # <=0.1
    hcho = hex(random.randint(0, 1000))
    # <=0.6
    tvoc = hex(random.randint(0, 1000))
    data2 = data1 + conversion(pm) + conversion(temp) + conversion(humidity) + conversion(hcho) + conversion(tvoc)

    crc = calc_crc(data2.replace(',', ''))
    data = data2 + crc
    return data
print(virtualdata())