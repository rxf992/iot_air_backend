# encoding:utf-8
import datetime
from functools import reduce
from flask import Flask, jsonify, request
from flask_cors import CORS
import json
from mailboxcode import mail, get_mail_code
from Tdengine import *
from gevent import pywsgi  # pip install gevent
from multiprocessing import Pool

app = Flask(__name__)
# 实现跨域访问
cors = CORS(app, resources={r"*": {"origins": "*"}})
conn, td = connect_tdengine()  # 连接数据库
print("db conn is type: ", type(conn))
mail_task_pool = Pool(1)


@app.route('/login', methods=['POST'])
def login():
    global conn, td
    print("!!!!!!!!!!!!!!!!!!!!!!")
    # {'requirement': xxx 'mailbox': '2550294419@qq.com', 'password': '123abc'}
    json_data = request.get_json()
    # conn, td = connect_tdengine()  # 连接数据库
    if json_data['requirement'] == 'registercode':
        print("!!!!!!!!!!!!registercode !!!!!!!!!!")
        res: list = select_account(conn, td, "mailbox", json_data['mailbox'])
        print("check if user exists: ", res)
        # if select_table(conn, td, "account", json_data['mailbox']) != 0:
        #     return json.dumps(False)  ###用户已经存在
        if len(res) > 0:
            print("user exist !!!")
            return json.dumps("user_exist")
        else:
            # coderesult = mail(json_data['mailbox'])
            code = get_mail_code()
            print("!!!! 待发送注册码 code=", code)
            mail_task_pool.apply_async(mail, (json_data['mailbox'], code))
            return json.dumps(code)
    if json_data['requirement'] == 'forgetcode':
        if select_table(conn, td, "account", json_data['mailbox']) != 0:
            code = get_mail_code()
            print("!!!! 待发送注册码 code=", code)
            mail_task_pool.apply_async(mail, (json_data['mailbox'], code))
            return json.dumps(code)
        else:
            return json.dumps(False)
    if json_data['requirement'] == 'register':
        print("!!!!!!!!!!!!register !!!!!!!!!!")
        if create_account_subtable(conn, td, json_data['mailbox'], json_data['password']) == 1:
            return json.dumps(True)
        else:
            return json.dumps(False)
    if json_data['requirement'] == 'forget':
        if alter_accountstable(conn, td, json_data['mailbox'], 'password', json_data['password']) == 1:
            return json.dumps(True)
        else:
            return json.dumps(False)
    if json_data['requirement'] == 'login':
        print("!!!!!!!!!!!!login !!!!!!!!!!")
        result = select_account(conn, td, 'mailbox', json_data['mailbox'])
        if not result:
            return json.dumps(False)
        else:
            if json_data['password'] == result[0][4]:
                if result[0][6] == "":
                    return json.dumps({'power': result[0][5], 'information': {}})
                else:
                    return json.dumps({'power': result[0][5], 'information': eval(result[0][6])})
            else:
                return json.dumps(False)


# 设备地图页
@app.route('/equipmentmap', methods=['POST'])
def equipmentmap():
    global conn, td
    # {'mailbox': 'xxx'}
    json_data = request.get_json()
    # conn, td = connect_tdengine()  # 连接数据库
    # return json.dumps([{'name': '北流市', 'lng': 110.35, 'lat': 22.72}])
    datacount = {}
    data = []
    result = select_account(conn, td, 'mailbox', json_data['mailbox'])
    if not result:
        return json.dumps(False)
    else:
        for count in result:
            datacount[count[1]] = count[2]
        # {'original': True, 'beiliulzx1': True, 'guilinlzx1': False, 'guilinlzx2': False, 'guilinlzx3': False, 'guilin1': True, 'beijing': True, 'beijin1': False, 'beijin2': True, 'beiliu1': True, 'beiliulzx2': True, 'beiliulzx10': True, 'beijin': False, 'beijin3': False}
        for datacountdata in datacount:
            if not datacount[datacountdata]:
                pass
            else:
                result1 = select_equipment(conn, td, "tid", datacountdata)
                if result1:
                    place = result1[0][3]
                    print("query mapaddress by:", place)
                    gps_location = mapaddress(place)
                    time_stamp = result1[0][0]
                    data.append({'address': place,
                                 'tid': datacountdata,
                                 'finalresponsetime': time_change(0, time_stamp),
                                 'lng': gps_location['lng'],
                                 'lat': gps_location['lat']
                                 })
                else:
                    print("!!! cannot find gps location for ", place)
                    pass
        return json.dumps(data)


# 设备数据页
@app.route('/equipmenthome', methods=['POST'])
def equipmenthome():
    global conn, td
    # {'requirement':'onquery', 'tid': 'xxx'}
    json_data = request.get_json()
    # conn, td = connect_tdengine()  # 连接数据库
    if json_data['requirement'] == 'onquery':
        print("get_lastest_sensor_data, tid=", json_data['tid'])
        result = get_lastest_sensor_data(conn, td, json_data['tid'])
        print("get_lastest_sensor_data res = ", result)
        if result:
            data = data_change(result)[0]
            address = get_address_from_tid(conn, td, json_data['tid'])
            print(address)
            data['address'] = address
            print("data = ", data)
            return json.dumps(data)
        else:
            return json.dumps(False)
    if json_data['requirement'] == 'ononeday':
        result = select_table(conn, td, 'equipmentday', json_data['tid'])
        datalist = []
        for i in range(0, len(result)):
            if i % 40 == 0:
                datalist.append(result[i])
        data = data_changelist(datalist)
        return json.dumps(data)
    if json_data['requirement'] == 'ononeweek':
        result = select_table(conn, td, 'equipmentweek', json_data['tid'])
        datalist = []
        for i in range(0, len(result)):
            if i % 240 == 0:
                datalist.append(result[i])
        data = data_changelist(datalist)
        return json.dumps(data)


# 设备控制台
@app.route('/consoleequipment', methods=['POST'])
def consoleequipment():
    global conn, td
    json_data = request.get_json()
    # conn, td = connect_tdengine()  # 连接数据库
    datacount = {}
    timerdata = {'total': 0, 'running': 0, 'stopped': 0}
    data = []
    if json_data['requirement'] == 'onquery':
        # {'requirement':'onquery', 'mailbox': '578761295@qq.com'}
        # result = [{'tid': 'beiliu1', 'status': 1, 'address': '北流', 'finalresponsetime': '2021-4-3'},]
        user_id = json_data['mailbox']
        # print("query equipment belongs to user : ", user_id)
        result = query_device_belongs_to_user_list(conn, td, user_id)
        print("Device_belongs_to_user {0}:{1}".format(user_id, result))
        if not result:
            print("!!! there is no equipment belongs to this user. return empty ")
            return json.dumps(False)
        else:

            # [(datetime.datetime(2022, 7, 17, 16, 25, 4, 42000), 'beiliulzx10', True, 'raoxuefeng@yeah.net', 'qwerpoiu', 'rwx', ''), (xxxxxx)]
            for d in result:
                dev_tid = d[1]
                res = query_equipment_last_status(conn, td, dev_tid)  # tid
                if res:
                    data.append({'tid': dev_tid, 'status': 1, 'address': get_address_from_tid(conn, td, dev_tid),
                                 'finalresponsetime': time_change(0, res[0][0])})
                else:
                    data.append({'tid': dev_tid, 'status': 0, 'address': 'xxx',
                                 'finalresponsetime': 'xxxx-xx-xx xx:xx:xx'})

            return json.dumps(data)

    if json_data['requirement'] == 'timer':
        print("!!! requirement== timer !!! ")
        # {'requirement':'onquery', 'mailbox': '578761295@qq.com'}
        result = query_device_belongs_to_user_list(conn, td, json_data['mailbox'])
        if not result:
            print("not result = true!!!")
            return json.dumps(False)
        else:
            for count in result:
                datacount[count[1]] = count[2]
            # {'original': True, 'beiliulzx1': True, 'guilinlzx1': False, 'guilinlzx2': False, 'guilinlzx3': False,
            # 'guilin1': True, 'beijing': True, 'beijin1': False, 'beijin2': True, 'beiliu1': True, 'beiliulzx2':
            # True, 'beiliulzx10': True, 'beijin': False, 'beijin3': False}
            for datacountdata in datacount:
                print("datacount = ", datacount)
                timerdata['total'] += 1
                if datacount[datacountdata]:
                    timerdata['running'] += 1
                else:
                    timerdata['stopped'] += 1
            return json.dumps(timerdata)
    if json_data['requirement'] == 'onchange':
        # {'requirement': 'onchange', 'tid': 'beiliu2', 'address': '桂林'}
        result = alter_equipmentstable(conn, td, json_data['tid'], 'address', json_data['address'])
        return json.dumps(result)


# 个人中心
@app.route('/consolepersonal', methods=['POST'])
def consolepersonal():
    global conn, td
    json_data = request.get_json()
    print("consolepersonal post data:", json_data)
    # conn, td = connect_tdengine()  # 连接数据库
    result = alter_accountstable(conn, td, json_data['mailbox'], 'information', str(json_data['data']))
    return json.dumps(True)


# 开发者
@app.route('/useradministrator', methods=['POST'])
def useradministrator():
    global conn, td
    json_data = request.get_json()
    print("!! useradministrator: json-data:", json_data)
    # conn, td = connect_tdengine()  # 连接数据库
    datacount = {}
    data = []
    if json_data['requirement'] == 'onquery':
        if check_passwd_match(conn, td, json_data['mailbox'], json_data['password']):
            print("passed.")
            res = query_device_belongs_to_user_dict(conn, td, json_data['mailbox'])
            print("query_device_belongs_to_user_dict result:", res)
            if not res:
                print("no result, return false")
                return json.dumps("no_device")
            print("!!! begin to do select_equipment !!!! ")
            for dev in res:
                result1 = select_equipment(conn, td, "tid", dev['tid'])
                if len(result1) > 0:
                    data.append({'tid': dev['tid'], 'address': result1[0][3], 'tkey': result1[0][4]})

            return json.dumps(data)
        else:
            print("check passwd not passed.")
            return json.dumps(False)

        # return json.dumps([{'tid': '123', 'address': '456', 'tkey': '789'}, {'tid': '987', 'address': '654', 'tkey': '321'}])
    if json_data['requirement'] == 'onadd':
        # {'requirement':'onadd', 'mailbox': 'xxx', 'tid':'xxx', 'address':'xxx', 'tkey':'xxx'}
        result = create_equipment_subtable(conn, td, json_data['tid'], json_data['address'], json_data['tkey'])
        result1 = insert_account_subtable(conn, td, json_data['mailbox'], json_data['tid'], 1)
        if result == 1 and result1 == 1:
            return json.dumps(True)
        else:
            return json.dumps(False)
    if json_data['requirement'] == 'deleteRow':
        #TDEngine 不支持删除和修改，只能通过插入一条新的记录，且后续在查询用户所属设备的时候，且对于表中存在的多条记录，需要以对应tid在表中最新的数据的tidflag 为准
        # 删除设备等于在该用户的表里对该客户的tidflag取反
        # {'requirement':'onquery', 'mailbox': 'xxx', 'tid':'xxx'}
        result = insert_account_subtable(conn, td, json_data['mailbox'], json_data['tid'], 0)
        if result == 1:
            return json.dumps(True)
        else:
            return json.dumps(False)
    if json_data['requirement'] == 'ondel':
        # {'requirement': 'ondel', 'mailbox': 'xxx', 'data': [{'tid': '123', 'address': '456', 'tkey': '789'}, {'tid': '987', 'address': '654', 'tkey': '321'}]}
        # 删除设备等于在该用户的表里对该客户的tidflag取反
        for data in json_data['data']:
            result = insert_account_subtable(conn, td, json_data['mailbox'], data['tid'], 0)
            if result != 1:
                return json.dumps(False)
        return json.dumps(True)


# 管理员
@app.route('/superadministrator', methods=['POST'])
def superadministrator():
    global conn, td
    json_data = request.get_json()
    # conn, td = connect_tdengine()  # 连接数据库
    if json_data['requirement'] == 'onquery':
        print("!!!! superadmin onquery: json_data", json_data)
        # {'requirement':'onquery', 'mailbox': '578761295@qq.com'}
        result = select_account(conn, td)
        print("result=", result)
        if not result:
            print("!!! select account has no result !!!")
            return json.dumps(False)
        else:
            data = []
            for r in result:
                data.append({'mailbox': r[3], 'password': r[4], 'power': r[5]})
            # data = [{'mailbox': "123456789@qq.com", 'password': "123abc", 'power': "r-x"},
            #         {'mailbox': "123", 'password': "123abc", 'power': "rwx"}]
            print("json.dumps(data) =", json.dumps(data))
            return json.dumps(data)
    if json_data['requirement'] == 'addRow':
        # {'requirement':'onadddel', 'mailbox': 'xxx','power':'xxx'}
        result = alter_accountstable(conn, td, json_data['mailbox'], 'power', json_data['power'])
        if result == 1:
            return json.dumps(True)
        else:
            return json.dumps(False)
    if json_data['requirement'] == 'delRow':
        # {'requirement':'onadddel', 'mailbox': 'xxx','power':'xxx'}
        result = alter_accountstable(conn, td, json_data['mailbox'], 'power', json_data['power'])
        if result == 1:
            return json.dumps(True)
        else:
            return json.dumps(False)
    if json_data['requirement'] == 'onchange':
        # {'requirement':'onadddel', 'mailbox': 'xxx','password':'xxx'}
        result = alter_accountstable(conn, td, json_data['mailbox'], 'password', json_data['password'])
        if result == 1:
            return json.dumps(True)
        else:
            return json.dumps(False)


if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=5000)
    server = pywsgi.WSGIServer(('0.0.0.0', 5000), app)
    server.serve_forever()
    mail_task_pool.join()
    print("BYE!!!")
