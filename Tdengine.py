# encoding:utf-8
"""
    超级表1 equipmentstable
        表结构（动态，数据上传后不可修改） 时间戳-datetime、数据-data
        表结构（静态，数据后续还可修改） 设备id-tid、地址-address、key-tkey
        data-str   tid-str, address-str, tkey-str
        增:CREATE TABLE IF NOT EXISTS %s USING equipmentstable TAGS ('%s', '%s', '%s');
        删:drop table %s;
        改:(改标签,涉及到表名会复刻表到新表,然后删除旧表)alter table %s set tag %s='%s';
        查:select %s from equipmentstable %s='%s';
    子表: 设备id-tid
        增:insert into %s values (now, '%s');
        删:不允许删除数据
        改:不允许修改数据
        查:select * from %s;

    超级表2 accountstable
        表结构（动态，数据上传后不可修改） 时间戳-datetime、设备id-tid、设备状态-tidflag
        表结构（静态，数据后续还可修改） 用户id-mailbox、用户密码-password、权限-power、个人信息-information
        tid-str, tidflag-int   mailbox-str, password-str, power-str, information-str
        增:CREATE TABLE IF NOT EXISTS %s USING accountstable TAGS ('%s', '%s', '%s', '%s');
        删:drop table %s;
        改:(改标签,涉及到表名会复刻表到新表,然后删除旧表)alter table %s set tag %s='%s';
        查:select %s from accountstable %s='%s';
    子表:
        增:insert into %s values (now, '%s', '%s');
        删:不允许删除数据
        改:不允许修改数据
        查:select * from %s;
"""
import datetime
import taos
import time


def connect_tdengine():
    try:
        conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos", database="air")
        td = conn.cursor()
        server_version = conn.server_info
        print("server_version", server_version)
        client_version = conn.client_info
        print("client_version", client_version)

        # result: taos.TaosResult = conn.query("select * from accountstable")
        #
        # datalist = result.fetch_all()
        # print("datalist =", datalist)

        return conn, td
    except Exception as err:
        print(err)
        return 0, 0


# 连接数据库，返回连接器conn、游标td
def connect_tdengine_2():
    print("!!! connect_tdengine!!!!")
    conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos")
    # conn = taos.connect(host="106.55.27.102", user="root", password="taosdata", config="/etc/taos")
    server_version = conn.server_info
    print("server_version", server_version)
    client_version = conn.client_info
    print("client_version", client_version)

    td = conn.cursor()
    data = []
    # 创建数据库
    try:
        if td.execute("show databases;") != 0:
            datalist = td.fetchall()
            print("show databases:", datalist)
            for i in datalist:
                data.append(i[0])
            if 'air' in data:
                if conn.select_db("air") != 0:
                    return 0, 0
            else:
                if td.execute("create database air;") != 0:
                    return 0, 0
                else:
                    if td.execute("use air;") != 0:
                        return 0, 0
        if create_stable(conn, td) == 1:
            return conn, td
        else:
            return 0, 0
    except Exception as err:
        conn.close()
        raise err


# 创建超级表,成功则返回1，失败则返回0
def create_stable(conn: taos.TaosConnection, td):
    try:
        equipmentflag = td.execute("""CREATE STABLE IF NOT EXISTS equipmentstable(
                                        datetime TIMESTAMP,
                                        data binary(75))
                                        TAGS(
                                        tid binary(15),
                                        address nchar(15),
                                        tkey binary(10));""")
        accountflag = td.execute("""CREATE STABLE IF NOT EXISTS accountstable(
                                        datetime timestamp,
                                        tid binary(15),
                                        tidflag bool)
                                        TAGS(
                                        mailbox binary(30),
                                        password binary(20),
                                        power binary(10),
                                        information binary(100));""")
        # CREATE STABLE [IF NOT EXISTS] stb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]) TAGS (tag1_name tag_type1, tag2_name tag_type2 [, tag3_name tag_type3]);
        if equipmentflag == 0 and accountflag == 0:
            return 1
        else:
            return 0
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur")


def get_table_name(mailbox: str):
    table_name = str(mailbox).replace('@', '_').replace('.', '_')
    print("table name is :", table_name)
    return table_name


# 增：创建用户子表，表名为stable+用户名，一个用户一张表记录全部设备,成功则返回1，失败则返回0。
def create_account_subtable(conn: taos.TaosConnection, td, mailbox, password, power="r-x", information=""):
    try:
        # conn.execute("DROP TABLE IF EXIST %s ;" % ('table' + get_table_name(mailbox)))
        res = conn.execute("CREATE TABLE %s USING accountstable TAGS ('%s', '%s', '%s', '%s');" % (
            'table' + get_table_name(mailbox), mailbox, password, power, information))
        print("create table res = ", res)
        res = insert_account_subtable(conn, td, mailbox, "original",
                                      0)  # tidflag = 0, no device bonded to this account yet
        print("insert table res = ", res)
        return res
        # result = select_table(conn, td, 'account', mailbox)
        # if result == 0:
        #     if conn.execute("CREATE TABLE %s USING accountstable TAGS ('%s', '%s', '%s', '%s');" % (
        #             'table' + get_table_name(mailbox), mailbox, password, power, information)) != 0:
        #         return 0
        #     else:
        #         if insert_account_subtable(conn, td, mailbox, "original", 0) == 0:
        #             return 0
        #         else:
        #             return 1
        # elif result is None:
        #     if insert_account_subtable(conn, td, mailbox, "original", 0) == 0:
        #         return 0
        #     else:
        #         return 1
        # else:
        #     return 1
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur")


# 增：用户子表插入数据,成功则返回1，失败则返回0
def insert_account_subtable(conn: taos.TaosConnection, td, mailbox, tid="", tidflag=1):
    try:
        if conn.execute(
                "insert into %s values (now, '%s', %s);" % ('table' + get_table_name(mailbox), tid, tidflag)) != 1:
            return 0
        else:
            return 1
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur")


# 增：创建设备子表，表名为设备名成功则返回1，失败则返回0。
def create_equipment_subtable(conn: taos.TaosConnection, td, tid, address, tkey):
    try:
        result = select_table(conn, td, 'equipment', tid)
        if result == 0:
            print("!!! can not find table subtable equipment-%s" % tid)
            if td.execute("CREATE TABLE %s USING equipmentstable TAGS ('%s', '%s', '%s');" % (
                    tid, tid, address, tkey)) != 0:
                return 0
            else:
                if insert_equipment_subtable(conn, td, tid, "original") == 0:
                    return 0
                else:
                    return 1
        elif result is None:
            if insert_equipment_subtable(conn, td, tid, "original") == 0:
                return 0
            else:
                return 1
        else:
            return 1
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur")


# 增：设备子表插入数据,成功则返回1，失败则返回0
def insert_equipment_subtable(conn: taos.TaosConnection, td, tid, data):
    try:
        if td.execute("insert into %s values (now, '%s');" % (tid, data)) != 1:
            return 0
        else:
            return 1
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur")


# 删：只能删除设备子表，用户不可改变
def drop_equipment_subtable(conn, td, tid):
    try:
        if td.execute("drop table %s;" % tid) != 0:
            return 0
        else:
            return 1
    except Exception as err:
        conn.close()
        raise err


# 改：修改accounttable的子表的password、power、information，成功则返回1，失败则返回0
def alter_accountstable(conn: taos.TaosConnection, td, mailbox, requirement, value):
    try:
        if requirement == "password":
            print("password")
            if td.execute("alter table %s set tag password=\"%s\";" % ('table' + get_table_name(mailbox), value)) != 0:
                return 0
            else:
                return 1
        if requirement == "power":
            print("power")
            if td.execute("alter table %s set tag power=\"%s\";" % ('table' + get_table_name(mailbox), value)) != 0:
                return 0
            else:
                return 1
        if requirement == "information":
            print("information: ")
            print("exec: alter table %s set tag information='%s';" % ('table' + get_table_name(mailbox), value))
            if td.execute(
                    "alter table %s set tag information=\"%s\";" % ('table' + get_table_name(mailbox), value)) != 0:
                return 0
            else:
                return 1
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur")


# 改：修改equipmentstable的子表的tid、address、tkey，tid需要修改表名，可能需要移植表到新表，然后删除旧表，成功则返回1，失败则返回0
def alter_equipmentstable(conn: taos.TaosConnection, td, tid, requirement, value):
    try:
        if requirement == "tid":
            datalist = select_equipment(conn, td, "tid", tid)
            if td.execute("create table if not exists %s using equipmentstable tags ('%s', '%s', '%s');" % (
                    value, value, datalist[0][3], datalist[0][4])) != 0:
                return 0
            else:
                for data in datalist:
                    if td.execute("insert into %s values (%s, '%s')" % (value, time_change(1, data), data[1])) != 1:
                        return 0
                    else:
                        pass
                if td.execute("drop table %s;" % tid) != 0:
                    return 0
                else:
                    return 1
        if requirement == "address":
            if td.execute("alter table %s set tag address='%s';" % (tid, value)) != 0:
                return 0
            else:
                return 1
        if requirement == "tkey":
            if td.execute("alter table %s set tag tkey='%s';" % (tid, value)) != 0:
                return 0
            else:
                return 1
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur")


def query_device_belongs_to_user_list(conn: taos.TaosConnection, td, user_id):
    print("!!! TDengine: query_device_belongs_to_user_list !!!")
    try:
        # select * from accountstable where mailbox = "raoxuefeng@yeah.net" and tidflag = "true" ;
        # TDEngine 不支持删除和修改，只能通过插入一条新的记录，且后续在查询用户所属设备的时候，且对于表中存在的多条记录，
        # 需要以对应tid在表中最新的数据的tidflag 为准
        # 1. 首先得知道有几个设备tid
        # 2. 对于每个tid，找到其最新的一条记录，并根据最新的记录tidflag来判断是否应当添加进最终返回的结果当中。
        tid_result: taos.TaosResult = conn.query("SELECT DISTINCT tid from accountstable WHERE mailbox = '%s' AND tid != 'original'" % user_id)
        tids = tid_result.fetch_all_into_dict()
        print("user:%s has tids: %s" % (user_id, tids))
        final_result = []
        for tid in tids:
            res: taos.TaosResult = conn.query("select last(*) from accountstable where mailbox ='%s'AND tid = '%s';" % (user_id, tid['tid']))
            devices = res.fetch_all()
            print("devices:", devices)
            for dev in devices:
                tid = dev[1]
                tid_flag = dev[2]
                print("dev: %s, tidflag=%s" % (tid, tid_flag))
                if tid_flag:
                    final_result.append(dev)

        print("final_result:", final_result)
        return final_result
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur")


def query_device_belongs_to_user_dict(conn: taos.TaosConnection, td, user_id):
    print("!!! TDengine: query_device_belongs_to_user_dict !!!")
    try:
        # select * from accountstable where mailbox = "raoxuefeng@yeah.net" and tidflag = "true" ;
        # TDEngine 不支持删除和修改，只能通过插入一条新的记录，且后续在查询用户所属设备的时候，且对于表中存在的多条记录，
        # 需要以对应tid在表中最新的数据的tidflag 为准
        # 1. 首先得知道有几个设备tid
        # 2. 对于每个tid，找到其最新的一条记录，并根据最新的记录tidflag来判断是否应当添加进最终返回的结果当中。
        tid_result: taos.TaosResult = conn.query("SELECT DISTINCT tid from accountstable WHERE mailbox = '%s' AND tid != 'original'" % user_id)
        tids = tid_result.fetch_all_into_dict()
        print("user:%s has tids: %s" % (user_id, tids))
        final_result = []
        for tid in tids:
            res: taos.TaosResult = conn.query("select last(*) from accountstable where mailbox ='%s'AND tid = '%s';" % (user_id, tid['tid']))
            devices = res.fetch_all_into_dict()
            for dev in devices:
                print("dev: %s, tidflag=%s" % (dev['tid'], dev['tidflag']))
                if dev['tidflag']:
                    final_result.append(dev)

        print("final_result:", final_result)
        return final_result
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur")


def check_passwd_match(conn, td, mailbox, passwd):
    print("!!! TDengine: check_passwd !!!")
    try:

        # print("conn type:", conn)
        result: taos.TaosResult = conn.query(
            "select * from accountstable where mailbox ='%s' and password = '%s' ;" % (mailbox, passwd))
        datalist = result.fetch_all()
        print("datalist = ", datalist)
        if len(datalist) > 0:
            print("user and pass match found.")
            return True
        else:
            return False
    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
        return False
    except BaseException as other:
        print("exception occur:", other)
        return False


# 查：查询用户表，需要什么列表就返回什么列表
def select_account(conn, td, requirement="", value=""):
    print("!!! TDengine: select_account !!!")
    try:
        result: taos.TaosResult = None
        if requirement == "" and value == "":
            result = conn.query("select * from accountstable where tidflag == false")

        else:
            print("conn type:", conn)
            result = conn.query("select * from accountstable where %s='%s'" % (requirement, value))

        datalist = result.fetch_all()
        print("datalist = ", datalist)
        return datalist

    except taos.Error as e:
        print(e)
        print("exception class: ", e.__class__.__name__)
        print("error number:", e.errno)
        print("error message:", e.msg)
    except BaseException as other:
        print("exception occur:", other)


def query_equipment_last_status(conn: taos.TaosConnection, td, tid):
    try:
        result: taos.TaosResult = conn.query(
            "SELECT LAST(*) FROM equipmentstable where tid = '%s';" % tid)  # there will be only one row .
        datalist = result.fetch_all()
        return datalist
    except Exception as err:
        print(err)
        return


# 查：查询设备表，需要什么列表就返回什么列表，比如你查找相同tid返回相同tid数据
def select_equipment(conn: taos.TaosConnection, td, requirement="", value=""):
    try:
        if requirement == "" and value == "":
            result: taos.TaosResult = conn.query("select * from equipmentstable;")
            datalist = result.fetch_all()
            return datalist
        else:
            result: taos.TaosResult = conn.query("SELECT * from equipmentstable where %s='%s';" % (requirement, value))
            datalist = result.fetch_all()
            return datalist
    except Exception as err:
        print("select equipment err:", err)
        # conn.close()
        raise err


def get_address_from_tid(conn: taos.TaosConnection, td, tid):
    try:
        result: taos.TaosResult = conn.query("SELECT address from equipmentstable where tid ='%s' LIMIT 1;" % tid)
        datalist = result.fetch_all()
        # [('桂林市',)]
        print("datalist = ", datalist)
        address = datalist[0][0]
        print("address: ", address)
        return address
    except Exception as err:
        print("get_address_from_tid %s err:%s" % (tid, err))
        # conn.close()
        raise err


# 查：查询表内数据返回
def select_table(conn: taos.TaosConnection, td, requirement="", value=""):
    try:
        if requirement == 'account':
            td.execute("select * from %s;" % ('table' + get_table_name(value)))
            datalist = td.fetchall()
        if requirement == 'equipment':
            td.execute("select * from %s;" % value)
            datalist = td.fetchall()
        if requirement == 'equipmentday':
            # 一天5760 一周40320
            # td.execute("select * from %s;" % value)
            # td.execute("select * from %s where datetime>NOW-1d" % value)
            td.execute("SELECT * FROM %s ORDER BY datetime DESC LIMIT 5760;" % value)
            datalist = td.fetchall()
        if requirement == 'equipmentweek':
            # 一天5760 一周40320
            # td.execute("select * from %s;" % value)
            # td.execute("select * from %s where datetime>NOW-1w" % value)
            td.execute("SELECT * FROM %s ORDER BY datetime DESC LIMIT 40320;" % value)
            datalist = td.fetchall()
        return datalist
    except Exception as err:
        print("Exception:", err)
        return 0
        # conn.close()
        # raise err


# 查：查询表内数据返回
def select_table_Finaldata(conn: taos.TaosConnection, td, tid):
    try:
        # SELECT * FROM user LIMIT 1;  # 第一行数据
        # SELECT * FROM beiliulzx1 ORDER BY datetime DESC LIMIT 1;  # 最后一行数据
        result: taos.TaosResult = conn.query("SELECT * FROM %s ORDER BY datetime DESC LIMIT 1;" % tid)
        data = result.fetch_all()
        return data
    except Exception as err:
        print("select_table_Finaldata err:", err)
        # conn.close()
        raise err


# 查：查询表内数据返回
def get_lastest_sensor_data(conn: taos.TaosConnection, td, tid):
    try:
        # SELECT * FROM user LIMIT 1;  # 第一行数据
        # SELECT * FROM beiliulzx1 ORDER BY datetime DESC LIMIT 1;  # 最后一行数据
        result: taos.TaosResult = conn.query(
            "SELECT LAST(*) FROM equipmentstable WHERE tid = '%s';" % tid)  # 此方法返回的内容不带静态标签的值
        data = result.fetch_all()
        if data:
            return data
        else:
            return None
    except Exception as err:
        print("get_lastest_sensor_data err:", err)
        # conn.close()
        raise err


# 13位时间戳正反转
# 日期正转返回时间戳str（传进来列表list）   flag=1   (datetime.datetime(2021, 3, 6, 17, 40, 54, 727000), 'lkslkslks', 'lks', 'llks', '234567')
# 时间戳反转返回日期str（传进来时间戳datetime.datetime）   flag=0   datetime.datetime(2021, 3, 6, 17, 40, 54, 727000)
def time_change(flag, data):
    if flag == 1:
        dt = datetime.datetime.strptime(str(str(data[0]).split('.')[0]), '%Y-%m-%d %H:%M:%S')
        # 10位，时间点相当于从1.1开始的当年时间编号
        date_stamp = str(int(time.mktime(dt.timetuple())))
        # 3位，微秒
        data_microsecond = str("%06d" % dt.microsecond)[0:3]
        # date_stamp是个列表，将每个date_stamp逐个append到列表列表中再写入到数据库里，或者每个直接写入
        date_stamp = date_stamp + data_microsecond
        return date_stamp
    if flag == 0:
        return data.strftime("%Y-%m-%d %H:%M:%S")


# 设备数据显示处理
def data_change(datalist):
    dataresult = []
    for data in datalist:
        date = time_change(0, data[0])
        data = data[1].split(',')
        mp25 = int(data[3] + data[4], 16)
        temperature = int(data[5] + data[6], 16)
        humidity = int(data[7] + data[8], 16)
        hcho = int(data[9] + data[10], 16) / 1000
        tvoc = int(data[11] + data[12], 16) / 1000
        dataresult.append(
            {'date': date, 'pm25': mp25, 'temperature': temperature, 'humidity': humidity, 'hcho': hcho, 'tvoc': tvoc})
    return dataresult


# 折线图数据处理
def data_changelist(datalist):
    dataresult = {}
    datelist = []
    pm25list = []
    temperaturelist = []
    humiditylist = []
    hcholist = []
    tvoclist = []
    for data in datalist:
        date = time_change(0, data[0])
        data = data[1].split(',')
        pm25 = int(data[3] + data[4], 16)
        temperature = int(data[5] + data[6], 16)
        humidity = int(data[7] + data[8], 16)
        hcho = int(data[9] + data[10], 16) / 1000
        tvoc = int(data[11] + data[12], 16) / 1000

        datelist.append(date)
        pm25list.append(pm25)
        temperaturelist.append(temperature)
        humiditylist.append(humidity)
        hcholist.append(hcho)
        tvoclist.append(tvoc)

    dataresult['date'] = datelist[::-1]
    dataresult['pm25'] = pm25list[::-1]
    dataresult['temperature'] = temperaturelist[::-1]
    dataresult['humidity'] = humiditylist[::-1]
    dataresult['hcho'] = hcholist[::-1]
    dataresult['tvoc'] = tvoclist[::-1]
    return dataresult


# 地图标点
# https://blog.csdn.net/qq_31635851/article/details/108727612
def mapaddress(address):
    maplist = dict({"桂林市": (110.260920147, 25.262901246),
                    "南宁市": (108.297233556, 22.8064929356),
                    "北海市": (109.122627919, 21.472718235),
                    "柳州市": (109.42240181, 24.3290533525),
                    "梧州市": (111.30547195, 23.4853946367)
                    })
    loc = maplist.get(address)
    if loc:
        return {'lng': float(loc[0]), 'lat': float(loc[1])}
    else:
        return {'lng': 0, 'lat': 0}


def mapaddress_by_file(address):
    maplist = []
    f = open("./mapcity.txt", 'r', encoding='utf-8')
    lines = f.readlines()
    for line in lines:
        maplist.append(line.split())
    for data in maplist:
        if address in data[2]:
            return {'lng': float(data[3]), 'lat': float(data[4])}
    else:
        return {'lng': 0, 'lat': 0}
