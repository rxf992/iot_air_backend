# encoding:utf-8
# 发送纯文本
import smtplib
# 发送标题
from email.header import Header
# 邮件正文
from email.mime.text import MIMEText
import random


# https://www.jianshu.com/p/129101b726b6?utm_campaign=maleskine&utm_content=note&utm_medium=seo_notes&utm_source=recommendation

def mail_code():
    code = ""
    x = random.randint(3, 12)
    y = x - 3
    while x != y:
        if x % 3 == 0:
            code += str(random.randint(0, 9))
            code += str(random.randint(0, 9))
        if x % 3 == 1:
            code += str(chr(random.randint(65, 90)))
            code += str(chr(random.randint(65, 90)))
        if x % 3 == 2:
            code += str(chr(random.randint(97, 122)))
            code += str(chr(random.randint(97, 122)))
        x -= 1
    return code


def sendmail(user, pwd, sender, receiver, content, title):
    """
    说明：此函数实现发送邮件功能。
    :param user: 用户名
    :param pwd: 授权码
    :param sender: 发送方
    :param receiver: 接收方
    :param content: 邮件的正文
    :param title: 邮件的标题
    :return:
    """
    mail_host_qq = "smtp.qq.com"  # qq的SMTP服务器
    mail_host  = "smtp.yeah.net"
    # 第一部分：准备工作
    # 1.将邮件的信息打包成一个对象
    message = MIMEText(content, "plain", "utf-8")  # 内容，格式，编码
    # 2.设置邮件的发送者
    message["From"] = sender
    # 3.设置邮件的接收方
    message["To"] = receiver
    # join():通过字符串调用，参数为一个列表
    # message["To"] = ",".join(receiver)
    # 4.设置邮件的标题
    message["Subject"] = title
    print("beginning to send mail via smtp")
    # 第二部分：发送邮件
    try:
        # 1.启用服务器发送邮件
        # 参数：服务器，端口号
        smtpObj = smtplib.SMTP_SSL(host=mail_host, port=465, timeout=5*1000)
        # smtpObj.connect(mail_host, 25)  # 25/ 465 / 587 为 SMTP 端口号
        print('已经连接,正在登录')
        # 2.登录邮箱进行验证
        # 参数：用户名，授权码
        smtpObj.login(user, pwd)
        print('已经登录，正在发送')
        # 3.发送邮件
        # 参数：发送方，接收方，邮件信息
        smtpObj.sendmail(sender, receiver, message.as_string())
        # 发送成功
        print("send mail success!!!")
        return 1
    except Exception as err:
        print("Exception sending mail:", err)
        return 0


def get_mail_code():
    code = mail_code()
    return str(code)


def mail(mail_receiver, code):
    # code = mail_code()
    print("本次的验证码 :", code)
    mail_user_qq = "xxxx@qq.com"  # 用户名
    mail_user = "xxx@bbbb"
    mail_pass_qq = ""  #qq发件人邮箱授权码
    mail_pass = "" # raoxuefeng@yeah.net
    mail_sender_qq = "xxxx@qq.com"  # 发送方
    mail_sender = "xxx@bbbb"
    mail_receiver = mail_receiver  # 接收方
    email_content = "本次登录的验证码是：%s" % code  # 正文
    email_title = "[验证码]户外环境监测可视化平台"  # 标题
    if sendmail(mail_user, mail_pass, mail_sender, mail_receiver, email_content, email_title) == 1:
        print("send mail code success:", code)
        return str(code)
    else:
        print("ERR: send mail code failed:", code)
        return "send_fail"
