# System Overview
## Data Path:
Sensor data --> MQTT Client.py---> EMQX Server ---> Server.py ----> Tdengine.py --->Database(TDEngine)  
iot_air_frontend (httpd deploy @80) <--- Flask(run.py@5000)   <--- Tdengine.py <--- Database(TDEngine)  
其中，部署时，EMQX，TDEngine Server.py, run.py , vue前端代码 均位于公网服务器  
注意修改各个文件中涉及到的ip地址为正确地址

##system dependency:
 - TDEngine Community Version >= 2.6
 - EMQX community version > 4.2
 - Python >=3.7 with packages listed in requirements.txt

# Installation
## TDEngine
download at https://www.taosdata.com/all-downloads/ or build from github source.  
启动：systemctl start taosd  
命令行终端命令：taos  

## EMQX
refer to https://www.emqx.com/zh/try?product=broker
emqx 下载开源版和合适的指令集版本，直接运行  
能够在浏览器中查看到仪表盘页面http://localhost:18083/#/dashboard/overview  
EMQX作为Brocker  
远程访问则需要给防火墙添加端口

## Python packages
```pip3 install -r requirements.txt``` to install.
## frontend deployment
refer to iot_air_frontend project.  
you can use Apache httpd /Nginx as website static server to deploy frontend.  
need to modify backend server URL in request.js. 

header.vue 60行左右 需要修改默认的超级管理员账号：  
```
return {
				headeruser: readData('information').nick != null ? readData('information').nick : readData('currentuser'),
				useradministrator_visible: readData('currentpower') == 'rwx' ? 1 : 0,
				superadministrator_visible: readData('currentuser') == 'raoxuefeng@yeah.net' ? 1 : 0,
				Openclick
			}
```
request.js中需要填写flask服务器所在的ip地址和端口号：  
```
export function request(config) {
	// 创建实例instance对服务器的URl进行请求
	const instance = axios.create({
		method: 'post',
//		baseURL: "http://106.55.27.102:5000",
		baseURL: "http://127.0.0.1:5000",
		timeout: 30*1000
	});
```
#Appendix
## firewall configuration
启动防火墙
```systemctl stop firewalld```  
停止防火墙
```systemctl  start firewalld``` 
为防火墙添加8085的端口
```firewall-cmd --permanent --add-port=8085/tcp```
批量开放8086到8089的所有端口: 
```firewall-cmd --permanent --add-port=8086-8989/tcp```
查看开启了那些端口:
```firewall-cmd --permanent --list-ports```
重新加载防火墙使刚才开放端口生效:  
```firewall-cmd --reload```
删除开放端口:  
```firewall-cmd --permanent --remove-port=8086-8089/tcp```   
删除开放端口时需要和添加时相对应，即批量添加批量删除，单个添加，单个对应删除  
## Apache Configuration
dnf  install ima-evm-utils --nogpgcheck
不使用GPG Check即可。

```dnf install httpd  --nogpgcheck```

apache默认不支持路由，需要作如下配置开启
/etc/httpd/conf/httpd.conf
新增<Directory />配置如下：
```
<Directory />
  AllowOverride All
  RewriteEngine On
  RewriteBase /
  RewriteRule ^index\.html$ - [L]
  RewriteCond %{REQUEST_FILENAME} !-f
  RewriteCond %{REQUEST_FILENAME} !-d
  RewriteRule . /index.html [L]
</Directory>
```
然后打开RewriteEngine，在配置文件中找到 LoadModule相关位置，添加
LoadModule rewrite_module modules/mod_rewrite.so （此步骤可以省略）

重启httpd
```ps -aux | grep httpd | grep -v httpd```
