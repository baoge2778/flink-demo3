# flink-demo3
# linux环境下Flink读取Kafka消息同步到ElasticSearch
## 1.	安装Kafka

当前服务器版本信息：Linux localhost.localdomain 3.10.0-327.el7.x86_64 #1 SMP Thu Nov 19 22:10:57 UTC 2015 x86_64 x86_64 x86_64 GNU/Linux

### 1.1.	下载软件包
官网下载2.4.0版本
http://kafka.apache.org/downloads
### 1.2.	上传并解压
放在/home/es/目录下
	tar -zxvf kafka_2.12-2.4.0.tgz
### 1.3.	后台启动自带的zookeeper
kafka使用zookeeper管理服务节点，如果没有安装zookeeper，可以使用kafka功能目录bin/zookeeper-server-start.sh脚本启动一个单节点的zookeeper实例：

	bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
加上-daemon参数就不会将启动日志打印到控制台了，然后通过jps（查看java进程命令）查看
	[root@localhost kafka_2.12-2.4.0]# jps
3636 QuorumPeerMain
4474 Jps
出现QuorumPeerMain就说明zookeeper启动成功了。zookeeper的默认监听端口是2181。
### 1.4.	后台启动kafka
	bin/kafka-server-start.sh -daemon config/server.properties
加上-daemon参数就不会将启动日志打印到控制台了，然后通过jps（查看java进程命令）查看：
	[root@localhost ~]# jps
3636 QuorumPeerMain
5334 Kafka
5382 Jps
出现Kafka就说明kafka启动成功了。kafka默认监听端口是9092。
### 1.5.	创建一个topic
首先创建一个名为test的topic来接收和发送消息。
通过以下命令创建test主题：
	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
其中：
--partitions 1指定该topic只有一个分区；
--replication-factor 1指定该分区只有一个副本处理消息。
可以执行以下命令查看test主题是否创建成功：
	bin/kafka-topics.sh --list --zookeeper localhost:2181

### 1.6.	用Kafka的console-producer在topic test 生产消息
运行kafka提供的生产者脚本，发送消息，默认情况下，每一行都将作为一个独立的消息被发送：
	[root@localhost kafka_2.12-2.4.0]# bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
然后键入你想要发送的消息，按回车，消息就会发送到test这个topic上，被订阅的消费者接收。如下图：
 

### 1.7.	用Kafka的console-consumer 消费topic test的消息
	[root@localhost kafka_2.12-2.4.0]# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
结果如下：
 

## 2.	安装ElasticSearch

### 2.1.	下载软件包
官网下载6.8.2版本
https://www.elastic.co/cn/downloads/past-releases/elasticsearch-6-8-2
### 2.2.	上传并解压
放在/home/es/目录下
	tar -zxvf elasticsearch-6.8.2.tar.gz
### 2.3.	修改配置文件
进入解压后的config目录，
	cd /home/elk/elasticsearch-6.8.2/config
修改elasticsearch.yml配置文件，找到下面两项配置，开放出来（删除前面的#符号，172.16.0.140是内网IP）
network.host: 172.16.0.140
http.port: 9200
 
### 2.4.	启动ES 
注意，es启动，不能用root启动权限启动，需要创建用户再启动
//创建用户分组
	groupadd esgroup  
//给分组添加一个用户 
	useradd esuser -g esgroup -p password  
//给用户添加操作elasticsearch-6.8.2的操作权限  
	chown -R esuser:esgroup  elasticsearch-6.8.2 
//切换用户
	su esuser
到elasticsearch/bin 目录下
	./elasticsearch  
可以用
	./elasticsearch  -d  
后台启动elasticsearch 。

访问：http://172.16.0.140:9200/ 启动成功。 
 


### 2.5.	启动es问题处理

#### 2.5.1.	max file descriptors [4096] for elasticsearch process is too low, increase to at least [65536]
　　	每个进程最大同时打开文件数太小，可通过下面2个命令查看当前数量
	ulimit -Hn
	ulimit -Sn
修改/etc/security/limits.conf文件，增加配置，用户退出后重新登录生效
* soft nofile 65536
* hard nofile 65536
 
 
#### 2.5.2.	max number of threads [3818] for user [es] is too low, increase to at least [4096]
　　	问题同上，最大线程个数太低。修改配置文件/etc/security/limits.conf（和问题1是一个文件），增加配置
* soft nproc 4096
* hard nproc 4096
　　	可通过命令查看
	ulimit -Hu
	ulimit -Su
 
修改后的文件：
 
#### 2.5.3.	max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]
　　	修改/etc/sysctl.conf文件，增加配置vm.max_map_count=262144
	vi /etc/sysctl.conf 
	sysctl -p
　　	执行命令sysctl -p生效



## 3.	安装kibana

### 3.1.	下载软件包
官网下载6.8.2版本
https://www.elastic.co/cn/downloads/past-releases/kibana-6-8-2
### 3.2.	上传并解压
放在/home/es/目录下
	tar -zxvf kibana-6.8.2-linux-x86_64.tar.gz

### 3.3.	修改配置文件
修改config/kibana.yml配置文件
elasticsearch.hosts: "http://172.16.0.140:9200"
server.host: "0.0.0.0"
server.port: 5601

### 3.4.	启动kibana 
Bin目录下 ./kibana 
也可以nohup ./kibana &  后台启动
访问：http://172.16.0.140:5601/app/kibana 启动成功。 


## 4.	安装flink

### 4.1.	下载软件包
官网下载1.9.0版本
https://archive.apache.org/dist/flink/flink-1.9.0/
### 4.2.	上传并解压
放在/home/es/目录下
	tar -zxvf flink-1.9.0-bin-scala_2.11.tgz

### 4.3.	启动flink 
Bin目录下 ./start-cluster.sh 启动
访问：http://172.16.0.140:8081/#/overview  启动成功。 
