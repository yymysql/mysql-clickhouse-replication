#!/usr/bin/python
# -*- coding:utf-8 -*-
# write by yayun 2019/03/10
# 增量同步mysql数据到clickhouse

import redis
import datetime
import time
import MySQLdb
import decimal
import re
import json
import gc
import logging
import ConfigParser
import os
import socket
import argparse
import clickhouse_driver
import sys
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
import smtplib
import MySQLdb.cursors
from email.mime.text import MIMEText
from email.message import Message
from email.header import Header

reload(sys) 
sys.setdefaultencoding('utf8')

'''
update转成insert
delete from t1 where id=19;          序号: 1
insert into t1 select 19;            序号：2
update t1 set name=cc wehre id=10;   序号：3
insert into t1 select 20;            序号：4
delete from t1 where id=20;          序号：5

如果先执行delte，再insert，最后结果是10,20 。 但是正确的结果应该是只有10.
解决方法：
delete列表的那些记录，根据主键 xx ，删除insert列表里主键和xx相同且序号小于delete记录的序号。对于insert列表里面主键相同，序号小的也需要删除。

delete=[{1：10},{5：20}]
insert=[{3：10},{2:10},{4:20}]
insert list 4:20记录需要删除，2:10需要删除

执行顺序 delete，insert

模块安装
yum -y install python-pip
yum -y install python-devel
pip install MySQL-python
pip install mysql-replication
pip install clickhouse-driver
pip install redis

如果选择pos点存入文件,文件格式如下
[log_position]
filename = mysql-bin.000066
position = 3977081

注意:
1. 需要调整mysql的参数(否则在有大批量数据的时候会导致接收数据丢失)
set global net_read_timeout=1800
set global net_write_timeout=1800

2. 同步的表如果字段有数字开头，比如123_is_old,那么程序会抛错。

'''


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


class my_db():
    def __init__(self):
        self.db_host = cnf['master_server']['host']
        self.db_port = int(cnf['master_server']['port'])
        self.db_user = cnf['master_server']['user']
        self.db_pwd  = cnf['master_server']['passwd']
        try:
            self.conn = self.getConnection()
        except Exception as error:
            message="mysql connect fail: %s,server ip：%s:%s" % (error,self.db_host,self.db_port)
            logger.error(message)
            exit(1)

    def getConnection(self):
        return MySQLdb.connect(
               host=self.db_host,
               port=int(self.db_port),
               user=self.db_user,
               passwd=self.db_pwd,
               connect_timeout=5,
               cursorclass = MySQLdb.cursors.DictCursor,
               charset='utf8'
               )

    # 关闭连接
    def close_db(self):
        self.conn.close()

    def slave_status(self):
        # 获取复制信息
        try:
            cursor=self.conn.cursor()
            cursor.execute('show slave status')
            result=cursor.fetchall()
            cursor.close()
        except MySQLdb.Warning,w:
            resetwarnings()
        else:
            if result:
                return result[0]

    def get_pri(self,db,table):
        # 获取自增主键信息
        primary_key=[]
        sql="select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{0}' and TABLE_NAME='{1}' and COLUMN_KEY='PRI' and EXTRA='auto_increment'".format(db,table)
        try:
            cursor=self.conn.cursor()
            cursor.execute(sql)
            result=cursor.fetchall()
            cursor.close()
        except MySQLdb.Warning,w:
            resetwarnings()
        else:
            if result:
                for data in result:
                    if colum_lower_upper:
                        primary_key.append(data['COLUMN_NAME'].upper())
                    else:
                        primary_key.append(data['COLUMN_NAME'].lower())
            return primary_key

    def check_table_exists(self,db,table):
        sql="select count(*) as count from information_schema.tables where TABLE_SCHEMA='{0}' and TABLE_NAME='{1}'".format(db,table)
        try:
            cursor=self.conn.cursor()
            cursor.execute(sql)
            result=cursor.fetchall()
            cursor.close()
        except MySQLdb.Warning,w:
            resetwarnings()
        return result[0]['count']


class my_redis:
    def __init__(self):
        self.host=cnf['redis_server']['host']
        self.port=int(cnf['redis_server']['port'])
        self.passwd=cnf['redis_server']['passwd']
        self.log_pos_prefix=cnf['redis_server']['log_pos_prefix']
        self.server_id=cnf['master_server']['server_id']
        self.key="{0}{1}".format(self.log_pos_prefix,self.server_id)
        self.connection=self.redis_conn()

    def redis_conn(self):
        try:
            self.connection=redis.StrictRedis(host=self.host, port=int(self.port),password=self.passwd)
            return self.connection
        except Exception as error:
            message='redis connect fail %s' % (error)
            logger.error(message)
            exit(1)

    def get_log_pos(self):
        try:
            ret = self.redis_conn().hgetall(self.key)
            return ret.get('log_file'), ret.get('log_pos')
        except Exception as error:
            logger.error("从redis 读取binlog pos点错误")
            logger.error(error)
            exit(1)

    def set_log_pos(self,*args):
        try:
            if args[0] == 'slave':
                self.redis_conn().hmset(self.key,{'log_pos':args[2], 'log_file':args[1]})
            elif args[0] == 'master':
                self.redis_conn().hmset(self.key,{'master_host':args[1],'master_port':args[2],'relay_master_log_file':args[3],'exec_master_log_pos':args[4]})
        except Exception as error:
            logger.error("binlog pos点写入redis错误")
            logger.error(error)
            exit(1)


class mark_log:
    def __init__(self):
        try:
            self.file=cnf['log_position']['file']
        except Exception as error:
            logger.error("%s 获取存放pos点的文件错误." % (error))
            exit(1)
        self.open_file()

    def open_file(self):
        try:
            self.config = ConfigParser.ConfigParser()
            self.config.readfp(open(self.file,'rw'))
        except Exception as error:
            logger.error("%s 文件打开错误" % (file)) 
            logger.error(error)
            exit(1)

    def get_log_pos(self):
        try:
            return self.config.get('log_position','filename'),int(self.config.get('log_position','position')),
        except Exception as error:
            logger.error("从文件 读取binlog pos点错误")
            logger.error(error)
            sys.exit(1)

    def set_log_pos(self,*args):
        try:
            if args[0] == 'slave':
                self.config.set('log_position','filename',args[1])
                self.config.set('log_position','position',str(args[2]))
                self.config.write(open(self.file, 'w'))
            elif args[0] == 'master':
                self.config.set('log_position','master_host',args[1])
                self.config.set('log_position','master_port',args[2])
                self.config.set('log_position','master_filename',args[3])
                self.config.set('log_position','master_position',str(args[4]))
                self.config.write(open(self.file, 'w'))
        except Exception as error:
            self.config.write(open(self.file, 'w'))
            logger.error("binlog pos点写入文件错误")
            logger.error(error)
            exit(1)

# 获取ch里面的字段类型，需要根据字段类型处理一些默认值的问题
def get_ch_column_type(db,table,conf):
    column_type_dic={}
    try:
        client = clickhouse_driver.Client(host=cnf['clickhouse_server']['host'],\
                                          port=cnf['clickhouse_server']['port'],\
                                          user=cnf['clickhouse_server']['user'],\
                                          password=cnf['clickhouse_server']['passwd'])
        sql="select name,type from system.columns where database='{0}' and table='{1}'".format(db,table)
        for d in client.execute(sql):
            if colum_lower_upper:
                column_type_dic[d[0].upper()]=d[1]
            else:
                column_type_dic[d[0].lower()]=d[1]
        return column_type_dic
    except Exception as error:
        message="获取clickhouse里面的字段类型错误. %s" % (error)
        logger.error(message)
        exit(1)


# 剔除比较旧的更新，保留最新的更新，否则update的时候数据会多出,因为update已经换成delete+insert。如果不这样处理同一时间update两次就会导致数据多出
def keep_new_update(tmp_data,schema,table,pk_dict):
    db_table="{}.{}".format(schema,table)
    t_dict = {}
    new_update_data = []
    max_time = 0
    same_info =[]
    for items in tmp_data:
        sequence_number = items['sequence_number']
        info = items['values'][pk_dict[db_table][0]]

        if info in same_info:
            same_info.append(info)
            if sequence_number > max_time:
                del t_dict[info]
                t_dict.setdefault(info,[]).append(items)
                max_time=sequence_number
        else:
            same_info.append(info)
            t_dict.setdefault(info, []).append(items)
            max_time = sequence_number

    for k,v in t_dict.items():
            for d in v:
                new_update_data.append(d)

    del t_dict
    del same_info
    gc.collect()
    return new_update_data

# 获取配置文件参数的函数
def get_config(conf):
    try:
        if not os.path.exists(args.conf):
            print "指定的配置文件: %s 不存在" % (conf)
            exit(1)
        config = ConfigParser.ConfigParser()
        config.readfp(open(conf,'r'))
        config_dict={}
        for title in config.sections():
            config_dict[title]={}
            for one in config.options(title):
                config_dict[title][one]=config.get(title,one).strip(' ').strip('\'').strip('\"')
    except Exception as error:
        message="从配置文件获取配置参数错误: %s" % (error)
        print message
        exit(1)
    else:
        return config_dict

# 邮件发送函数
def send_mail(to_list,sub,content):
    me=mail_send_from
    msg = MIMEText(content, _subtype='html', _charset='utf-8')
    msg['Subject'] = Header(sub,'utf-8')
    msg['From'] = Header(me,'utf-8')
    msg['To'] = ";".join(to_list)
    try:
        smtp = smtplib.SMTP()
        smtp.connect(mail_host,mail_port)
        smtp.login(mail_user,mail_pass)
        smtp.sendmail(me,to_list, msg.as_string())
        smtp.close()
        return True
    except Exception as error:
        logging.error("邮件发送失败: %s" % (error))
        return False


# binglog接收函数
def binlog_reading(only_events,conf,debug):
    mysql_conf={}
    clickhouse_conf={}
    event_list=[]
    sequence = 0
    mysql_server_id=int(cnf['master_server']['server_id'])
    mysql_conf['host']=cnf['master_server']['host']
    mysql_conf['port']=int(cnf['master_server']['port'])
    mysql_conf['user']=cnf['master_server']['user']
    mysql_conf['passwd']=cnf['master_server']['passwd']
 
    clickhouse_conf['host']=cnf['clickhouse_server']['host']
    clickhouse_conf['port']=int(cnf['clickhouse_server']['port'])
    clickhouse_conf['passwd']=cnf['clickhouse_server']['passwd']
    clickhouse_conf['user']=cnf['clickhouse_server']['user']
   
    only_schemas=cnf['only_schemas']['schemas'].split(",")
    only_tables=cnf['only_tables']['tables'].split(",")
 
    alarm_mail=cnf['failure_alarm']['alarm_mail'].split(",")
    skip_dmls_all=cnf['skip_dmls_all']['skip_type'].split(",")

    skip_delete_tb_name=cnf['skip_dmls_sing']['skip_delete_tb_name'].split(",")
    skip_update_tb_name=cnf['skip_dmls_sing']['skip_update_tb_name'].split(",")

    insert_nums=int(cnf['bulk_insert_nums']['insert_nums'])
    interval=int(cnf['bulk_insert_nums']['interval'])
    logger.info('开始同步数据时间 %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    if logtoredis:
        redis=my_redis()
        logger.info("同步binlog pos点从Redis读取")
    else:
        logger.info("同步binlog pos点从文件读取")
        redis=mark_log()

    db=my_db()
    log_file, log_pos = redis.get_log_pos()
    if log_file and log_pos:
        log_file == log_file
        log_pos == log_pos
    else:
        logger.error("获取binlog pos点错误,程序退出....")
        exit(1)

    pk_dict={}
    for schema in only_schemas:
        for table in only_tables:
            pk=db.get_pri(schema,table)
            if pk:
                name="{}.{}".format(schema,table)
                pk_dict[name]=pk
            else:
                name="{}.{}".format(schema,table)
                if db.check_table_exists(schema,table):
                    logger.error("要同步的表: %s 不存在自增主键，程序退出...." %(name))
                    exit(1)

    message="读取binlog: {0}:{1}".format(log_file,log_pos)
    ch_info="同步到clickhouse server {0}:{1}".format(cnf['clickhouse_server']['host'],cnf['clickhouse_server']['port'])
    repl_info="{0}:{1}".format(cnf['master_server']['host'],cnf['master_server']['port'])
    alarm_info="{0} 库:{1} 表:{2} 同步数据到clickhouse服务器:{3}失败".format(repl_info,only_schemas,only_tables,socket.gethostname())
    logger.info('从服务器 %s 同步数据' % (repl_info))
    logger.info(message)
    logger.info(ch_info)
    logger.info('同步到clickhouse的数据库: %s' % (only_schemas))
    logger.info('同步到clickhouse的表: %s' % (only_tables))

    stream = BinLogStreamReader(connection_settings=mysql_conf,resume_stream=True,blocking=True,\
                                server_id=mysql_server_id, only_tables=only_tables,only_schemas=only_schemas,\
                                only_events=only_events,log_file=log_file,log_pos=int(log_pos),fail_on_table_metadata_unavailable=True,slave_heartbeat=10)
    try:
        for binlogevent in stream:
            for row in binlogevent.rows:
                sequence += 1
                event = {"schema": binlogevent.schema, "table": binlogevent.table}
                event['sequence_number'] = sequence
                if isinstance(binlogevent, WriteRowsEvent):
                    event["action"] = "insert"
                    event["values"] = row["values"]
                    event['event_unixtime']=int(time.time())
                    event['action_core']='2'
               
                elif isinstance(binlogevent, UpdateRowsEvent):
                    event["action"] = "insert"
                    event["values"] = row["after_values"]
                    event['event_unixtime']=int(time.time())
                    event['action_core']='2'

                elif isinstance(binlogevent, DeleteRowsEvent):
                    event["action"] = "delete"
                    event["values"] = row["values"]
                    event['event_unixtime']=int(time.time())
                    event['action_core']='1'

                event_list.append(event)

                if len(event_list) == insert_nums or ( int(time.time()) - event_list[0]['event_unixtime'] >= interval and interval > 0 ):
                    repl_status=db.slave_status()
                    log_file=stream.log_file
                    log_pos=stream.log_pos                    
                    if repl_status:
                        redis.set_log_pos('master',repl_status['Master_Host'],repl_status['Master_Port'],repl_status['Relay_Master_Log_File'],repl_status['Exec_Master_Log_Pos'])
         
                    data_dict = {}
                    tmp_data = []
                    for items in event_list:
                        table = items['table']
                        schema = items['schema']
                        action=items['action']
                        action_core=items['action_core']
                        data_dict.setdefault(table+schema+action+action_core,[]).append(items)
                    for k,v in data_dict.items():
                        tmp_data.append(v)

                    status=data_to_ck(tmp_data,alarm_info,alarm_mail,debug,skip_dmls_all,skip_delete_tb_name,skip_update_tb_name,pk_dict,only_schemas,**clickhouse_conf)
                    if status:
                        redis.set_log_pos('slave',log_file,log_pos)
                        del event_list
                        event_list=[]
                        sequence = 0
                        gc.collect()
                    else:
                        log_file,log_pos = redis.get_log_pos()
                        message="SQL执行错误,当前binlog位置 {0}:{1}".format(log_file,log_pos)
                        logger.error(message)
                        exit(1)

    except KeyboardInterrupt:
        log_file,log_pos = redis.get_log_pos()
        message="同步程序退出,当前同步位置 {0}:{1}".format(log_file,log_pos)
        logger.info(message)
    finally:
        stream.close()

def insert_update(tmp_data,pk_dict):
    insert_data=[]
    exec_sql={}
    table=tmp_data[0]['table']
    schema=tmp_data[0]['schema']
    column_type=get_ch_column_type(schema,table,args.conf)

    for data in tmp_data:
        for key,value in data['values'].items():
            if value == datetime.datetime(1970, 1, 1, 0, 0):
                data['values'][key]=datetime.datetime(1970,1,2,14,1)

            # 处理mysql里面是Null的问题
            if value is None:
                int_list=['Int8','Int16','Int32','Int64','UInt8','UInt16','UInt32','UInt64']
                if column_type[key] == 'DateTime':
                    data['values'][key]=datetime.datetime(1970,1,2,14,1)
                elif column_type[key] == 'Date':
                    data['values'][key]=datetime.date(1970,1,2)
                elif column_type[key] == 'String':
                    data['values'][key]=''
                elif column_type[key] in int_list:
                    data['values'][key]=0
        
            # decimal 字段类型处理，后期ch兼容mysql协议可以删除
            if type(value) == decimal.Decimal:
                data['values'][key]=str(value)
            
        insert_data.append(data['values'])

    del_sql=event_primary_key(schema,table,tmp_data,pk_dict)
    insert_sql="INSERT INTO {0}.{1} VALUES,".format(schema,table)
    exec_sql['del_sql'] = del_sql
    exec_sql['insert_sql'] = insert_sql
    exec_sql['insert_data'] = insert_data
    exec_sql['db_tb']="{0}.{1}".format(schema,table)
    query_sql=del_sql
    query_sql=query_sql.replace('alter table','select count(*) from')
    pattern = re.compile(r'\sdelete\s')
    query_sql = re.sub(pattern, ' ', query_sql)
    exec_sql['query_sql'] = query_sql
    del insert_data
    gc.collect()
    return exec_sql

# 排序记录,处理insert里面数据多的情况
def action_reverse(event_table):
    del_ins=[]
    data_dict={}
    for items in event_table:
        table = items.split('.')[1]
        schema = items.split('.')[0]
        data_dict.setdefault(table+schema,[]).append(items)

    for k,v in data_dict.items():
        if len(v) == 2:
            if v[0].split(".")[2] == 'insert':
                v.reverse()
                del_ins.append(v)
            else:
                del_ins.append(v)
        else:
            del_ins.append(v)

    del data_dict
    gc.collect()
    return del_ins


# 删除insert中多余的记录
def del_insert_record(table_action,tmp_data_dic,pk_dict):
    if len(table_action) == 2:
        delete=tmp_data_dic[table_action[0]]
        insert=tmp_data_dic[table_action[1]]
        tb_name=table_action[0].split(".")
        name="{}.{}".format(tb_name[0],tb_name[1])
        pk=pk_dict[name]
        delete_list = []
        for i in delete:
            delete_list.append((i['values'][pk[0]],i['sequence_number']))

        insert2 = []
        for i in insert:
            pk_id = i['values'][pk[0]]
            sequence_number = i['sequence_number']
            insert2.append(i)
            for x,y in delete_list:
                if pk_id == x and sequence_number < y:
                    insert2.remove(i)
        tmp_data_dic[table_action[1]]=insert2
        del delete_list
        del insert2
        gc.collect()


# 根据主键以及没有主键的删除数据处理函数
def event_primary_key(schema,table,tmp_data,pk_dict):
    del_list=[]
    last_del = {}
    db_table="{}.{}".format(schema,table)
    primary_key=pk_dict[db_table][0]
    for data in tmp_data:
        for k,v in data['values'].items():
            data_dict={}
            if type(v) == datetime.datetime:
                data_dict[k]=str(v)
            elif type(v) == datetime.date:
                data_dict[k]=str(v)
            elif type(v) == decimal.Decimal:
                data_dict[k]=str(v)
            elif type(v) == unicode:
                data_dict[k]=v.encode('utf-8')
            else:
                data_dict[k]=(v)

            del_list.append(data_dict)

    for i in del_list:
        for k,v in i.items():
            last_del.setdefault(k,[]).append(v)    

    if primary_key:
        for k,v in last_del.items():
            if k not in primary_key:
                del last_del[k]
    else:
        message="delete {0}.{1} 但是mysql里面没有定义主键...".format(schema,table)
        logger.warning(message)

    for k,v in last_del.items():
        last_del[k]=tuple(v)
        nk=k + " in"
        last_del[nk] = last_del.pop(k)
        value_num=len(v)

    replace_max=len(last_del) -1
    tmp_sql=''
    for k,v in last_del.items():
        c=str(k) + ' ' + str(v) + " "
        tmp_sql+=c
    tmp_sql=tmp_sql.replace(')',') and',replace_max)

    if value_num == 1:
        del_sql="alter table {0}.{1} delete where {2}".format(schema,table,tmp_sql)
        del_sql=del_sql.replace(',','')
        del_sql=del_sql.replace('L','')
    else:
        del_sql="alter table {0}.{1} delete where {2}".format(schema,table,tmp_sql)
        del_sql=del_sql.replace('L','')

    del del_list
    del last_del
    gc.collect()
    return del_sql


# 把解析以后的binlog内容拼接成sql入库到ch里面
def data_to_ck(event,alarm_info,alarm_mail,debug,skip_dmls_all,skip_delete_tb_name,skip_update_tb_name,pk_dict,only_schemas,**kwargs):
    client = clickhouse_driver.Client(host=kwargs['host'], port=kwargs['port'],user=kwargs['user'], password=kwargs['passwd'])
    #检查mutations是否有失败的(ch后台异步的update和delete变更)
    mutation_list=['mutation_faild','table','create_time']
    fail_list=[]
    mutation_data=[]
    if len(only_schemas) == 1:
        query_sql="select count(*) from system.mutations where is_done=0 and database in %s" % (str(tuple(only_schemas)))
        query_sql=query_sql.replace(",",'')  
    else:
        query_sql="select count(*) from system.mutations where is_done=0 and database in %s" % (str(tuple(only_schemas)))
    mutation_sql="select count(*) as mutation_faild ,concat(database,'.',table)as db,create_time from system.mutations where is_done=0 and database in %s group by db,create_time" % (str(tuple(only_schemas)))
    mutations_faild_num=client.execute(query_sql)[0][0]
    if mutations_faild_num >= 10:
        fail_data=client.execute(mutation_sql)
        for d in fail_data:
            fail_list.append(list(d))
        for d in fail_list:
            tmp=dict(zip(mutation_list,d))
            mutation_data.append(tmp)
        last_data=json.dumps(mutation_data,indent=4,cls=DateEncoder)
        message="mutations error faild num {0}. delete有失败.请进行检查. 详细信息: {1}".format(mutations_faild_num,last_data)
        logger.error(message)
        send_mail(alarm_mail,alarm_info,message)

    # 字段大小写问题的处理
    for data in event:
        for items in data:
            for key,value in items['values'].items():
                if colum_lower_upper:
                    items['values'][key.upper()] = items['values'].pop(key)
                else:
                    items['values'][key.lower()] = items['values'].pop(key)
    
    # 处理同一条记录update多次的情况
    new_data=[]
    for tmp_data in event:
        table=tmp_data[0]['table']
        schema=tmp_data[0]['schema']
       
        if tmp_data[0]['action'] == 'insert':
            new_data.append(keep_new_update(tmp_data,schema,table,pk_dict))
        else:
            new_data.append(tmp_data)
    
    tmp_data_dic={}
    event_table=[]
    for data in new_data:
        name='{}.{}.{}'.format(data[0]['schema'],data[0]['table'],data[0]['action'])
        tmp_data_dic[name]=data
        event_table.append(name)

    event_table=list(set(event_table))
    del_ins=action_reverse(event_table)

    # 删除多余的insert，并且最后生成需要的格式[[],[]]
    for table_action in del_ins:
        del_insert_record(table_action,tmp_data_dic,pk_dict)

    # 生成最后处理好的数据
    last_data=[]
    for k,v in tmp_data_dic.items():
        if len(v) != 0:
            last_data.append(v)

    # 排序，执行顺序，delete，insert
    tmp_dict = {}
    i = 0
    for d in last_data:
        tmp_dict[str(str(i))] = d[0]['action_core']
        i = i + 1
    sort_list = sorted(tmp_dict.items(),key=lambda x:x[1])
    new_event = []
    for i in sort_list:
        index = int(i[0])
        new_event.append(last_data[index])
    
    
    # 正式把处理完成的数据插入clickhouse
    for tmp_data in new_event:
        if tmp_data[0]['action'] == 'delete':
            table=tmp_data[0]['table']
            schema=tmp_data[0]['schema']
            skip_dml_table_name="{0}.{1}".format(schema,table)
        
            del_sql=event_primary_key(schema,table,tmp_data,pk_dict)
            if debug:
                message="DELETE 数据删除SQL: %s" % (del_sql)
                logger.info(message)
            try:
                if 'delete' in skip_dmls_all:
                    return True
                elif skip_dml_table_name in skip_delete_tb_name:
                    return True
                else:
                    client.execute(del_sql)
        
            except Exception as error:
                message="执行出错SQL:  " + del_sql
                mail_contex="{0} {1}".format(message,error)
                send_mail(alarm_mail,alarm_info,mail_contex)
                logger.error(message)
                logger.error(error)
                return False
    
        elif tmp_data[0]['action'] == 'insert':
            sql=insert_update(tmp_data,pk_dict)
            try:
                if client.execute(sql['query_sql'])[0][0] >= 1:
                    client.execute(sql['del_sql'])
            except Exception as error:
                message="在插入数据之前删除数据,执行出错SQL:  " + sql['del_sql']
                mail_contex="{0} {1}".format(message,error)
                send_mail(alarm_mail,alarm_info,mail_contex)
                logger.error(message)
                logger.error(error)
                return False

            if debug:
                message="INSERT 数据插入SQL: %s %s " % (sql['insert_sql'],str(sql['insert_data']))
                logger.info(message)
            try:
                client.execute(sql['insert_sql'],sql['insert_data'],types_check=True)
            except Exception as error:
                message="插入数据执行出错SQL:  " + sql['insert_sql'] + str(sql['insert_data'])
                mail_contex="{0} {1}".format(message,error)
                send_mail(alarm_mail,alarm_info,mail_contex)
                logger.error(message)
                logger.error(error)
                return False
    del new_event
    del tmp_dict
    del last_data
    del tmp_data_dic
    del new_data
    gc.collect()
    return True

def init_parse():
    parser = argparse.ArgumentParser(
        prog='Data Replication to clikhouse',
        description='mysql data is copied to clikhouse',
        epilog='By dengyayun @2019',
    )
    parser.add_argument('-c','--conf',required=False,default='./metainfo.conf',help='Data synchronization information file')
    parser.add_argument('-d','--debug',action='store_true',default=False,help='Display SQL information')
    parser.add_argument('-l','--logtoredis',action='store_true',default=False,help='log position to redis ,default file')
    return parser


if __name__ == '__main__':
    parser = init_parse()
    args = parser.parse_args()
    config = args.conf
    logtoredis = args.logtoredis
    global cnf
    cnf=get_config(config)
    global colum_lower_upper
    global mail_host
    global mail_port
    global mail_user
    global mail_pass
    global mail_send_from
    colum_lower_upper=int(cnf['clickhouse_server']['column_lower_upper'])
    mail_host=cnf['failure_alarm']['mail_host']
    mail_port=int(cnf['failure_alarm']['mail_port'])
    mail_user=cnf['failure_alarm']['mail_user']
    mail_pass=cnf['failure_alarm']['mail_pass']
    mail_send_from=cnf['failure_alarm']['mail_send_from']

    logger = logging.getLogger("mylogger")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(cnf['repl_log']['log_dir'])
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
    fmt="%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s",
    datefmt="%a %d %b %Y %H:%M:%S"
    ))

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter(
    fmt="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S"
    ))

    logger.addHandler(fh)
    logger.addHandler(sh)
    
    only_events=(DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent)
    binlog_reading(only_events,conf=args.conf,debug=args.debug)
    
