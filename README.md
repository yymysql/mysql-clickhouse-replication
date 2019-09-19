# mysql-clickhouse-replication

用于从mysql增量同步数据到clickhouse



环境要求：

python 2.7，同时可以利用pypy提高性能。



pypy部署命令（如果使用原生的python无需执行下面命令）：

```bash
yum -y install pypy-libs pypy pypy-devel
wget https://bootstrap.pypa.io/get-pip.py
pypy get-pip.py
/usr/lib64/pypy-5.0.1/bin/pip install MySQL-python
/usr/lib64/pypy-5.0.1/bin/pip install mysql-replication
/usr/lib64/pypy-5.0.1/bin/pip install clickhouse-driver
/usr/lib64/pypy-5.0.1/bin/pip install redis
```

