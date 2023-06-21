# -*- coding: utf-8 -*-
"""
首次编辑 2019-02-21
将redis-pro中的数据导入到SQLite数据库中，便于拷贝到笔记本上使用
@author: Administrator
"""

import redis
import pandas as pd
import sys
import pprint
#from wttools.sqlite2kv import ConnectionPool, StrictRedis
from sqlite2kv import ConnectionPool, StrictRedis
import configparser

def trans_string(name, r, lr):
    data = r.get(name)
    lr.set(name, data)

def trans_hash(name, r, lr):
    keys = r.hkeys(name)
    for key in keys:
        data = r.hget(name, key)
        lr.hset(name, key, data)
        #sys.stdout.flush()
        sys.stdout.write("%s:%d/%d          \r"%(name, keys.index(key), len(keys)))


def trans():
    conf = configparser.ConfigParser()
    conf.read('../config.ini',encoding="utf-8-sig")
    host = conf.get('redis', 'host')
    port = conf.get('redis', 'port')
    try:
        password = conf.get('redis', 'pass')
    except:
        password = ""

    r = redis.StrictRedis(host=host, port=port, password=password)
    rpool = ConnectionPool('/workdir/default.db')
    lr = StrictRedis(connection_pool=rpool)

    rkeys = list(r.keys())
    result = {}
    for k in rkeys:
        result[k] = r.type(k)

    print('redis中有如下数据：')
    pprint.pprint(pd.Series(result))

    print('开始转移数据：')
    rkeys = list(r.keys())
    for k in rkeys:
        if r.type(k)==b'string':
            trans_string(k, r, lr)
        elif r.type(k)==b'hash':
            trans_hash(k, r, lr)
        else:
            pass
        print('%s 完成               '%(k))
    print('转换完成。')
    lr.connection_pool.disconnect()
    r.connection_pool.disconnect()
    return


if __name__ == '__main__':

    trans()