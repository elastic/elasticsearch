import json
import os
from typing import List
from elasticsearch import Elasticsearch 
from datetime import datetime
import yaml

security_config_path = f'{os.path.dirname(__file__)}/configs/security.yml'

def create_es(  ip:str|None = None,
                port:int|None = None,
                username:str|None = None,
                password:str|None = None,
                use_ssl:bool|None = None):

    if not (ip and port and username and password):
        fp = open(security_config_path, 'r')
        config = yaml.safe_load(fp)
        fp.close()
        print(f'传入es验证信息不完整，使用默认配置: {security_config_path}')
        ip = config.get('ip') if not ip else ip
        port = str(config.get('port')) if not port else port
        username = str(config.get('username')) if not username else username
        password = config.get('password') if password is None else password
        use_ssl = config.get('use_ssl') if config.get('use_ssl') is not None else False
    if not use_ssl:
        es = Elasticsearch([f'http://{ip}:{port}'], basic_auth=(username, password), timeout=30, max_retries=10, retry_on_timeout=True)
    else:
        es = Elasticsearch([f'https://{ip}:{port}'], basic_auth=(username, password), timeout=30, max_retries=10, 
                           retry_on_timeout=True, verify_certs=False, ssl_show_warn=False)
    return es

# def create_es(ip, port, username, password):
#     es = Elasticsearch([f'http://{ip}:{port}'], basic_auth=(username, password), timeout=30, max_retries=10, retry_on_timeout=True)
#     return es

# 根据index_name和对应的operation找到在元数据索引中的操作记录
def query_metadata(es:Elasticsearch, index_name, operation):
    query = {
        "query":{
            "bool":{
                "must":[{
                    "match_phrase":{
                        "index_name": index_name}},
                        {
                            "match_phrase":{
                                "operation":operation
                            }
                        }
                ]
            }
        }
    }
    response = es.search(index='metadata_index', body=query)
    hits = response.get('hits')
    if hits:
        hits_list = hits.get('hits')
        if hits_list:
            return hits_list[-1]
    else:
        print("No documents found.")
        return None
    
# 根据index_name和对应的operation找到在元数据索引中的操作记录
def get_doc_num(es:Elasticsearch, index_name):
    response = es.count(index=index_name)
    return response['count']

def update_metadata(es:Elasticsearch, index_name , operation, **para_dict):
    # 更新元数据索引分，更新create操作的元数据 
    doc_num = para_dict.get('doc_num')
    if doc_num is not None:
        doc_num = get_doc_num(es, index_name) if doc_num < 0 else doc_num
    else:
        doc_num = get_doc_num(es, index_name)

    last_id = para_dict.get('last_id')

    meta_doc_id = para_dict.get('meta_doc_id')

    if not meta_doc_id:
        doc = query_metadata(es, index_name, operation)
        if not doc:
            print(f'No such data: index_name={index_name}, operation={operation}!!!')
            return
        meta_doc_id = doc['_id']

    update_doc = {
        'doc':{
            #'last_id':last_id,
            'last_update_time': datetime.now().isoformat().split('.')[0],
            'doc_num':doc_num
        }
    }
    if last_id is not None:
        update_doc['doc']['last_id'] = last_id             
    response = es.update(index='metadata_index', id=meta_doc_id, body=update_doc)
    if response['result'] == 'updated':
        print(f"metadata_index record about '{index_name}' updated successfully, the change is: {update_doc}")

    else:
        print("Document was not updated")

def get_begin_id(es:Elasticsearch, index_name, operation):
    # 获取begin_id的逻辑：先从metadata里获取，如果找不到，则对数据按_id进行排序
    begin_id = -1
    meta_doc_id = ''
    meta_doc = query_metadata(es, index_name, operation)
    if meta_doc:
        print('The doc is found:')
        print(f"Document ID: {meta_doc['_id']}, Source: {meta_doc['_source']}")
        begin_id = meta_doc['_source']['last_id']
        meta_doc_id = meta_doc['_id']
    else:
        body = {
            "aggs": {
                "max_id": {
                    "max": {
                        "script": "doc['_id'].value.length() == 0 ? 0 : Long.parseLong(doc['_id'].value, 10)"
                    }
                }
            }
        }
        try:
            response = es.search(index=index_name, body=body)
            begin_id = int(response['aggregations']['max_id']['value'])
        except Exception as ex:
            print(ex)
            print('The begin_id can not get, default is -1')
    return begin_id + 1, meta_doc_id

def index_exists(es:Elasticsearch, index_name):
    index_name = index_name
    is_exist = es.indices.exists(index=index_name)
    if not is_exist:
        print(f'index {index_name} 不存在！！')
        return False
    else:
        return True

if __name__ == '__main__':
    pass