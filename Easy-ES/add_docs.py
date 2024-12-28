import json
import os
from typing import List
import yaml
import argparse
from elasticsearch import Elasticsearch 
from datetime import datetime
import time

import elastic_utils

class InsertDocs:
    config_path = f'{os.path.dirname(__file__)}/configs/add_docs.yaml'
    suffix_list = ['json', 'jsonl']
    file_list:List[str] = []
    meta_doc_id = ''
    lang = 'not_known'
    total_num = 0
    doc_id = 'continue'

    def __init__(self) :
        # 创建 ArgumentParser 对象
        parser = argparse.ArgumentParser(description='这是一个命令行参数解析器')
        # 添加一个可选的命令行参数'--config'
        parser.add_argument('--config', type=str, help='配置文件路径')

        # 解析命令行参数
        args = parser.parse_args()
        if args.config:
            self.config_path = args.config
            print(f"配置文件路径：{args.config}")
        else:
            print(f"使用默认配置文件：{self.config_path}")
        self.load_config()
        self.es = elastic_utils.create_es()

    def load_config(self):
        with open(self.config_path, 'r') as cp:
            config = yaml.safe_load(cp)
        # 先建立日志
            self.ip = config.get('ip')
            self.port = config.get('port')
            self.username = config.get('username') if config.get('username') else self.username
            self.password = config.get('password') if config.get('password') else self.password
            self.auth_pair = (self.username, self.password)
            self.index_name = config.get('index_name')
            self.source_dir = config.get('source_dir')
            self.suffix_list = config.get('suffix_list') if config.get('suffix_list') else self.suffix_list
            self.add_files(self.source_dir, self.file_list, self.suffix_list)
            self.metadata = config.get('metadata') if config.get('metadata') else {}
            self.doc_id = config.get('id') if config.get('id') else self.doc_id


    def process(self):
        index_name = self.index_name
        is_exist = self.es.indices.exists(index=index_name)
        if not is_exist:
            print(f'The index {self.index_name} do not exist!!')
            return
        if self.doc_id == 'continue':
            (current_id, meta_doc_id) = elastic_utils.get_begin_id(self.es, self.index_name, 'create')
            self.curretnt_id = current_id
            self.meta_doc_id = meta_doc_id
        for file in self.file_list:
            doc_list = []
            json_error = False
            if file.endswith('.json'):
                with open(file, 'r') as fp:
                    try:
                        content = fp.read()
                        doc_list.append(json.loads(content))
                    except json.JSONDecodeError as e:
                        print("解析错误:", e)
                        json_error = True   # 或者设置一个默认值
            if file.endswith('.jsonl') or json_error:
                fp = open(file, 'r')
                for line in fp:
                    doc = json.loads(line)
                    doc_list.append(doc)                
                fp.close()
            self.insert_docs(doc_list, self.total_num)
        elastic_utils.update_metadata(self.es, self.index_name, 'create', last_id=self.curretnt_id-1, meta_doc_id=self.meta_doc_id)
        self.insert_metadata()

    def insert_docs(self, doc_list:List[dict], begin_number):
        operation_list = []
        for doc in doc_list:
            for key,value in self.metadata.items():
                if not doc.get(key):
                    doc[key] = value
            operation = {
                "index": {
                    "_index": self.index_name,
                    # "_id": self.curretnt_id
                }
            }
            if self.doc_id == 'continue':
                operation['index']['_id'] = self.curretnt_id
            operation_list.append(operation)
            operation_list.append(doc)
            if len(operation_list) == 200:
                # print(f'operation_list length is {len(operation_list)}, total_num is {self.total_num}')
                if (self.total_num - begin_number) and (self.total_num - begin_number) % 2000 == 0:
                    print(f'Now total num is {self.total_num}, sleep 1s')
                    time.sleep(1)
                self.send_to_es(operation_list)
                self.total_num += int(len(operation_list)/2)
                operation_list = []
            self.curretnt_id += 1
        if len(operation_list):
            self.send_to_es(operation_list)
            self.total_num += int(len(operation_list)/2)
            operation_list = []
        print(f'doc_list length is {len(doc_list)}, now sleep 2 s')
        time.sleep(2)
        
    def send_to_es(self, operation_list):
        try:
            response = self.es.bulk(index=self.index_name, body=operation_list, refresh=False)
            if response['errors']:
                print('Bulk operation errors')
                print(response)                
                # raise Exception('Bulk operation errors')
            else:
                print("Bulk operation completed successfully")
        except Exception as ex:
            print(f'批量插入操作错误::{ex}')
           # raise Exception('Bulk operation errors')


    def add_files(self, source_dir:str, file_list:list, suffix_list:list):
        if os.path.isfile(source_dir):
            if source_dir.split('.')[-1] in suffix_list:
                file_list.append(source_dir)
                return
            else:
                return
        files = os.listdir(source_dir)
        for file in files:
            full_path = os.path.join(source_dir, file)
            if os.path.isdir(full_path):
                self.add_files(full_path, file_list, suffix_list)
            elif os.path.isfile(full_path) and file.split('.')[-1] in suffix_list:
                file_list.append(full_path)
    
    # def get_begin_id(self)->int:
    #     begin_id = -1
    #     meta_doc = query_metadata(self.es, self.index_name, 'create')
    #     if meta_doc:
    #         print(f"Document ID: {meta_doc['_id']}, Source: {meta_doc['_source']}")
    #         begin_id = meta_doc['_source']['last_id']
    #         meta_doc_id = meta_doc['_id']
    #     return begin_id + 1, meta_doc_id
        # return 1

    # def query_metadata(self, index_name):
    #     query = {
    #         "query":{
	# 	        "match_phrase":{
    #                 "index_name": index_name}
    #         }
    #     }
    #     response = self.es.search(index='metadata_index', body=query)
    #     hits = response.get('hits')
    #     if hits:
    #         return response['hits']['hits'][-1]
    #     else:
    #         print("No documents found.")
    #         return None
    
    # def update_metadata(self, **para_dict):
    #     # 更新元数据索引分，更新create操作的元数据 
    #     doc_num = para_dict.get('doc_num')
    #     if doc_num is not None:
    #         doc_num = get_doc_num(self.es, self.index_name) if doc_num < 0 else doc_num
    #     else:
    #         doc_num = get_doc_num(self.es, self.index_name)

    #     last_id = para_dict.get('last_id')
    #     if last_id is None:
    #         last_id = -1

    #     if not self.meta_doc_id:
    #         doc = utils.query_metadata(self.es, self.index_name, 'create')
    #         if not doc:
    #             return
    #         self.meta_doc_id = doc['_id']

    #     update_doc = {
    #         'doc':{
    #             'last_id':last_id,
    #             'last_update_time': datetime.now().isoformat().split('.')[0],
    #             'doc_num':doc_num
    #         }
    #     }            
    #     response = self.es.update(index='metadata_index', id=self.meta_doc_id, body=update_doc)
    #     if response['result'] == 'updated':
    #         print(f"metadata_index record about '{self.index_name}' updated successfully, the last_id after update : {last_id}")
    #     else:
    #         print("Document was not updated")
    
    def insert_metadata(self):
        insert_body = {
            'index_name':self.index_name,
            'operation':'bulk insert',
            'operation_time': datetime.now().isoformat().split(".")[0],
            'add_num': self.total_num,
            'source_dir': self.source_dir
        }
        self.es.index(index='metadata_index', body=insert_body)

    def clear_index(self):
        body = {
            "query": {
                "match_all": {}
            }
        }
        self.es.delete_by_query(index=self.index_name, body=body)
        elastic_utils.update_metadata(self.es, self.index_name, 'create', last_id=-1, doc_num=0)

    def add_metadata(self):
        pass


if __name__ == '__main__':
    insertDocs = InsertDocs()
    # id = insertDocs.get_begin_id()
    # print(id)
    insertDocs.process()
    # insertDocs.clear_index()