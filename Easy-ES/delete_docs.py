import json
import os
from typing import List
import yaml
import argparse
from elasticsearch import Elasticsearch 
from datetime import datetime

import elastic_utils

class DeleteDocs:
    config_path = f'{os.path.dirname(__file__)}/configs/delete_docs.yaml'
    clear = False
    total_num = 0
    
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
        self.es:Elasticsearch = elastic_utils.create_es()

    def load_config(self):
        with open(self.config_path, 'r') as cp:
            config = yaml.safe_load(cp)
            self.ip = config.get('ip')
            self.port = config.get('port')
            self.username = config.get('username') if config.get('username') else self.username
            self.password = config.get('password') if config.get('password') else self.password
            # self.clear = config.get('clear') if config.get('clear') else self.clear
            self.index_name = config.get('index_name')
            self.query = config.get('query') 

    def process(self):
        if not elastic_utils.index_exists(self.es, self.index_name):
            print(f'The index {self.index_name} do not exist!!')
            return
        if self.query == 'all':
            query = {
                "query":{
                    "match_all":{}
                }
            }
        elif isinstance(self.query, dict):
            query = {
                "query":{
                    "bool":{
                        "must":[]
                    }
                }
            }
            for key,value in self.query.items():
                if key == 'match_all':
                    query['query']['bool']['must'].append(self.query)
                    break
                condition = {
                    "match":{
                        key: value
                    }
                }
                query['query']['bool']['must'].append(condition)
        try:
            response = self.es.delete_by_query(index=self.index_name, body=query)
            # es的增删改操作是异步的，1s刷新一次到硬盘，这里使用refresh强制刷新到硬盘
            self.es.indices.refresh(index=self.index_name)
            delete_num = response['deleted']
            if delete_num:
                print(f'Delete is done, {delete_num} docs are deleted!!')
            elif delete_num == 0:
                print(f'Nothing hasppened, can not find the docs that equal to the query:{self.query}')
            self.delete_num = delete_num
            if delete_num:
                self.insert_metadata()
                elastic_utils.update_metadata(self.es, self.index_name, 'create')

        except Exception as ex:
            print(f'An error occured when delete:: {ex}')
            raise ex
        
    def insert_metadata(self):
        insert_body = {
            'index_name':self.index_name,
            'operation':'delete',
            # 'query':self.query,
            'operation_time': datetime.now().isoformat().split(".")[0],
            'delete_num': self.delete_num,
            'current_num': elastic_utils.get_doc_num(self.es, self.index_name)
        }
        if self.query == 'all':
            insert_body['query'] = {'match_all':True}
        elif isinstance(self.query, dict):
            insert_body['query'] = self.query
        self.es.index(index='metadata_index', body=insert_body)
        print(f'The operation record is saved into metadata_index!!')

if __name__ == '__main__':
    deleteDocs = DeleteDocs()
    deleteDocs.process()
