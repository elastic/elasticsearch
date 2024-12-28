import json
import os
from typing import List
import yaml
import argparse
from elasticsearch import Elasticsearch 
from datetime import datetime

import elastic_utils

class UpdateDocs:
    config_path = f'{os.path.dirname(__file__)}/configs/update_docs.yaml'
    total_num = 0
    username = 'elastic'
    password = 'elastic@zhijiang'

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
            self.index_name = config.get('index_name')
            self.doc_id = str(config.get('id')) if config.get('id') is not None else None
            self.query = config.get('query')
            self.doc = {'doc':config.get('doc')} if config.get('doc') else None

    def process(self):
        if not elastic_utils.index_exists(self.es, self.index_name):
            print(f'The index {self.index_name} do not exist!!')
            return
        if not self.doc:
            print(f'There is no  "doc" field in config: {self.config_path} !!')
            return

        for key,value in self.doc['doc'].items():
            if value == '$time-now$':
                self.doc['doc'][key] = datetime.now().isoformat().split(".")[0]
        try:
            if self.doc_id is not None:
                response = self.es.update(index=self.index_name, body=self.doc, id=self.doc_id)
                if response['result'] == 'updated':
                    self.update_num = 1
                    print(f'The doc in {self.index_name} has been updated seccessfully, the id is {self.doc_id}')
            else:
                script = {
                    "script":{
                        "lang": "painless"
                    }
                }
                script_source = ''
                for key,value in self.doc['doc'].items():
                    # ctx._source.title = 'title added'
                    if isinstance(value, str):
                        value = f"'{value}'"
                    script_text = f'ctx._source.{key} = {value};'
                    script_source += script_text
                script['script']['source'] = script_source
                
                # 全量更新
                if isinstance(self.query, str) and self.query == "all":
                    query = {
                        "query":{
                            "match_all":{}
                        }
                    }
                else:
                    query = {
                        "query":{
                            "bool":{
                                "must":[]
                            }
                        }
                    }
                    for key,value in self.query.items():
                        condition = {
                            "match":{
                                key: value
                            }
                        }
                        query['query']['bool']['must'].append(condition)
                
                body = {**script, **query}
                response = self.es.update_by_query(index=self.index_name, body=body)                    
                if response['updated']:
                    self.update_num = response['updated']
                    print(f'{self.update_num} docs in {self.index_name} has been updated seccessfully')

            self.es.indices.refresh(index=self.index_name)
            self.insert_metadata()

        except Exception as ex:
            print(f'An error occured when insert the doc:: {ex}')
            raise ex
        
    def insert_metadata(self):
        insert_body = {
            'index_name':self.index_name,
            'operation':'update',
            'operation_time': datetime.now().isoformat().split(".")[0],
            'update_num': self.update_num
        }
        if self.doc_id is not None:
            insert_body['doc_id'] = self.doc_id
        if self.query == 'all':
            insert_body['query'] = {'match_all':True}
        self.es.index(index='metadata_index', body=insert_body)
        print(f'The operation record is saved into metadata_index!!')

if __name__ == '__main__':
    updateDocs = UpdateDocs()
    updateDocs.process()
