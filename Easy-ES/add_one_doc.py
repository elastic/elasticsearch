import json
import os
from typing import List
import yaml
import argparse
from elasticsearch import Elasticsearch 
from datetime import datetime

import elastic_utils

class AddOneDoc:
    config_path = f'{os.path.dirname(__file__)}/configs/add_one_doc.yaml'
    clear = False
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
        self.es:Elasticsearch = elastic_utils.create_es()

    def load_config(self):
        with open(self.config_path, 'r') as cp:
            config = yaml.safe_load(cp)
            self.ip = config.get('ip')
            self.port = config.get('port')
            self.username = config.get('username') if config.get('username') else self.username
            self.password = config.get('password') if config.get('password') else self.password
            self.index_name = config.get('index_name')
            self.doc = config.get('doc')
            self.doc_id = str(config.get('id')) if config.get('id') is not None else self.doc_id

    def process(self):
        if not elastic_utils.index_exists(self.es, self.index_name):
            print(f'The index {self.index_name} do not exist!!')
            return
        for key,value in self.doc.items():
            if value == '$time-now$':
                self.doc[key] = datetime.now().isoformat().split(".")[0]
        try:
            if self.doc_id == 'continue':
                (current_id, meta_doc_id) = elastic_utils.get_begin_id(self.es, self.index_name, 'create')
                self.current_id = current_id
                self.meta_doc_id = meta_doc_id 
                response = self.es.index(index=self.index_name, body=self.doc, id=self.current_id)               
            elif self.doc_id == 'random':
                response = self.es.index(index=self.index_name, body=self.doc)
            else:
                response = self.es.index(index=self.index_name, body=self.doc, id=self.doc_id)
            # es的增删改操作是异步的，1s刷新一次到硬盘，这里使用refresh强制刷新到硬盘            
            result = response['result']
            self.doc_id = response['_id']
            if result == 'created':
                print(self.doc)
                print('Insert-doc success !!')
            else:
                print(self.doc)
                print('Insert-doc failed !!')
            self.es.indices.refresh(index=self.index_name)
            self.insert_metadata()
            elastic_utils.update_metadata(self.es, self.index_name, 'create', last_id = self.current_id)

        except Exception as ex:
            print(f'An error occured when insert the doc:: {ex}')
            raise ex
        
    def insert_metadata(self):
        insert_body = {
            'index_name':self.index_name,
            'operation':'insert',
            'operation_time': datetime.now().isoformat().split(".")[0],
            'doc_id': self.doc_id,
            'current_num': elastic_utils.get_doc_num(self.es, self.index_name)
        }
        self.es.index(index='metadata_index', body=insert_body)
        print(f'The operation record is saved into metadata_index!!')

if __name__ == '__main__':
    addOneDoc = AddOneDoc()
    addOneDoc.process()
