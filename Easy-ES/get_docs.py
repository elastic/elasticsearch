import json
import os
from typing import List
import yaml
import argparse
from elasticsearch import Elasticsearch 
from datetime import datetime
import elastic_utils

class GetDocs:
    config_path = f'{os.path.dirname(__file__)}/configs/get_docs.yaml'
    query = {}
    max_num = 'unlimited'
    data_append = True
    page_num = 0
    page_size = 10
    timeout = '10m'
    total_num = 0
    batch_num = 0


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
        if self.target_path:
            self.current_path = self.target_path
        elif self.target_dir:
            self.current_part_num = -1
            # self.current_path = f'{self.target_dir}/part{self.current_part_num}.jsonl'
        else:
            raise Exception('Please specify the output path use "target_path" or "target_dir"')

    def load_config(self):
        with open(self.config_path, 'r') as cp:
            config = yaml.safe_load(cp)
            self.ip = config.get('ip')
            self.port = config.get('port')
            self.username = config.get('username') if config.get('username') else self.username
            self.password = config.get('password') if config.get('password') else self.password
            self.auth_pair = (self.username, self.password)
            self.url = f'http://{self.ip}:{self.port}'
            self.index_name = config.get('index_name')

            query = config.get('query')
            if query == 'all':
                self.query = {'match_all':{}}
            elif isinstance(query, dict): 
                self.query = query
                
            self.target_path = config.get('target_path')
            self.target_dir  = config.get('target_dir')
            self.data_append = config.get('data_append') if config.get('data_append') else self.data_append
            self.max_num = config.get('max_num') if config.get('max_num') else self.max_num
    

    def index_exists(self):
        index_name = self.index_name
        is_exist = self.es.indices.exists(index=index_name)
        if not is_exist:
            print(f'The index {self.index_name} do not exist!!')
            return False
        else:
            return True

    def process(self):
        if not self.index_exists():
            return
        # if self.current_path:
        #     self.open_file()
        query = self.query
        result_list = []
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
        query['size'] = self.page_size
        
        try:
            response = self.es.search(index=self.index_name, body=query, scroll=self.timeout)
            batch = response['hits']['hits']
            if batch:
                for item in batch:
                    if not self.is_reach_limit():
                        doc = item['_source']
                        result_list.append(doc)
                        self.total_num += 1
                        self.batch_num += 1
                    else:
                        if self.target_path:
                            self.open_file()
                            self.flush_to_disk(result_list)
                            result_list = [] 
                            print(f'Reach the limit, records have been saved in the file {self.target_path}')
                            return
                        elif self.target_dir:
                            # 如果是往一个目录下写入，且规定了一个文件最大的条数，则创建一个新文件写入后续数据
                            self.current_part_num += 1
                            self.open_file()
                            self.flush_to_disk(result_list)
                            result_list = [] 
                            self.batch_num = 0
                            print(f'Reach the limit, records have been saved in the file {self.current_path}')
                # self.open_file()
                # self.flush_to_disk(result_list)
                # result_list = []

            scroll_id = response['_scroll_id']

            scroll_body = {
                "scroll" : self.timeout,
                "scroll_id" : scroll_id
            }
            while len(batch) == self.page_size:
                response = self.es.scroll(body=scroll_body)
                batch = response['hits']['hits']
                if batch:
                    for item in batch:
                        if not self.is_reach_limit():
                            doc = item['_source']
                            result_list.append(doc)
                            self.total_num += 1
                            self.batch_num += 1
                        else:
                            if self.target_path:
                                self.open_file()
                                self.flush_to_disk(result_list)
                                result_list = [] 
                                print(f'Reach the limit, records have been saved in the file {self.target_path}')
                                return
                            elif self.target_dir:
                                # 如果是往一个目录下写入，且规定了一个文件最大的条数，则创建一个新文件写入后续数据
                                self.current_part_num += 1
                                self.open_file()
                                self.flush_to_disk(result_list)
                                result_list = [] 
                                self.batch_num = 0
                                print(f'Reach the limit, records have been saved in the file {self.current_path}')
            if len(result_list):
                print(f'The last batch (length is {len(result_list)}) have been saved in the file {self.current_path}')
                self.current_part_num += 1
                self.open_file()                           
                self.flush_to_disk(result_list)
                result_list = []
            self.target_file.close()
            self.insert_metadata()

        except Exception as ex:
            print(f'An error occured when scroll:: {ex}')
            raise ex

    def flush_to_disk(self, doc_list):
        if not doc_list:
            return
        write_str = ''
        for doc in doc_list:
            write_str += json.dumps(doc, ensure_ascii=False) + '\n'
        self.target_file.write(write_str)
        print(f'{len(doc_list)} are saved into file!')
        

    def open_file(self):
        if self.target_path:
            current_path = self.target_path
        else:
            current_path = f'{self.target_dir}/part{self.current_part_num}.jsonl'
        data_append = self.data_append
        if data_append:
            if not os.path.exists(current_path):
                with open(current_path, 'w'):
                    pass
            fp = open(current_path, 'a')
        else:
            fp = open(current_path, 'w')
        
        self.target_file = fp
        self.current_path = current_path

    def is_reach_limit(self):
        if (not self.max_num == 'unlimited') and isinstance(self.max_num, int):
            if self.batch_num >= self.max_num:
                print('Total count reach the limit!')
                return True
            else:
                return False
            
    def insert_metadata(self):
        insert_body = {
            'index_name':self.index_name,
            'operation':'get',
            'query':self.query,
            'operation_time': datetime.now().isoformat().split(".")[0],
            'get_num': self.total_num,
        }
        if self.target_path:
            insert_body['target_path'] = self.target_path
        elif self.target_dir:
            insert_body['target_dir'] = self.target_dirs
        self.es.index(index='metadata_index', body=insert_body)

if __name__ == '__main__':
    getDocs = GetDocs()
    getDocs.process()
