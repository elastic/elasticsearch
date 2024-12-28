import json
import os
import yaml
import argparse
from elasticsearch import Elasticsearch 
from datetime import datetime

from elastic_utils import create_es

class CreateIndex:
    config_path = f'{os.path.dirname(__file__)}/configs/create_index.yaml'
    number_of_shards = 2
    number_of_replicas = 1
    all_fields_text = False
    use_ik = False # 是否使用ik分词器

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
        self.es = create_es(self.ip, self.port, self.username, self.password)

    def load_config(self):
        with open(self.config_path, 'r') as cp:
            config = yaml.safe_load(cp)
        # 先建立日志
            self.ip = config.get('ip')
            self.port = config.get('port')
            self.url = f'http://{self.ip}:{self.port}'
            self.username = config.get('username') if config.get('username') else self.username
            self.password = config.get('password') if config.get('password') else self.password
            self.auth_pair = (self.username, self.password)
            self.index_name = config.get('index_name')
            self.number_of_shards = config.get('number_of_shards')
            self.number_of_replicas = config.get('number_of_replicas')
            self.use_ik = config.get('use_ik') if config.get('use_ik') is not None else self.use_ik
            self.mapping_fields = config.get('mapping_fields')
            self.create_comment = config.get('comment')
            self.all_fields_text = config.get('all_fields_text')
    
    def create_index(self):
        index_name = self.index_name
        is_exist = self.es.indices.exists(index=index_name)
        if is_exist:
            print(f'{index_name}已经存在！！')
            return

        if self.mapping_fields:
            # mapping_properties = self.mapping_fields
            # for key in mapping_properties.keys():
            #     value_obj = mapping_properties[key]
            #     field_type = value_obj['type']
            #     field_index = value_obj['index']
            #     mapping_properties[key] = {
            #         'type': field_type,
            #         'index': field_index
            #     }
            mapping_properties = self.mapping_fields
        else:
            mapping_properties = {}
        
        body = {
            'settings':{
                'number_of_shards': self.number_of_shards,
                'number_of_replicas': self.number_of_replicas
            }
        }
        if self.use_ik:
            body['settings']['analysis'] = {
                "analyzer":{
                    "default":{
                            "type":"ik_max_word"
                        }
                    },            
                "search_analyzer":{
                    "default":{
                        "type":"ik_smart"
                        }
                    }
                }
        if mapping_properties:
            body['mappings'] = {}
            body['mappings']['properties'] = mapping_properties
        if self.all_fields_text == True:
            body['mappings'] = {}
            body['mappings']['dynamic_templates'] = [{
                "all_fields_as_text": {
                    "match_mapping_type": "*",
                    "mapping": {
                        "type": "text"
                    }
                }
            }]
        body_json = json.dumps(body, ensure_ascii=False)
        try:
            response = self.es.indices.create(index=self.index_name, body=body_json)
            if response['acknowledged']:
                print(f"Index '{self.index_name}' created.")
                if not self.es.indices.exists(index='metadata_index'):
                    self.create_metadata_index()
                    # print(f"Index 'metadata_index' created.")
                insert_body = {
                    'index_name':index_name,
                    'operation': 'create',
                    'create_time':datetime.now().isoformat().split(".")[0],
                    'last_update_time': datetime.now().isoformat().split(".")[0],
                    'last_id': -1,
                    'doc_num': 0
                }
                try:
                    self.es.index(index='metadata_index', body=json.dumps(insert_body, ensure_ascii=False))
                except Exception as insert_ex:
                    print(f'An error occured when write to the metadata_index: {insert_ex}')
            else:
                print("Failed to create index.")
        except Exception as ex:
            print('An error occured: ', ex)

    def delete_index(self, index_name):
        self.es.indices.delete(index=index_name)

    def create_metadata_index(self):
        index_name = 'metadata_index'
        is_exist = self.es.indices.exists(index=index_name)
        if is_exist:
            print(f'{index_name}已经存在！！')
            return

        body = {
            'settings':{
                'number_of_shards': self.number_of_shards,
                'number_of_replicas': self.number_of_replicas
            }
        }
        body_json = json.dumps(body, ensure_ascii=False)
        create_response = self.es.indices.create(index=index_name, body=body_json)
        if create_response['acknowledged']:
            print(f"Index '{index_name}' created.")
            insert_body = {
                'index_name':index_name,
                'operation': 'create',
                'create_time':datetime.now().isoformat().split(".")[0],
                'last_update_time': datetime.now().isoformat().split(".")[0],
                'last_id': -1,
                'doc_num': 0,
            }
            if self.create_comment:
                insert_body['comment'] = self.create_comment
            self.es.index(index=index_name, body=json.dumps(insert_body, ensure_ascii=False))
        else:
            print("Failed to create index.")        


if __name__ == '__main__':
    ci = CreateIndex()
    ci.create_index()
    # ci.delete_index(ci.index_name)
    # ci.delete_index("metadata_index")
    