# ES数据库介绍

Elasticsearch是一个<font style="color:#DF2A3F;">开源的</font>分布式搜索和分析引擎，用于快速、可扩展地存储、搜索和分析大量数据。它提供了简单而强大的RESTful API，并且官方提供了各种语言的客户端api工具，使用户可以轻松地进行全文搜索、结构化查询、数据聚合和分析。

Elasticsearch被广泛应用于各种用例，包括日志分析、实时指标监控、全文搜索、安全情报分析等。它具有以下主要特点：

分布式架构：Elasticsearch采用分布式架构，可以将数据分散存储在多个节点上，实现横向扩展和高可用性。每个节点都具有相同的功能，包括数据存储、索引和搜索等，从而实现数据的分布和负载均衡。

实时搜索和分析：Elasticsearch提供了高效的全文搜索和匹配功能，支持复杂的查询语法和过滤器。它还能够实时地对大规模数据进行聚合和分析，如统计数据、分组、排序等操作。

强大的数据处理能力：Elasticsearch支持自动索引和分片数据，以实现高性能和可扩展性。它还提供了数据的近实时（near real-time）更新和检索，使用户可以快速获取最新的结果。

多种数据格式支持：Elasticsearch可以处理各种数据格式，包括结构化、半结构化和非结构化数据。它采用动态映射机制，能够自动推断和识别文档字段的类型，并根据需要进行索引和分析。

易用的开发接口：Elasticsearch提供了简单易用的RESTful API，使用户可以使用各种编程语言（如Java、Python、JavaScript等）与之交互。此外，Elasticsearch还提供了丰富的客户端库和工具，简化了开发和管理工作。

总的来说，Elasticsearch是一个强大而灵活的搜索和分析引擎，适用于处理大规模数据并实时获取有价值的信息。它的分布式架构、全文搜索和聚合分析功能使其成为许多应用和系统中的核心组件。

# ES数据库基本概念
Elasticsearch 是面向文档型数据库，一条数据在这里就是一个文档。数据存储格式均为json。这里将 Elasticsearch 里存储文档数据和关系型数据库 MySQL 存储数据的概念进行一个类比：


ES 里的 Index 可以看做一个库，而 Types 相当于表， Documents 则相当于表的行。这里 Types 的概念已经被逐渐弱化，由于Elasticsearch主要是面向文档型数据库， 通过倒排索引文档位置，不需要表的概念。Elasticsearch 7.X 中, Type 的概念已经被删除了。

## 索引（Index）
一个索引就是一个拥有几分相似特征的文档的集合，在概念上可以类比Mysql中的一个数据库。

Elasticsearch 索引的理念是：一切设计都是为了提高搜索的性能，为提高数据的查询速度，能搜索的数据必须索引。

## 文档（Doc）
一个文档是一个可被索引的基础信息单元，也就是一条数据。

比如：你可以拥有某一个客户的文档，某一个产品的一个文档，当然，也可以拥有某个订单的一个文档。文档以 JSON（Javascript Object Notation）格式来表示，在一个 index里可以存储任意多的文档。

## 字段（Field）
相当于是数据表的字段，对文档数据根据不同属性进行的分类标识。

# easy_es工具介绍

## 环境准备
1. 安装elasticsearch python客户端
pip install elasticsearch 
2. 搭建elasticsearch服务
在configs/security.xml中指定es服务ip和鉴权信息
3. 手动创建metadata_index用于存放操作日志，或在第一次调用create_index.py脚本时，如果metadata_index不存在会自动创建

## 总体说明
elasticsearch在Python端提供了一套api调用组件，但由于elasticsearch语法相对比较复杂，源数据提供的参数必须在python代码里修改，使用起来不是很方便。

比如，如果在某个index中查找满足text中包含“导弹”的清洗后的数据，无论是直接使用restful风格的api，还是直接调用python端的es组件，查询条件都为：

```python
{
	"query":{
		"bool":{
			"must":[{
				"match":{
					"text":"导弹"
				}
			},{
				"match":{
					"stage":"washed"
				}
			}]
		}
	}
}
```

为了从es中繁琐的语法中解放出来，同时增加安全验证、操作日志记录等功能，在python端的es组件的基础上做了一层封装，简化了基本的CRUD服务的同时，也提供以下功能：

（1）安全验证：使用了授权的用户名和密码才能够访问团队的elasticsearch集群。

（2）操作日志记录：所有CRUD操作都会记录在索引metadata_index中。典型的如：

```python
"_source": {
        "index_name": "test",
        "operation": "delete",
        "query": {
                "title": "电门"
                },
        "operation_time": "2024-08-21T16:48:12",
        "delete_num": 124,
        "current_num": 1191
            }
```

```python
"_source": {
        "index_name": "aerospace_book",
        "operation": "create",
        "create_time": "2024-08-23T10:45:59",
        "last_update_time": "2024-08-23T10:45:59",
        "last_id": 181684,
        "doc_num": 181685
            }
```

其中，operation为create的操作记录中，若其对应的index中doc的id值是按顺序+1递增的，last_id为当前数据库中已添加数据的最大id，doc_num为当前index中的文档数。

（3）统一配置：每个CRUD模块相关的参数都可在yaml风格的配置文件中进行统一配置，每一个功能模块中，都可用-config参数指定该模块的配置文件路径。

如运行批量添加文档程序时，可以使用：

```python
python add_docs.py
--config configs/add_docs.yaml
```

如果不加--config参数，则会使用每个模块对应的默认配置文件。

（4）操作结果提示：每次CRUD操作的结果详情都会在命令行中实时打印出来。

以下对该套工具的功能模块进行说明：

## es数据库连接
### 功能描述
建立与es数据库的连接。该模块是提供CRUD服务的前提，作为一个工具放在elastic_utils脚本中。

### 脚本路径
easy_es/elastic_utils.py

### 配置项
| 配置项 | 说明 |
| --- | --- |
| ip | es所在服务器ip，因为集群中的服务可以彼此连接，可以是集群中任一服务器的ip |
| port | 端口，默认为9200 |
| username | 用户名 |
| password | 密码 |
| use_ssl | 是否以https方式连接。这需要搭建的es服务支持https |

该部分配置文件在security.yml中，除use_ssl参数外必须指定，否则无法建立与es数据库的连接。

## 创建索引
### 功能描述
创建一个新的索引。

### 脚本路径
create_index.py

### 默认配置文件路径
configs/create_index.yaml

### 配置项
| 配置项 | 说明 |
| --- | --- |
| index_name | 创建的索引名称 |
| number_of_shards | 索引分片数 |
| number_of_replicas | 每个分片的副本数 |
| mapping_fields | 所创建索引的字段约束规则 |


```python
index_name: test
number_of_shards: 2
number_of_replicas: 1
mapping_fields:
  index: 
    type: integer
    index: false
  title:
    type: text
    index: true
  text:
    type: text
    index: true
  stage:
    type: keyword
    index: true
  lang:
    type: keyword
    index: true
```

## 添加单条文档
### 功能描述
添加单独一条doc。一般是用于补充某个特殊的数据，或输入用于测试的数据。

### 脚本路径
add_one_doc.py

### 默认配置文件路径
configs/add_one_doc.yaml

### 配置项
| 配置项 | 说明 |
| --- | --- |
| index_name | 添加数据的索引名称 |
| doc | 需要添加的文档字段，是key:value形式的dict，注意value可以用$time-now$，表示为当前时间。 |
| id | 要添加doc的id值，id有三种取值：（1）random es随机生成id （2）continue 在原有index中最大id基础上+1 （3）除以上两种外均视为指定id |


```python
index_name: test
doc:
  title: test3
  text: This is a text for test, num is 3
  time: $time-now$
id: continue # 可以加id，id有三种取值：（1）random es随机生成 （2）continue 在原有index基础上增加 （3）除以上两种外均视为指定id
```

## 批量添加文档
### 功能描述
批量添加doc

### 脚本路径
add_docs.py

### 默认配置文件路径
configs/add_docs.yaml

### 配置项
| 配置项 | 说明 |
| --- | --- |
| index_name | 添加数据的索引名称 |
| metadata | 在原本json的基础上增加的元数据，是key:value形式的dict。 |
| id | 批量添加doc的id生成方式，id有两种取值：（1）random es随机生成id （2）continue 在原有index中最大id基础上+1  |
| source_dir | 导入的源数据所在的文件夹或文件路径。如果是文件夹，该文件夹下的所有符合suffix_list后缀的文件中的数据都会被导入。 |
| suffix_list | 导入es的文件后缀名。为list类型，只有后缀名在该列表中的文件中的数据才会被导入到es中。 |


```python
index_name: test
metadata: # 在原本json的基础上增加的元数据
  stage: washed # 有多个阶段：origin原始的 washed清洗后的
  lang: zh # 标记语种   
id: continue # id有两种方式：（1）random 即不传递id，el自动随机生成 （2）continue 按照数字顺序设置id
source_dir: test.jsonl
# source_dir: test
suffix_list:
  - json
  - jsonl
```

## 查询并下载数据
### 功能描述
根据条件查询数据，查询到数据并下载到指定文件或目录。

### 脚本路径
get_docs.py

### 默认配置文件路径
configs/get_docs.yaml

### 配置项
| 配置项 | 说明 |
| --- | --- |
| index_name | 获取数据的索引名称 |
| query | 获取数据的查询条件。是key:value形式的dict，也可以是”all”，表示获取该index中所有数据。 |
| max_num | 每个文件中doc的最大数量。如果导入到文件（有target_path字段），则表示只取前max_num条数据，如果导入到文件夹（target_dir字段）表示读取数据后，在该文件夹下创建的文件最多包含max_num条数据。可以是"unlimited"，表示不限制条数。 |
| data_append | 是否追加到目标文件。true为追加到target_path指向的目标文件，false则直接覆盖。 |
| target_path | 数据导入的文件。优先级比target_dir高，如果再配置了target_dir，则target_dir失效。 |
| target_dir | 数据导入的文件夹。导出数据会以part0.jsonl、part1.jsonl的方式在该目录下创建文件并写入数据，文件大小按max_num分割。优先级比target_path低，如果再配置了target_path，则target_dir失效。 |


```python
index_name: aerospace_manual
# max_num: unlimited
max_num: 10
timeout: 10m
query: 
  stage: washed # 有多个阶段：origin原始的 washed清洗后的
  lang: zh # 标记语种
  text: 飞机控制
target_path: test/quality_zh.jsonl
# 当数据规模太大不适合放在一个文件中时，可以提供target_dir，自动把文件分为若干个part放在target_dir中
# 当target_path和target_dir同时存在时，target_dir不生效
# target_dir: test
data_append: true
```

## 修改文档
### 功能描述
更新指定的doc。

### 脚本路径
update_docs.py

### 默认配置文件路径
configs/update_docs.yaml

### 配置项
| 配置项 | 说明 |
| --- | --- |
| index_name | 获取数据的索引名称 |
| query | 所更新数据的查询条件，限制更新数据的范围。是key:value形式的dict，也可以是”all”，表示更新该index中所有数据。 |
| id | 要更新的文档id。只针对单条doc，如果定义了id，则query失效。 |
| doc | 数据的更新字段。是key:value形式的dict。value可以是$time-now$，表示当前时间。 |


```python
index_name: test
# 要更新的文档id，只针对单条doc，如果定义了id，则query失效
# id: 0
# 用于查询的字段
# query:
# 要更新的内容
query: all
doc:
  update: all changed from python 

```

## 删除文档
### 功能描述
删除指定的doc。

### 脚本路径
delete_docs.py

### 默认配置文件路径
configs/delete_docs.yaml

### 配置项
| 配置项 | 说明 |
| --- | --- |
| index_name | 删除数据的索引名称 |
| query | 所删除数据的查询条件，限制删除数据的范围。是key:value形式的dict，也可以是”all”，表示删除该index中所有数据。 |
| id | 要更新的文档id。只针对单条doc，如果定义了id，则query失效。 |


```python
index_name: test
query:
  title: test1
```

