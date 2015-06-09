Python lang Plugin for Elasticsearch
==================================

The Python (jython) language plugin allows to have `python` as the language of scripts to execute.

In order to install the plugin, simply run: 

```sh
bin/plugin install elasticsearch/elasticsearch-lang-python/2.5.0
```

You need to install a version matching your Elasticsearch version:

| elasticsearch |  Python Lang Plugin   |   Docs     |  
|---------------|-----------------------|------------|
| master        |  Build from source    | See below  |
| es-1.x        |  Build from source    | [2.6.0-SNAPSHOT](https://github.com/elasticsearch/elasticsearch-lang-python/tree/es-1.x/#version-260-snapshot-for-elasticsearch-1x)  |
|    es-1.5              |     2.5.0         | [2.5.0](https://github.com/elastic/elasticsearch-lang-python/tree/v2.5.0/#version-250-for-elasticsearch-15)                  |
|    es-1.4              |     2.4.1         | [2.4.1](https://github.com/elasticsearch/elasticsearch-lang-python/tree/v2.4.1/#version-241-for-elasticsearch-14)                  |
|    es-1.3              |     2.3.1         | [2.3.1](https://github.com/elasticsearch/elasticsearch-lang-python/tree/v2.3.1/#version-231-for-elasticsearch-13)                  |
| < 1.3.5       |  2.3.0                | [2.3.0](https://github.com/elasticsearch/elasticsearch-lang-python/tree/v2.3.0/#version-230-for-elasticsearch-13)                  |
| es-1.2        |  2.2.0                | [2.2.0](https://github.com/elasticsearch/elasticsearch-lang-python/tree/v2.2.0/#python-lang-plugin-for-elasticsearch)  |
| es-1.0        |  2.0.0                | [2.0.0](https://github.com/elasticsearch/elasticsearch-lang-python/tree/v2.0.0/#python-lang-plugin-for-elasticsearch)  |
| es-0.90       |  1.0.0                | [1.0.0](https://github.com/elasticsearch/elasticsearch-lang-python/tree/v1.0.0/#python-lang-plugin-for-elasticsearch)  |

To build a `SNAPSHOT` version, you need to build it with Maven:

```bash
mvn clean install
plugin --install lang-python \
       --url file:target/releases/elasticsearch-lang-python-X.X.X-SNAPSHOT.zip
```

User Guide
----------

Using python with function_score
--------------------------------

Let's say you want to use `function_score` API using `python`. Here is
a way of doing it:

```sh
curl -XDELETE "http://localhost:9200/test"

curl -XPUT "http://localhost:9200/test/doc/1" -d '{
  "num": 1.0
}'

curl -XPUT "http://localhost:9200/test/doc/2?refresh" -d '{
  "num": 2.0
}'

curl -XGET "http://localhost:9200/test/_search?pretty" -d'
{
  "query": {
    "function_score": {
      "script_score": {
        "script": "doc[\"num\"].value * _score",
        "lang": "python"
      }
    }
  }
}'
```

gives

```javascript
{
   // ...
   "hits": {
      "total": 2,
      "max_score": 2,
      "hits": [
         {
            // ...
            "_score": 2
         },
         {
            // ...
            "_score": 1
         }
      ]
   }
}
```

Using python with script_fields
-------------------------------

```sh
curl -XDELETE "http://localhost:9200/test"

curl -XPUT "http://localhost:9200/test/doc/1?refresh" -d'
{
  "obj1": {
   "test": "something"
  },
  "obj2": {
    "arr2": [ "arr_value1", "arr_value2" ]
  }
}'

curl -XGET "http://localhost:9200/test/_search" -d'
{
  "script_fields": {
    "s_obj1": {
      "script": "_source[\"obj1\"]", "lang": "python"
    },
    "s_obj1_test": {
      "script": "_source[\"obj1\"][\"test\"]", "lang": "python"
    },
    "s_obj2": {
      "script": "_source[\"obj2\"]", "lang": "python"
    },
    "s_obj2_arr2": {
      "script": "_source[\"obj2\"][\"arr2\"]", "lang": "python"
    }
  }
}'
```

gives

```javascript
{
  // ...
  "hits": [
     {
        // ...
        "fields": {
           "s_obj2_arr2": [
              [
                 "arr_value1",
                 "arr_value2"
              ]
           ],
           "s_obj1_test": [
              "something"
           ],
           "s_obj2": [
              {
                 "arr2": [
                    "arr_value1",
                    "arr_value2"
                 ]
              }
           ],
           "s_obj1": [
              {
                 "test": "something"
              }
           ]
        }
     }
  ]
}
```

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
