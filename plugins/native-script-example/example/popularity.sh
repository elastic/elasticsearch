#!/bin/sh
curl -s -XDELETE "http://localhost:9200/test"
echo
curl -s -XPUT "http://localhost:9200/test/" -d '{
    "settings": {
        "index.number_of_shards": 1,
        "index.number_of_replicas": 0
    },
    "mappings": {
        "type1": {
            "properties": {
                "name": {
                    "type": "string"
                },
                "number": {
                    "type": "integer"
                }
            }
        }
    }
}'
echo
curl -s -XPUT "localhost:9200/test/type1/1" -d '{"name" : "foo bar baz", "number": 10000 }'
curl -s -XPUT "localhost:9200/test/type1/2" -d '{"name" : "foo foo foo", "number": 1 }'
curl -s -XPOST "http://localhost:9200/test/_refresh"
echo
curl -s "localhost:9200/test/type1/_search?pretty=true" -d '{
    "query": {
        "function_score" : {
            "boost_mode": "replace",
            "query": {
                "match": {
                    "name": "foo"
                }
            },
            "script_score": {
                "script": "popularity",
                "lang": "native",
                "params": {
                    "field": "number"
                }
            }
        }
    }
}
'
echo