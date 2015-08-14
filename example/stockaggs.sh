#!/bin/sh
curl -s -XDELETE "http://localhost:9200/transactions"
echo
curl -s -XPUT "http://localhost:9200/transactions/" -d '{
    "settings": {
        "index.number_of_shards": 1,
        "index.number_of_replicas": 0
    },
    "mappings": {
        "stock": {
            "properties": {
                "type": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "amount": {
                    "type": "long"
                }
            }
        }
    }
}'
echo
curl -s -XPUT 'http://localhost:9200/transactions/stock/1'  -d '{"type": "sale", "amount": 80}'
curl -s -XPUT 'http://localhost:9200/transactions/stock/2'  -d '{"type": "cost", "amount": 10}'
curl -s -XPUT 'http://localhost:9200/transactions/stock/3'  -d '{"type": "cost", "amount": 30}'
curl -s -XPUT 'http://localhost:9200/transactions/stock/4'  -d '{"type": "sale", "amount": 130}'

curl -s -XPOST "http://localhost:9200/transactions/_refresh"
echo
curl -s -XGET "localhost:9200/transactions/stock/_search?pretty=true" -d '{
    "query" : {
        "match_all" : {}
    },
    "aggs": {
        "profit": {
            "scripted_metric": {
                "init_script" : "stockaggs_init",
                "map_script" : "stockaggs_map",
                "combine_script" : "stockaggs_combine",
                "reduce_script" : "stockaggs_reduce",
                "lang": "native"
            }
        }
    },
    "size": 0
}'
