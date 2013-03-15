curl -XDELETE "http://localhost:9200/test"
echo
curl -XPUT "http://localhost:9200/test/" -d '{
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
curl -XPUT "localhost:9200/test/type1/1" -d '{"name" : "foo bar baz", "number": 10000 }'
curl -XPUT "localhost:9200/test/type1/2" -d '{"name" : "foo foo foo", "number": 1 }'
curl -XPOST "http://localhost:9200/test/_refresh"
echo
curl -s "localhost:9200/test/type1/_search?pretty=true" -d '{
    "query": {
        "custom_score" : {
            "query": {
                "match": {
                    "name" : "foo"
                }
            },
            "script": "popularity",
            "lang": "native",
            "params": {
                "field": "number"
            }
        }
    }
}
'
echo
curl -s "localhost:9200/test/type1/_search?pretty=true" -d '{
    "query": {
        "custom_score" : {
            "query": {
                "match": {
                    "name" : "foo"
                }
            },
            "script": "_score * (1 + Math.log10(doc[field].value + 1))",
            "params": {
                "field": "number"
            }
        }
    }
}
'
echo