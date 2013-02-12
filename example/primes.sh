curl -XDELETE "http://localhost:9200/test"
echo
curl -XPUT "http://localhost:9200/test/" -d '{
    "settings": {
        "index.number_of_shards": 1,
        "index.nubmer_of_replicas": 0
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
for i in {0..100}; do curl -XPUT "localhost:9200/test/type1/$i" -d "{\"name\":\"rec $i\", \"number\":$i}"; done
curl -XPOST "http://localhost:9200/test/_refresh"
echo
curl "localhost:9200/test/type1/_search?pretty=true" -d '{
    "query": {
        "filtered": {
            "query": {
                "match_all": {}
            },
            "filter": {
                "script": {
                    "script": "is_prime",
                    "lang": "native",
                    "params": {
                        "field": "number"
                    }
                }
            }
        }
    }
}
'

