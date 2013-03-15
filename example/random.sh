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
for i in {0..100}; do curl -XPUT "localhost:9200/test/type1/$i" -d "{\"name\":\"rec $i\", \"number\":$i}"; done
curl -XPOST "http://localhost:9200/test/_refresh"
echo
echo "Salt 123"
curl -s "localhost:9200/test/type1/_search?pretty=true" -d '{
    "query": {
        "match_all": {}
    },
    "sort": {
        "_script": {
            "script": "random",
            "lang": "native",
            "type": "number",
            "params": {
                "salt": "123"
            }
        }
    }
}
' | grep \"_id\"
echo "Salt 123"
curl -s "localhost:9200/test/type1/_search?pretty=true" -d '{
    "query": {
        "match_all": {}
    },
    "sort": {
        "_script": {
            "script": "random",
            "lang": "native",
            "type": "number",
            "params": {
                "salt": "123"
            }
        }
    }
}
' | grep \"_id\"
echo "Salt 124"
curl -s "localhost:9200/test/type1/_search?pretty=true" -d '{
    "query": {
        "match_all": {}
    },
    "sort": {
        "_script": {
            "script": "random",
            "lang": "native",
            "type": "number",
            "params": {
                "salt": "124"
            }
        }
    }
}
' | grep \"_id\"
echo "No salt"
curl -s "localhost:9200/test/type1/_search?pretty=true" -d '{
    "query": {
        "match_all": {}
    },
    "sort": {
        "_script": {
            "script": "random",
            "lang": "native",
            "type": "number"
        }
    }
}
' | grep \"_id\"
echo "No salt"
curl -s "localhost:9200/test/type1/_search?pretty=true" -d '{
    "query": {
        "match_all": {}
    },
    "sort": {
        "_script": {
            "script": "random",
            "lang": "native",
            "type": "number"
        }
    }
}
' | grep \"_id\"
echo
