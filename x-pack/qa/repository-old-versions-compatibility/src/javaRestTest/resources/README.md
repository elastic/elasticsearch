
### Create data structure and config file
```
mkdir /tmp/sharedESData
mkdir /tmp/sharedESData/config
mkdir /tmp/sharedESData/data
mkdir /tmp/sharedESData/snapshots
```

```
touch /tmp/sharedESData/config/elasticsearch.yml

cat <<EOF >> /tmp/sharedESData/config/elasticsearch.yml
cluster.name: "archive-indides-test"
node.name: "node-1"
path.repo: ["/usr/share/elasticsearch/snapshots"]
network.host: 0.0.0.0
http.port: 9200

discovery.type: single-node
xpack.security.enabled: false
EOF
```

### Define path
```
SHARED_FOLDER=/tmp/sharedESData
```

### Deploy container
```
docker run -d --name es \
-p 9200:9200 -p 9300:9300 \
-v ${SHARED_FOLDER}/config/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml \
-v ${SHARED_FOLDER}/data:/usr/share/elasticsearch/data \
-v ${SHARED_FOLDER}/snapshots:/usr/share/elasticsearch/snapshots \
--env "discovery.type=single-node" \
docker.elastic.co/elasticsearch/elasticsearch:5.6.16

// Version 6
docker.elastic.co/elasticsearch/elasticsearch:6.8.23
```

### Create Index Version 5
```
PUT /index
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1
  },
  "mappings": {
    "my_type": {
      "properties": {
        "title": {
          "type": "text"
        },
        "created_at": {
          "type": "date"
        },
        "views": {
          "type": "integer"
        }
      }
    }
  }
}
```

### Create Index Version 5 - Custom-Analyzer
```
PUT /index
{
  "settings": {
    "analysis": {
      "analyzer": {
        "custom_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "standard",
            "lowercase"
          ]
        }
      }
    }
  },
  "mappings": {
    "my_type": {
      "properties": {
        "content": {
          "type": "text",
          "analyzer": "custom_analyzer"
        }
      }
    }
  }
}
```

### Create Index Version 6
```
PUT /index
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1
  },
  "mappings": {
    "_doc": {
      "properties": {
        "title": {
          "type": "text"
        },
        "content": {
          "type": "text"
        },
        "created_at": {
          "type": "date"
        }
      }
    }
  }
}
```

### Create Index Version 6 - Custom-Analyzer
```
PUT /index
{
  "settings": {
    "analysis": {
      "analyzer": {
        "custom_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "standard",
            "lowercase"
          ]
        }
      }
    }
  },
  "mappings": {
    "doc": {
      "properties": {
        "content": {
          "type": "text",
          "analyzer": "custom_analyzer"
        }
      }
    }
  }
}
```

### Add documents Version 5
```
POST /index/my_type
{
  "title": "Title 5",
  "content": "Elasticsearch is a powerful search engine.",
  "created_at": "2024-12-16"
}
```

### Add documents Version 5 - Custom-Analyzer
```
POST /index/my_type
{
  "content": "Doc 1"
}
```

### Add documents Version 6
```
POST /index/_doc
{
  "title": "Title 5",
  "content": "Elasticsearch is a powerful search engine.",
  "created_at": "2024-12-16"
}
```

### Add documents Version 5 - Custom-Analyzer
```
POST /index/_doc
{
  "content": "Doc 1"
}
```

### Register repository
```
PUT /_snapshot/repository
{
  "type": "fs",
  "settings": {
    "location": "/usr/share/elasticsearch/snapshots",
    "compress": true
  }
}
```

### Create a snapshot
```
PUT /_snapshot/repository/snapshot
{
    "indices": "index",
    "ignore_unavailable": "true",
    "include_global_state": false
}
```

### Create zip file
```
zip -r snapshot.zip /tmp/sharedESData/snapshots/*
```

### Cleanup
```
docker rm -f es
rm -rf /tmp/sharedESData/
```
