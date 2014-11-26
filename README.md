alerting
========

This is the elasticsearch alerting plugin repo.

Creating an alert :

````
PUT /.alerts/alert/testalert
{
  "request" : {
    "indices" : [
      "logstash*"
    ],
    "body" : {
    "query" : {
      "filtered": {
        "query": {
          "match": {
            "response": 404
          }
        },
        "filter": {
          "range": {
          "@timestamp" : {
            "from": "{{SCHEDULED_FIRE_TIME}}||-5m",
            "to": "{{SCHEDULED_FIRE_TIME}}"  
          }
          }
        }
      } 
      }
    }
    }
  ,
  "trigger" : { "script" : {
    "script" : "hits.total > 1",
    "script_lang" : "groovy"
  } },
  "actions" : 
    {
      "email" : {
        "addresses" : ["brian.murphy@elasticsearch.com"]
      }
    },
    
    "schedule" : "0 0/1 * * * ?"
}

````
Expected response : 
````
{
   "_index": ".alerts",
   "_type": "alert",
   "_id": "testalert",
   "_version": 1,
   "created": true
}
````

Viewing an existing alert :

````
GET /.alerts/alert/testalert
````

````
{
   "found": true,
   "_index": ".alerts",
   "_type": "alert",
   "_id": "testalert",
   "_version": 1,
   "alert": {
      "trigger": {
         "script": {
            "script_lang": "groovy",
            "script": "hits.total > 1"
         }
      },
      "schedule": "0 0/1 * * * ?",
      "request": {
         "body": {
            "query": {
               "filtered": {
                  "query": {
                     "match": {
                        "response": 404
                     }
                  },
                  "filter": {
                     "range": {
                        "@timestamp": {
                           "to": "{{SCHEDULED_FIRE_TIME}}",
                           "from": "{{SCHEDULED_FIRE_TIME}}||-5m"
                        }
                     }
                  }
               }
            }
         },
         "indices": [
            "logstash*"
         ]
      },
      "actions": {
         "email": {
            "addresses": [
               "brian.murphy@elasticsearch.com"
            ]
         }
      }
   }
}
````

Deleting an alert :
````
DELETE /.alerts/alert/testalert
````

Expected output :
````
{
   "found": true,
   "_index": ".alerts",
   "_type": "alert",
   "_id": "testalert",
   "_version": 4
}
````

Creating a alert that looks uses a script to dig into an aggregation :
````
PUT _alert/404alert
{
  "request" : {
    "indices" : [
      "logstash*"
    ],
    "body" : {
     "query" : {
      "filtered": {
        "query": {
          "match_all": {}
        },
        "filter": {
          "range": {
          "@timestamp" : {
            "from": "{{SCHEDULED_FIRE_TIME}}||-5m",
            "to": "{{SCHEDULED_FIRE_TIME}}"  
            }
          }
        }
      } 
      },
      "aggs": {
        "response": {
          "terms": {
            "field": "response",
            "size": 100
          }
        }
      }, "size":0
    }
  },
  "trigger" : { 
    "script" : {
      "script" : "ok_count = 0.0;error_count = 0.0;for(bucket in aggregations.response.buckets) {if (bucket.key < 400){ok_count += bucket.doc_count;} else {error_count += bucket.doc_count;}}; return error_count/(ok_count+1) >= 0.1;",
    " script_lang" : "groovy"
  } },
  "actions" : 
    {
      "email" : {
        "addresses" : ["brian.murphy@elasticsearch.com"]
      }
    },
    "schedule" : "0 0/1 * * * ?"
}
````

This alert will trigger if the responses field has a value greater or equal to 400 for more than 10% of all values.
