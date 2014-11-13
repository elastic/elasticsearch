alerting
========

This is the elasticsearch alerting plugin repo.

Creating an alert :

````
PUT _alert/testalert
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
    
    "schedule" : "0 0/1 * * * ?",
    "enable" : true
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
      "enable": true,
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
DELETE _alert/testalert
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


