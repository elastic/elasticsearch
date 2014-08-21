alerting
========

This is the elasticsearch alerting plugin repo.

````

POST _search/template/webErrors
{ 
  "template": { "range" : {
      "response" : {
        "from" :400,
        "to" : 600
      }
  }  
  }
}

DELETE /_alerting/_delete/myNewAlert
POST /_alerting/_create/myNewAlert
{
    "query" : "webErrors",
    "schedule" : "05 * * * * ?",
    "trigger" : {
         "numberOfEvents" : ">2"
     },
    "timeperiod" : "300s",
     "action" : {
         "email" : {
           "addresses" : [ "brian.murphy@elasticsearch.com" ],
           "display": "message"
         }
     },
    "indices" : [ "logstash*" ],
    "enabled" : false
}



POST /_alerting/_enable/myNewAlert

DELETE /_alerting/_delete/myIndexAlert
POST /_alerting/_create/myIndexAlert
{
    "query" : "webErrors",
    "schedule" : "05 * * * * ?",
    "trigger" : {
         "numberOfEvents" : ">0"
     },
    "timeperiod" : "300s",
     "action" : {
         "index" : {
           "index" : "weberrorhistory",
           "type" : "weberrorresult"
         }
     },
    "indices" : [ "logstash*" ],
    "enabled" : true
}


POST /_search/template/testFilteredAgg
{
  "query" : { 
    "filtered" : {
      "query" : { 
        "match_all" : {}
     },
     "filter": {
       "range" : {
         "@timestamp" : {
             "gte" : "{{from}}",
             "lt" : "{{to}}"
         }
       }
     }
    } 
  },
    "aggs" : { 
      "response" : {
        "terms" : {
          "field" : "response",
          "size" : 100
        }
      }
}, "size" : 0  }


POST /_scripts/groovy/testScript 
{
  "script" : "ok_count = 0.0;error_count = 0.0;for(bucket in aggregations.response.buckets) {if (bucket.key < 400){ok_count += bucket.doc_count;} else {error_count += bucket.doc_count;}}; return error_count/(ok_count+1) >= 0.1;"
}

DELETE /_alerting/_delete/myScriptedAlert
POST /_alerting/_create/myScriptedAlert
{
    "query" : "testFilteredAgg",
    "schedule" : "05 * * * * ?",
    "trigger" : {
         "script" : {
           "script" : "testScript",
           "script_lang" : "groovy",
           "script_type" : "INDEXED"
         }
     },
    "timeperiod" : "300s",
     "action" : {
         "index" : {
           "index" : "weberrorhistory",
           "type" : "weberrorresult"
         },
        "email" : {
           "addresses" : [ "brian.murphy@elasticsearch.com" ],
           "display": "message"
         }

     },
    "indices" : [ "logstash*" ],
    "enabled" : true,
    "simple" : false
}

````


The email will look like : 
````
The following query triggered because numberOfEvents > 1
The total number of hits returned : 25
For query : {
  "query" : {
    "filtered" : {
      "query" : {
        "template" : {
          "id" : "myAlertQuery"
        }
      },
      "filter" : {
        "range" : {
          "@timestamp" : {
            "gte" : "2014-08-13T16:45:00.000Z",
            "lt" : "2014-08-13T16:50:00.000Z"
          }
        }
      }
    }
  }
}

Indices : logstash*/

128.141.154.156 - - [13/Aug/2014:17:46:21 +0100] "GET /blog/geekery/debugging-java-performance.html HTTP/1.1" 200 15796 "http://logstash.net/docs/1.1.6/life-of-an-event" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"
128.141.154.156 - - [13/Aug/2014:17:46:21 +0100] "GET /reset.css HTTP/1.1" 200 1015 "http://www.semicomplete.com/blog/geekery/debugging-java-performance.html" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"
128.141.154.156 - - [13/Aug/2014:17:46:21 +0100] "GET /style2.css HTTP/1.1" 200 4877 "http://www.semicomplete.com/blog/geekery/debugging-java-performance.html" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"
50.16.19.13 - - [13/Aug/2014:17:48:06 +0100] "GET /blog/tags/puppet?flav=rss20 HTTP/1.1" 200 14872 "http://www.semicomplete.com/blog/tags/puppet?flav=rss20" "Tiny Tiny RSS/1.11 (http://tt-rss.org/)"
50.7.228.180 - - [13/Aug/2014:17:48:32 +0100] "GET /misc/sample.log HTTP/1.1" 200 54306753 "http://www.semicomplete.com/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:22.0) Gecko/20100101 Firefox/22.0"
193.138.160.116 - - [13/Aug/2014:17:49:23 +0100] "GET /scripts/backup/ HTTP/1.1" 200 1328 "http://www.semicomplete.com/scripts/" "Mozilla/5.0 (X11; Linux i686; rv:18.0) Gecko/20100101 Firefox/18.0 Iceweasel/18.0.1"
128.141.154.156 - - [13/Aug/2014:17:46:21 +0100] "GET /images/jordan-80.png HTTP/1.1" 200 6146 "http://www.semicomplete.com/blog/geekery/debugging-java-performance.html" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"
128.141.154.156 - - [13/Aug/2014:17:46:22 +0100] "GET /favicon.ico HTTP/1.1" 200 3638 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"
195.168.193.181 - - [13/Aug/2014:17:48:15 +0100] "GET /projects/xdotool/ HTTP/1.1" 200 12292 "http://www.linuxquestions.org/questions/programming-9/simulating-a-mouse-click-594576/" "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:27.0) Gecko/20100101 Firefox/27.0"
193.138.160.116 - - [13/Aug/2014:17:48:42 +0100] "GET /scripts/parsehttp HTTP/1.1" 200 332 "http://www.semicomplete.com/scripts/" "Mozilla/5.0 (X11; Linux i686; rv:18.0) Gecko/20100101 Firefox/18.0 Iceweasel/18.0.1"
````
