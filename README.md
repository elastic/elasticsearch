alerting
========

This is the elasticsearch alerting plugin repo.

Sample Alert : 
````
POST _search/template/myAlertQuery
{ 
  "template": { "match_all" : {}  }
}

POST .alerts/alert/myTestAlert
{
    "query" : "myAlertQuery",
    "schedule" : "00 * * * * ?",
    "trigger" : {
         "numberOfEvents" : ">2"
     },
    "timeperiod" : "300s",
     "action" : {
         "email" : {
           "addresses" : [ "brian.murphy@elasticsearch.com" ]
         }
     },
    "version" : 1,
    "lastRan" : "2014-05-05T12:12:12.123Z", 
    "indices" : [ "logstash*" ] 
}
````

This will create an alert that runs over all events every minute looking at the last 5 minutes, sending an email to brian.murphy@elasticsearch.com when there are more than 2 events in a 5 minute window.

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
