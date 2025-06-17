---
applies_to:
  stack: all
navigation_title: Find text structure API examples
---
# Find text structure API examples

The [find text structure API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-text-structure-find-structure) provides a starting point for ingesting data into {{es}} in a format that is suitable for subsequent use with other Elastic Stack functionality. This page shows you examples of using the API.  

## Finding the structure of NYC yellow cab example data [find-structure-example-nyc]

The next example shows how it's possible to find the structure of some New York City yellow cab trip data. The first `curl` command downloads the data, the first 20000 lines of which are then piped into the `find_structure` endpoint. The `lines_to_sample` query parameter of the endpoint is set to 20000 to match what is specified in the `head` command.

```
curl -s "s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-06.csv" | head -20000 | curl -s -H "Content-Type: application/json" -XPOST "localhost:9200/_text_structure/find_structure?pretty&lines_to_sample=20000" -T -
```

% NOTCONSOLE
% Not converting to console because this shows how curl can be used

::::{note}
The `Content-Type: application/json` header must be set even though in this case the data is not JSON. (Alternatively the `Content-Type` can be set to any other supported by {es}, but it must be set.)
::::

If the request does not encounter errors, you receive the following result:

```console-result
{
  "num_lines_analyzed" : 20000,
  "num_messages_analyzed" : 19998, <1>
  "sample_start" : "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount\n\n1,2018-06-01 00:15:40,2018-06-01 00:16:46,1,.00,1,N,145,145,2,3,0.5,0.5,0,0,0.3,4.3\n",
  "charset" : "UTF-8",
  "has_byte_order_marker" : false,
  "format" : "delimited", <2>
  "multiline_start_pattern" : "^.*?,\"?\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
  "exclude_lines_pattern" : "^\"?VendorID\"?,\"?tpep_pickup_datetime\"?,\"?tpep_dropoff_datetime\"?,\"?passenger_count\"?,\"?trip_distance\"?,\"?RatecodeID\"?,\"?store_and_fwd_flag\"?,\"?PULocationID\"?,\"?DOLocationID\"?,\"?payment_type\"?,\"?fare_amount\"?,\"?extra\"?,\"?mta_tax\"?,\"?tip_amount\"?,\"?tolls_amount\"?,\"?improvement_surcharge\"?,\"?total_amount\"?",
  "column_names" : [ <3>
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount"
  ],
  "has_header_row" : true, <4>
  "delimiter" : ",", <5>
  "quote" : "\"", <6>
  "timestamp_field" : "tpep_pickup_datetime", <7>
  "joda_timestamp_formats" : [ <8>
    "YYYY-MM-dd HH:mm:ss"
  ],
  "java_timestamp_formats" : [ <9>
    "yyyy-MM-dd HH:mm:ss"
  ],
  "need_client_timezone" : true, <10>
  "mappings" : {
    "properties" : {
      "@timestamp" : {
        "type" : "date"
      },
      "DOLocationID" : {
        "type" : "long"
      },
      "PULocationID" : {
        "type" : "long"
      },
      "RatecodeID" : {
        "type" : "long"
      },
      "VendorID" : {
        "type" : "long"
      },
      "extra" : {
        "type" : "double"
      },
      "fare_amount" : {
        "type" : "double"
      },
      "improvement_surcharge" : {
        "type" : "double"
      },
      "mta_tax" : {
        "type" : "double"
      },
      "passenger_count" : {
        "type" : "long"
      },
      "payment_type" : {
        "type" : "long"
      },
      "store_and_fwd_flag" : {
        "type" : "keyword"
      },
      "tip_amount" : {
        "type" : "double"
      },
      "tolls_amount" : {
        "type" : "double"
      },
      "total_amount" : {
        "type" : "double"
      },
      "tpep_dropoff_datetime" : {
        "type" : "date",
        "format" : "yyyy-MM-dd HH:mm:ss"
      },
      "tpep_pickup_datetime" : {
        "type" : "date",
        "format" : "yyyy-MM-dd HH:mm:ss"
      },
      "trip_distance" : {
        "type" : "double"
      }
    }
  },
  "ingest_pipeline" : {
    "description" : "Ingest pipeline created by text structure finder",
    "processors" : [
      {
        "csv" : {
          "field" : "message",
          "target_fields" : [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "RatecodeID",
            "store_and_fwd_flag",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount"
          ]
        }
      },
      {
        "date" : {
          "field" : "tpep_pickup_datetime",
          "timezone" : "{{ event.timezone }}",
          "formats" : [
            "yyyy-MM-dd HH:mm:ss"
          ]
        }
      },
      {
        "convert" : {
          "field" : "DOLocationID",
          "type" : "long"
        }
      },
      {
        "convert" : {
          "field" : "PULocationID",
          "type" : "long"
        }
      },
      {
        "convert" : {
          "field" : "RatecodeID",
          "type" : "long"
        }
      },
      {
        "convert" : {
          "field" : "VendorID",
          "type" : "long"
        }
      },
      {
        "convert" : {
          "field" : "extra",
          "type" : "double"
        }
      },
      {
        "convert" : {
          "field" : "fare_amount",
          "type" : "double"
        }
      },
      {
        "convert" : {
          "field" : "improvement_surcharge",
          "type" : "double"
        }
      },
      {
        "convert" : {
          "field" : "mta_tax",
          "type" : "double"
        }
      },
      {
        "convert" : {
          "field" : "passenger_count",
          "type" : "long"
        }
      },
      {
        "convert" : {
          "field" : "payment_type",
          "type" : "long"
        }
      },
      {
        "convert" : {
          "field" : "tip_amount",
          "type" : "double"
        }
      },
      {
        "convert" : {
          "field" : "tolls_amount",
          "type" : "double"
        }
      },
      {
        "convert" : {
          "field" : "total_amount",
          "type" : "double"
        }
      },
      {
        "convert" : {
          "field" : "trip_distance",
          "type" : "double"
        }
      },
      {
        "remove" : {
          "field" : "message"
        }
      }
    ]
  },
  "field_stats" : {
    "DOLocationID" : {
      "count" : 19998,
      "cardinality" : 240,
      "min_value" : 1,
      "max_value" : 265,
      "mean_value" : 150.26532653265312,
      "median_value" : 148,
      "top_hits" : [
        {
          "value" : 79,
          "count" : 760
        },
        {
          "value" : 48,
          "count" : 683
        },
        {
          "value" : 68,
          "count" : 529
        },
        {
          "value" : 170,
          "count" : 506
        },
        {
          "value" : 107,
          "count" : 468
        },
        {
          "value" : 249,
          "count" : 457
        },
        {
          "value" : 230,
          "count" : 441
        },
        {
          "value" : 186,
          "count" : 432
        },
        {
          "value" : 141,
          "count" : 409
        },
        {
          "value" : 263,
          "count" : 386
        }
      ]
    },
    (...)
  }
}
```

% NOTCONSOLE

1. `num_messages_analyzed` is 2 lower than `num_lines_analyzed` because only data records count as messages. The first line contains the column names and in this sample the second line is blank.
2. Unlike the first example, in this case the `format` has been identified as `delimited`.
3. Because the `format` is `delimited`, the `column_names` field in the output lists the column names in the order they appear in the sample.
4. `has_header_row` indicates that for this sample the column names were in the first row of the sample. (If they hadn't been then it would have been a good idea to specify them in the `column_names` query parameter.)
5. The `delimiter` for this sample is a comma, as it's CSV formatted text.
6. The `quote` character is the default double quote. (The structure finder does not attempt to deduce any other quote character, so if you have delimited text that's quoted with some other character you must specify it using the `quote` query parameter.)
7. The `timestamp_field` has been chosen to be `tpep_pickup_datetime`. `tpep_dropoff_datetime` would work just as well, but `tpep_pickup_datetime` was chosen because it comes first in the column order. If you prefer `tpep_dropoff_datetime` then force it to be chosen using the `timestamp_field` query parameter.
8. `joda_timestamp_formats` are used to tell {ls} how to parse timestamps.
9. `java_timestamp_formats` are the Java time formats recognized in the time fields. {es} mappings and ingest pipelines use this format.
10. The timestamp format in this sample doesn't specify a timezone, so to accurately convert them to UTC timestamps to store in {es} it's necessary to supply the timezone they relate to. `need_client_timezone` will be `false` for timestamp formats that include the timezone.

## Setting the timeout parameter [find-structure-example-timeout]

If you try to analyze a lot of data then the analysis will take a long time. If you want to limit the amount of processing your {es} cluster performs for a request, use the `timeout` query parameter. The analysis will be aborted and an error returned when the timeout expires. For example, you can replace 20000 lines in the previous example with 200000 and set a 1 second timeout on theanalysis:

```
curl -s "s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-06.csv" | head -200000 | curl -s -H "Content-Type: application/json" -XPOST "localhost:9200/_text_structure/find_structure?pretty&lines_to_sample=200000&timeout=1s" -T -
```

% NOTCONSOLE
% Not converting to console because this shows how curl can be used

Unless you are using an incredibly fast computer you'll receive a timeout error:

```console-result
{
  "error" : {
    "root_cause" : [
      {
        "type" : "timeout_exception",
        "reason" : "Aborting structure analysis during [delimited record parsing] as it has taken longer than the timeout of [1s]"
      }
    ],
    "type" : "timeout_exception",
    "reason" : "Aborting structure analysis during [delimited record parsing] as it has taken longer than the timeout of [1s]"
  },
  "status" : 500
}
```

% NOTCONSOLE

::::{note}
If you try the example above yourself you will note that the overall running time of the `curl` commands is considerably longer than 1 second. This is because it takes a while to download 200000 lines of CSV from the internet, and the timeout is measured from the time this endpoint starts to process the data.
::::

## Analyzing {es} log files [find-structure-example-eslog]

This is an example of analyzing an {{es}} log file:

```
curl -s -H "Content-Type: application/json" -XPOST
"localhost:9200/_text_structure/find_structure?pretty&ecs_compatibility=disabled" -T "$ES_HOME/logs/elasticsearch.log"
```

% NOTCONSOLE
% Not converting to console because this shows how curl can be used

If the request does not encounter errors, the result will look something like this:

```console-result
{
  "num_lines_analyzed" : 53,
  "num_messages_analyzed" : 53,
  "sample_start" : "[2018-09-27T14:39:28,518][INFO ][o.e.e.NodeEnvironment    ] [node-0] using [1] data paths, mounts [[/ (/dev/disk1)]], net usable_space [165.4gb], net total_space [464.7gb], types [hfs]\n[2018-09-27T14:39:28,521][INFO ][o.e.e.NodeEnvironment    ] [node-0] heap size [494.9mb], compressed ordinary object pointers [true]\n",
  "charset" : "UTF-8",
  "has_byte_order_marker" : false,
  "format" : "semi_structured_text", <1>
  "multiline_start_pattern" : "^\\[\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}", <2>
  "grok_pattern" : "\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel}.*", <3>
  "ecs_compatibility" : "disabled", <4>
  "timestamp_field" : "timestamp",
  "joda_timestamp_formats" : [
    "ISO8601"
  ],
  "java_timestamp_formats" : [
    "ISO8601"
  ],
  "need_client_timezone" : true,
  "mappings" : {
    "properties" : {
      "@timestamp" : {
        "type" : "date"
      },
      "loglevel" : {
        "type" : "keyword"
      },
      "message" : {
        "type" : "text"
      }
    }
  },
  "ingest_pipeline" : {
    "description" : "Ingest pipeline created by text structure finder",
    "processors" : [
      {
        "grok" : {
          "field" : "message",
          "patterns" : [
            "\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel}.*"
          ]
        }
      },
      {
        "date" : {
          "field" : "timestamp",
          "timezone" : "{{ event.timezone }}",
          "formats" : [
            "ISO8601"
          ]
        }
      },
      {
        "remove" : {
          "field" : "timestamp"
        }
      }
    ]
  },
  "field_stats" : {
    "loglevel" : {
      "count" : 53,
      "cardinality" : 3,
      "top_hits" : [
        {
          "value" : "INFO",
          "count" : 51
        },
        {
          "value" : "DEBUG",
          "count" : 1
        },
        {
          "value" : "WARN",
          "count" : 1
        }
      ]
    },
    "timestamp" : {
      "count" : 53,
      "cardinality" : 28,
      "earliest" : "2018-09-27T14:39:28,518",
      "latest" : "2018-09-27T14:39:37,012",
      "top_hits" : [
        {
          "value" : "2018-09-27T14:39:29,859",
          "count" : 10
        },
        {
          "value" : "2018-09-27T14:39:29,860",
          "count" : 9
        },
        {
          "value" : "2018-09-27T14:39:29,858",
          "count" : 6
        },
        {
          "value" : "2018-09-27T14:39:28,523",
          "count" : 3
        },
        {
          "value" : "2018-09-27T14:39:34,234",
          "count" : 2
        },
        {
          "value" : "2018-09-27T14:39:28,518",
          "count" : 1
        },
        {
          "value" : "2018-09-27T14:39:28,521",
          "count" : 1
        },
        {
          "value" : "2018-09-27T14:39:28,522",
          "count" : 1
        },
        {
          "value" : "2018-09-27T14:39:29,861",
          "count" : 1
        },
        {
          "value" : "2018-09-27T14:39:32,786",
          "count" : 1
        }
      ]
    }
  }
}
```

% NOTCONSOLE

1. This time the `format` has been identified as `semi_structured_text`.
2. The `multiline_start_pattern` is set on the basis that the timestamp appears in the first line of each multi-line log message.
3. A very simple `grok_pattern` has been created, which extracts the timestamp and recognizable fields that appear in every analyzed message. In this case the only field that was recognized beyond the timestamp was the log level.
4. The ECS Grok pattern compatibility mode used, may be one of either `disabled` (the default if not specified in the request) or `v1`

## Specifying `grok_pattern` as query parameter [find-structure-example-grok]

If you recognize more fields than the simple `grok_pattern` produced by the structure finder unaided then you can resubmit the request specifying a more advanced `grok_pattern` as a query parameter and the structure finder will calculate `field_stats` for your additional fields.

In the case of the {es} log a more complete Grok pattern is `\[%{TIMESTAMP_ISO8601:timestamp}\]\[%{LOGLEVEL:loglevel} *\]\[%{JAVACLASS:class} *\] \[%{HOSTNAME:node}\] %{JAVALOGMESSAGE:message}`. You can analyze the same text again, submitting this `grok_pattern` as a query parameter (appropriately URL escaped):

```
curl -s -H "Content-Type: application/json" -XPOST "localhost:9200/_text_structure/find_structure?pretty&format=semi_structured_text&grok_pattern=%5C%5B%25%7BTIMESTAMP_ISO8601:timestamp%7D%5C%5D%5C%5B%25%7BLOGLEVEL:loglevel%7D%20*%5C%5D%5C%5B%25%7BJAVACLASS:class%7D%20*%5C%5D%20%5C%5B%25%7BHOSTNAME:node%7D%5C%5D%20%25%7BJAVALOGMESSAGE:message%7D" -T "$ES_HOME/logs/elasticsearch.log"
```

% NOTCONSOLE
% Not converting to console because this shows how curl can be used

If the request does not encounter errors, the result will look something like this:

```console-result
{
  "num_lines_analyzed" : 53,
  "num_messages_analyzed" : 53,
  "sample_start" : "[2018-09-27T14:39:28,518][INFO ][o.e.e.NodeEnvironment    ] [node-0] using [1] data paths, mounts [[/ (/dev/disk1)]], net usable_space [165.4gb], net total_space [464.7gb], types [hfs]\n[2018-09-27T14:39:28,521][INFO ][o.e.e.NodeEnvironment    ] [node-0] heap size [494.9mb], compressed ordinary object pointers [true]\n",
  "charset" : "UTF-8",
  "has_byte_order_marker" : false,
  "format" : "semi_structured_text",
  "multiline_start_pattern" : "^\\[\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
  "grok_pattern" : "\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} *\\]\\[%{JAVACLASS:class} *\\] \\[%{HOSTNAME:node}\\] %{JAVALOGMESSAGE:message}", <1>
  "ecs_compatibility" : "disabled", <2>
  "timestamp_field" : "timestamp",
  "joda_timestamp_formats" : [
    "ISO8601"
  ],
  "java_timestamp_formats" : [
    "ISO8601"
  ],
  "need_client_timezone" : true,
  "mappings" : {
    "properties" : {
      "@timestamp" : {
        "type" : "date"
      },
      "class" : {
        "type" : "keyword"
      },
      "loglevel" : {
        "type" : "keyword"
      },
      "message" : {
        "type" : "text"
      },
      "node" : {
        "type" : "keyword"
      }
    }
  },
  "ingest_pipeline" : {
    "description" : "Ingest pipeline created by text structure finder",
    "processors" : [
      {
        "grok" : {
          "field" : "message",
          "patterns" : [
            "\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} *\\]\\[%{JAVACLASS:class} *\\] \\[%{HOSTNAME:node}\\] %{JAVALOGMESSAGE:message}"
          ]
        }
      },
      {
        "date" : {
          "field" : "timestamp",
          "timezone" : "{{ event.timezone }}",
          "formats" : [
            "ISO8601"
          ]
        }
      },
      {
        "remove" : {
          "field" : "timestamp"
        }
      }
    ]
  },
  "field_stats" : { <3>
    "class" : {
      "count" : 53,
      "cardinality" : 14,
      "top_hits" : [
        {
          "value" : "o.e.p.PluginsService",
          "count" : 26
        },
        {
          "value" : "o.e.c.m.MetadataIndexTemplateService",
          "count" : 8
        },
        {
          "value" : "o.e.n.Node",
          "count" : 7
        },
        {
          "value" : "o.e.e.NodeEnvironment",
          "count" : 2
        },
        {
          "value" : "o.e.a.ActionModule",
          "count" : 1
        },
        {
          "value" : "o.e.c.s.ClusterApplierService",
          "count" : 1
        },
        {
          "value" : "o.e.c.s.MasterService",
          "count" : 1
        },
        {
          "value" : "o.e.d.DiscoveryModule",
          "count" : 1
        },
        {
          "value" : "o.e.g.GatewayService",
          "count" : 1
        },
        {
          "value" : "o.e.l.LicenseService",
          "count" : 1
        }
      ]
    },
    "loglevel" : {
      "count" : 53,
      "cardinality" : 3,
      "top_hits" : [
        {
          "value" : "INFO",
          "count" : 51
        },
        {
          "value" : "DEBUG",
          "count" : 1
        },
        {
          "value" : "WARN",
          "count" : 1
        }
      ]
    },
    "message" : {
      "count" : 53,
      "cardinality" : 53,
      "top_hits" : [
        {
          "value" : "Using REST wrapper from plugin org.elasticsearch.xpack.security.Security",
          "count" : 1
        },
        {
          "value" : "adding template [.monitoring-alerts] for index patterns [.monitoring-alerts-6]",
          "count" : 1
        },
        {
          "value" : "adding template [.monitoring-beats] for index patterns [.monitoring-beats-6-*]",
          "count" : 1
        },
        {
          "value" : "adding template [.monitoring-es] for index patterns [.monitoring-es-6-*]",
          "count" : 1
        },
        {
          "value" : "adding template [.monitoring-kibana] for index patterns [.monitoring-kibana-6-*]",
          "count" : 1
        },
        {
          "value" : "adding template [.monitoring-logstash] for index patterns [.monitoring-logstash-6-*]",
          "count" : 1
        },
        {
          "value" : "adding template [.triggered_watches] for index patterns [.triggered_watches*]",
          "count" : 1
        },
        {
          "value" : "adding template [.watch-history-9] for index patterns [.watcher-history-9*]",
          "count" : 1
        },
        {
          "value" : "adding template [.watches] for index patterns [.watches*]",
          "count" : 1
        },
        {
          "value" : "starting ...",
          "count" : 1
        }
      ]
    },
    "node" : {
      "count" : 53,
      "cardinality" : 1,
      "top_hits" : [
        {
          "value" : "node-0",
          "count" : 53
        }
      ]
    },
    "timestamp" : {
      "count" : 53,
      "cardinality" : 28,
      "earliest" : "2018-09-27T14:39:28,518",
      "latest" : "2018-09-27T14:39:37,012",
      "top_hits" : [
        {
          "value" : "2018-09-27T14:39:29,859",
          "count" : 10
        },
        {
          "value" : "2018-09-27T14:39:29,860",
          "count" : 9
        },
        {
          "value" : "2018-09-27T14:39:29,858",
          "count" : 6
        },
        {
          "value" : "2018-09-27T14:39:28,523",
          "count" : 3
        },
        {
          "value" : "2018-09-27T14:39:34,234",
          "count" : 2
        },
        {
          "value" : "2018-09-27T14:39:28,518",
          "count" : 1
        },
        {
          "value" : "2018-09-27T14:39:28,521",
          "count" : 1
        },
        {
          "value" : "2018-09-27T14:39:28,522",
          "count" : 1
        },
        {
          "value" : "2018-09-27T14:39:29,861",
          "count" : 1
        },
        {
          "value" : "2018-09-27T14:39:32,786",
          "count" : 1
        }
      ]
    }
  }
}
```

% NOTCONSOLE

1. The `grok_pattern` in the output is now the overridden one supplied in the query parameter.
2. The ECS Grok pattern compatibility mode used, may be one of either `disabled` (the default if not specified in the request) or `v1`
3. The returned `field_stats` include entries for the fields from the overridden `grok_pattern`.

The URL escaping is hard, so if you are working interactively it is best to use the UI!
