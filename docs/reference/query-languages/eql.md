---
navigation_title: "EQL"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/eql.html
---

# EQL search [eql]

:::{note}
This section provides detailed **reference information**.

Refer to [EQL overview](docs-content://explore-analyze/query-filter/languages/eql.md) in the **Explore and analyze** section for overview and conceptual information about the SQL query language.
:::


Event Query Language (EQL) is a query language for event-based time series data, such as logs, metrics, and traces.


## Advantages of EQL [eql-advantages]

* **EQL lets you express relationships between events.**<br> Many query languages allow you to match single events. EQL lets you match a sequence of events across different event categories and time spans.
* **EQL has a low learning curve.**<br> [EQL syntax](/reference/query-languages/eql/eql-syntax.md) looks like other common query languages, such as SQL. EQL lets you write and read queries intuitively, which makes for quick, iterative searching.
* **EQL is designed for security use cases.**<br> While you can use it for any event-based data, we created EQL for threat hunting. EQL not only supports indicator of compromise (IOC) searches but can describe activity that goes beyond IOCs.


## Required fields [eql-required-fields]

With the exception of sample queries, EQL searches require that the searched data stream or index  contains a *timestamp* field. By default, EQL uses the `@timestamp` field from the [Elastic Common Schema (ECS)][Elastic Common Schema (ECS)](ecs://reference/index.md)).

EQL searches also require an *event category* field, unless you use the [`any` keyword](/reference/query-languages/eql/eql-syntax.md#eql-syntax-match-any-event-category) to search for  documents without an event category field. By default, EQL uses the ECS `event.category` field.

To use a different timestamp or event category field, see [Specify a timestamp or event category field](#specify-a-timestamp-or-event-category-field).

::::{tip}
While no schema is required to use EQL, we recommend using the [ECS](ecs://reference/index.md). EQL searches are designed to work with core ECS fields by default.
::::



## Run an EQL search [run-an-eql-search]

Use the [EQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-search) to run a [basic EQL query](/reference/query-languages/eql/eql-syntax.md#eql-basic-syntax).

```console
GET /my-data-stream/_eql/search
{
  "query": """
    process where process.name == "regsvr32.exe"
  """
}
```

By default, basic EQL queries return the 10 most recent matching events in the `hits.events` property. These hits are sorted by timestamp, converted to milliseconds since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time), in ascending order.

```console-result
{
  "is_partial": false,
  "is_running": false,
  "took": 60,
  "timed_out": false,
  "hits": {
    "total": {
      "value": 2,
      "relation": "eq"
    },
    "events": [
      {
        "_index": ".ds-my-data-stream-2099.12.07-000001",
        "_id": "OQmfCaduce8zoHT93o4H",
        "_source": {
          "@timestamp": "2099-12-07T11:07:09.000Z",
          "event": {
            "category": "process",
            "id": "aR3NWVOs",
            "sequence": 4
          },
          "process": {
            "pid": 2012,
            "name": "regsvr32.exe",
            "command_line": "regsvr32.exe  /s /u /i:https://...RegSvr32.sct scrobj.dll",
            "executable": "C:\\Windows\\System32\\regsvr32.exe"
          }
        }
      },
      {
        "_index": ".ds-my-data-stream-2099.12.07-000001",
        "_id": "xLkCaj4EujzdNSxfYLbO",
        "_source": {
          "@timestamp": "2099-12-07T11:07:10.000Z",
          "event": {
            "category": "process",
            "id": "GTSmSqgz0U",
            "sequence": 6,
            "type": "termination"
          },
          "process": {
            "pid": 2012,
            "name": "regsvr32.exe",
            "executable": "C:\\Windows\\System32\\regsvr32.exe"
          }
        }
      }
    ]
  }
}
```

Use the `size` parameter to get a smaller or larger set of hits:

```console
GET /my-data-stream/_eql/search
{
  "query": """
    process where process.name == "regsvr32.exe"
  """,
  "size": 50
}
```


## Search for a sequence of events [eql-search-sequence]

Use EQL’s [sequence syntax](/reference/query-languages/eql/eql-syntax.md#eql-sequences) to search for a series of ordered events. List the event items in ascending chronological order, with the most recent event listed last:

```console
GET /my-data-stream/_eql/search
{
  "query": """
    sequence
      [ process where process.name == "regsvr32.exe" ]
      [ file where stringContains(file.name, "scrobj.dll") ]
  """
}
```

The response’s `hits.sequences` property contains the 10 most recent matching sequences.

```console-result
{
  ...
  "hits": {
    "total": ...,
    "sequences": [
      {
        "events": [
          {
            "_index": ".ds-my-data-stream-2099.12.07-000001",
            "_id": "OQmfCaduce8zoHT93o4H",
            "_source": {
              "@timestamp": "2099-12-07T11:07:09.000Z",
              "event": {
                "category": "process",
                "id": "aR3NWVOs",
                "sequence": 4
              },
              "process": {
                "pid": 2012,
                "name": "regsvr32.exe",
                "command_line": "regsvr32.exe  /s /u /i:https://...RegSvr32.sct scrobj.dll",
                "executable": "C:\\Windows\\System32\\regsvr32.exe"
              }
            }
          },
          {
            "_index": ".ds-my-data-stream-2099.12.07-000001",
            "_id": "yDwnGIJouOYGBzP0ZE9n",
            "_source": {
              "@timestamp": "2099-12-07T11:07:10.000Z",
              "event": {
                "category": "file",
                "id": "tZ1NWVOs",
                "sequence": 5
              },
              "process": {
                "pid": 2012,
                "name": "regsvr32.exe",
                "executable": "C:\\Windows\\System32\\regsvr32.exe"
              },
              "file": {
                "path": "C:\\Windows\\System32\\scrobj.dll",
                "name": "scrobj.dll"
              }
            }
          }
        ]
      }
    ]
  }
}
```

Use [`with maxspan`](/reference/query-languages/eql/eql-syntax.md#eql-with-maxspan-keywords) to constrain matching sequences to a timespan:

```console
GET /my-data-stream/_eql/search
{
  "query": """
    sequence with maxspan=1h
      [ process where process.name == "regsvr32.exe" ]
      [ file where stringContains(file.name, "scrobj.dll") ]
  """
}
```

Use `!` to match [missing events](/reference/query-languages/eql/eql-syntax.md#eql-missing-events): events in a sequence that do not meet a condition within a given timespan:

```console
GET /my-data-stream/_eql/search
{
  "query": """
    sequence with maxspan=1d
      [ process where process.name == "cmd.exe" ]
      ![ process where stringContains(process.command_line, "ocx") ]
      [ file where stringContains(file.name, "scrobj.dll") ]
  """
}
```

Missing events are indicated in the response as `missing": true`:

```console-result
{
  ...
  "hits": {
    "total": ...,
    "sequences": [
      {
        "events": [
          {
            "_index": ".ds-my-data-stream-2023.07.04-000001",
            "_id": "AnpTIYkBrVQ2QEgsWg94",
            "_source": {
              "@timestamp": "2099-12-07T11:06:07.000Z",
              "event": {
                "category": "process",
                "id": "cMyt5SZ2",
                "sequence": 3
              },
              "process": {
                "pid": 2012,
                "name": "cmd.exe",
                "executable": "C:\\Windows\\System32\\cmd.exe"
              }
            }
          },
          {
            "_index": "",
            "_id": "",
            "_source": {},
            "missing": true
          },
          {
            "_index": ".ds-my-data-stream-2023.07.04-000001",
            "_id": "BHpTIYkBrVQ2QEgsWg94",
            "_source": {
              "@timestamp": "2099-12-07T11:07:10.000Z",
              "event": {
                "category": "file",
                "id": "tZ1NWVOs",
                "sequence": 5
              },
              "process": {
                "pid": 2012,
                "name": "regsvr32.exe",
                "executable": "C:\\Windows\\System32\\regsvr32.exe"
              },
              "file": {
                "path": "C:\\Windows\\System32\\scrobj.dll",
                "name": "scrobj.dll"
              }
            }
          }
        ]
      }
    ]
  }
}
```

Use the [`by` keyword](/reference/query-languages/eql/eql-syntax.md#eql-by-keyword) to match events that share the same field values:

```console
GET /my-data-stream/_eql/search
{
  "query": """
    sequence with maxspan=1h
      [ process where process.name == "regsvr32.exe" ] by process.pid
      [ file where stringContains(file.name, "scrobj.dll") ] by process.pid
  """
}
```

If a field value should be shared across all events, use the `sequence by` keyword. The following query is equivalent to the previous one.

```console
GET /my-data-stream/_eql/search
{
  "query": """
    sequence by process.pid with maxspan=1h
      [ process where process.name == "regsvr32.exe" ]
      [ file where stringContains(file.name, "scrobj.dll") ]
  """
}
```

The `hits.sequences.join_keys` property contains the shared field values.

```console-result
{
  ...
  "hits": ...,
    "sequences": [
      {
        "join_keys": [
          2012
        ],
        "events": ...
      }
    ]
  }
}
```

Use the [`until` keyword](/reference/query-languages/eql/eql-syntax.md#eql-until-keyword) to specify an expiration event for sequences. Matching sequences must end before this event.

```console
GET /my-data-stream/_eql/search
{
  "query": """
    sequence by process.pid with maxspan=1h
      [ process where process.name == "regsvr32.exe" ]
      [ file where stringContains(file.name, "scrobj.dll") ]
    until [ process where event.type == "termination" ]
  """
}
```


## Sample chronologically unordered events [eql-search-sample]

Use EQL’s [sample syntax](/reference/query-languages/eql/eql-syntax.md#eql-samples) to search for events that match one or more join keys and a set of filters. Samples are similar to sequences, but do not return events in chronological order. In fact, sample queries can run on data without a timestamp. Sample queries can be useful to find correlations in events that don’t always occur in the same sequence, or that occur across long time spans.

::::{dropdown} Click to show the sample data used in the examples below
```console
PUT /my-index-000001
{
    "mappings": {
        "properties": {
            "ip": {
                "type":"ip"
            },
            "version": {
                "type": "version"
            },
            "missing_keyword": {
                "type": "keyword"
            },
            "@timestamp": {
              "type": "date"
            },
            "type_test": {
                "type": "keyword"
            },
            "@timestamp_pretty": {
              "type": "date",
              "format": "dd-MM-yyyy"
            },
            "event_type": {
              "type": "keyword"
            },
            "event": {
              "properties": {
                "category": {
                  "type": "alias",
                  "path": "event_type"
                }
              }
            },
            "host": {
              "type": "keyword"
            },
            "os": {
              "type": "keyword"
            },
            "bool": {
              "type": "boolean"
            },
            "uptime" : {
              "type" : "long"
            },
            "port" : {
              "type" : "long"
            }
        }
    }
}

PUT /my-index-000002
{
    "mappings": {
        "properties": {
            "ip": {
                "type":"ip"
            },
            "@timestamp": {
              "type": "date"
            },
            "@timestamp_pretty": {
              "type": "date",
              "format": "yyyy-MM-dd"
            },
            "type_test": {
                "type": "keyword"
            },
            "event_type": {
              "type": "keyword"
            },
            "event": {
              "properties": {
                "category": {
                  "type": "alias",
                  "path": "event_type"
                }
              }
            },
            "host": {
              "type": "keyword"
            },
            "op_sys": {
              "type": "keyword"
            },
            "bool": {
              "type": "boolean"
            },
            "uptime" : {
              "type" : "long"
            },
            "port" : {
              "type" : "long"
            }
        }
    }
}

PUT /my-index-000003
{
    "mappings": {
        "properties": {
            "host_ip": {
                "type":"ip"
            },
            "@timestamp": {
              "type": "date"
            },
            "date": {
              "type": "date"
            },
            "event_type": {
              "type": "keyword"
            },
            "event": {
              "properties": {
                "category": {
                  "type": "alias",
                  "path": "event_type"
                }
              }
            },
            "missing_keyword": {
                "type": "keyword"
            },
            "host": {
              "type": "keyword"
            },
            "os": {
              "type": "keyword"
            },
            "bool": {
              "type": "boolean"
            },
            "uptime" : {
              "type" : "long"
            },
            "port" : {
              "type" : "long"
            }
        }
    }
}

POST /my-index-000001/_bulk?refresh
{"index":{"_id":1}}
{"@timestamp":"1234567891","@timestamp_pretty":"12-12-2022","missing_keyword":"test","type_test":"abc","ip":"10.0.0.1","event_type":"alert","host":"doom","uptime":0,"port":1234,"os":"win10","version":"1.0.0","id":11}
{"index":{"_id":2}}
{"@timestamp":"1234567892","@timestamp_pretty":"13-12-2022","event_type":"alert","type_test":"abc","host":"CS","uptime":5,"port":1,"os":"win10","version":"1.2.0","id":12}
{"index":{"_id":3}}
{"@timestamp":"1234567893","@timestamp_pretty":"12-12-2022","event_type":"alert","type_test":"abc","host":"farcry","uptime":1,"port":1234,"bool":false,"os":"win10","version":"2.0.0","id":13}
{"index":{"_id":4}}
{"@timestamp":"1234567894","@timestamp_pretty":"13-12-2022","event_type":"alert","type_test":"abc","host":"GTA","uptime":3,"port":12,"os":"slack","version":"10.0.0","id":14}
{"index":{"_id":5}}
{"@timestamp":"1234567895","@timestamp_pretty":"17-12-2022","event_type":"alert","host":"sniper 3d","uptime":6,"port":1234,"os":"fedora","version":"20.1.0","id":15}
{"index":{"_id":6}}
{"@timestamp":"1234568896","@timestamp_pretty":"17-12-2022","event_type":"alert","host":"doom","port":65123,"bool":true,"os":"redhat","version":"20.10.0","id":16}
{"index":{"_id":7}}
{"@timestamp":"1234567897","@timestamp_pretty":"17-12-2022","missing_keyword":"yyy","event_type":"failure","host":"doom","uptime":15,"port":1234,"bool":true,"os":"redhat","version":"20.2.0","id":17}
{"index":{"_id":8}}
{"@timestamp":"1234567898","@timestamp_pretty":"12-12-2022","missing_keyword":"test","event_type":"success","host":"doom","uptime":16,"port":512,"os":"win10","version":"1.2.3","id":18}
{"index":{"_id":9}}
{"@timestamp":"1234567899","@timestamp_pretty":"15-12-2022","missing_keyword":"test","event_type":"success","host":"GTA","port":12,"bool":true,"os":"win10","version":"1.2.3","id":19}
{"index":{"_id":10}}
{"@timestamp":"1234567893","missing_keyword":null,"ip":"10.0.0.5","event_type":"alert","host":"farcry","uptime":1,"port":1234,"bool":true,"os":"win10","version":"1.2.3","id":110}

POST /my-index-000002/_bulk?refresh
{"index":{"_id":1}}
{"@timestamp":"1234567991","type_test":"abc","ip":"10.0.0.1","event_type":"alert","host":"doom","uptime":0,"port":1234,"op_sys":"win10","id":21}
{"index":{"_id":2}}
{"@timestamp":"1234567992","type_test":"abc","event_type":"alert","host":"CS","uptime":5,"port":1,"op_sys":"win10","id":22}
{"index":{"_id":3}}
{"@timestamp":"1234567993","type_test":"abc","@timestamp_pretty":"2022-12-17","event_type":"alert","host":"farcry","uptime":1,"port":1234,"bool":false,"op_sys":"win10","id":23}
{"index":{"_id":4}}
{"@timestamp":"1234567994","event_type":"alert","host":"GTA","uptime":3,"port":12,"op_sys":"slack","id":24}
{"index":{"_id":5}}
{"@timestamp":"1234567995","event_type":"alert","host":"sniper 3d","uptime":6,"port":1234,"op_sys":"fedora","id":25}
{"index":{"_id":6}}
{"@timestamp":"1234568996","@timestamp_pretty":"2022-12-17","ip":"10.0.0.5","event_type":"alert","host":"doom","port":65123,"bool":true,"op_sys":"redhat","id":26}
{"index":{"_id":7}}
{"@timestamp":"1234567997","@timestamp_pretty":"2022-12-17","event_type":"failure","host":"doom","uptime":15,"port":1234,"bool":true,"op_sys":"redhat","id":27}
{"index":{"_id":8}}
{"@timestamp":"1234567998","ip":"10.0.0.1","event_type":"success","host":"doom","uptime":16,"port":512,"op_sys":"win10","id":28}
{"index":{"_id":9}}
{"@timestamp":"1234567999","ip":"10.0.0.1","event_type":"success","host":"GTA","port":12,"bool":false,"op_sys":"win10","id":29}

POST /my-index-000003/_bulk?refresh
{"index":{"_id":1}}
{"@timestamp":"1334567891","host_ip":"10.0.0.1","event_type":"alert","host":"doom","uptime":0,"port":12,"os":"win10","id":31}
{"index":{"_id":2}}
{"@timestamp":"1334567892","event_type":"alert","host":"CS","os":"win10","id":32}
{"index":{"_id":3}}
{"@timestamp":"1334567893","event_type":"alert","host":"farcry","bool":true,"os":"win10","id":33}
{"index":{"_id":4}}
{"@timestamp":"1334567894","event_type":"alert","host":"GTA","os":"slack","bool":true,"id":34}
{"index":{"_id":5}}
{"@timestamp":"1234567895","event_type":"alert","host":"sniper 3d","os":"fedora","id":35}
{"index":{"_id":6}}
{"@timestamp":"1234578896","host_ip":"10.0.0.1","event_type":"alert","host":"doom","bool":true,"os":"redhat","id":36}
{"index":{"_id":7}}
{"@timestamp":"1234567897","event_type":"failure","missing_keyword":"test","host":"doom","bool":true,"os":"redhat","id":37}
{"index":{"_id":8}}
{"@timestamp":"1234577898","event_type":"success","host":"doom","os":"win10","id":38,"date":"1671235200000"}
{"index":{"_id":9}}
{"@timestamp":"1234577899","host_ip":"10.0.0.5","event_type":"success","host":"GTA","bool":true,"os":"win10","id":39}
```

::::


A sample query specifies at least one join key, using the [`by` keyword](/reference/query-languages/eql/eql-syntax.md#eql-by-keyword), and up to five filters:

```console
GET /my-index*/_eql/search
{
  "query": """
    sample by host
      [any where uptime > 0]
      [any where port > 100]
      [any where bool == true]
  """
}
```

By default, the response’s `hits.sequences` property contains up to 10 samples. Each sample has a set of `join_keys` and an array with one matching event for each of the filters. Events are returned in the order of the filters they match:

```console-result
{
  ...
  "hits": {
    "total": {
      "value": 2,
      "relation": "eq"
    },
    "sequences": [
      {
        "join_keys": [
          "doom"                                      <1>
        ],
        "events": [
          {                                           <2>
            "_index": "my-index-000001",
            "_id": "7",
            "_source": {
              "@timestamp": "1234567897",
              "@timestamp_pretty": "17-12-2022",
              "missing_keyword": "yyy",
              "event_type": "failure",
              "host": "doom",
              "uptime": 15,
              "port": 1234,
              "bool": true,
              "os": "redhat",
              "version": "20.2.0",
              "id": 17
            }
          },
          {                                           <3>
            "_index": "my-index-000001",
            "_id": "1",
            "_source": {
              "@timestamp": "1234567891",
              "@timestamp_pretty": "12-12-2022",
              "missing_keyword": "test",
              "type_test": "abc",
              "ip": "10.0.0.1",
              "event_type": "alert",
              "host": "doom",
              "uptime": 0,
              "port": 1234,
              "os": "win10",
              "version": "1.0.0",
              "id": 11
            }
          },
          {                                           <4>
            "_index": "my-index-000001",
            "_id": "6",
            "_source": {
              "@timestamp": "1234568896",
              "@timestamp_pretty": "17-12-2022",
              "event_type": "alert",
              "host": "doom",
              "port": 65123,
              "bool": true,
              "os": "redhat",
              "version": "20.10.0",
              "id": 16
            }
          }
        ]
      },
      {
        "join_keys": [
          "farcry"                                    <5>
        ],
        "events": [
          {
            "_index": "my-index-000001",
            "_id": "3",
            "_source": {
              "@timestamp": "1234567893",
              "@timestamp_pretty": "12-12-2022",
              "event_type": "alert",
              "type_test": "abc",
              "host": "farcry",
              "uptime": 1,
              "port": 1234,
              "bool": false,
              "os": "win10",
              "version": "2.0.0",
              "id": 13
            }
          },
          {
            "_index": "my-index-000001",
            "_id": "10",
            "_source": {
              "@timestamp": "1234567893",
              "missing_keyword": null,
              "ip": "10.0.0.5",
              "event_type": "alert",
              "host": "farcry",
              "uptime": 1,
              "port": 1234,
              "bool": true,
              "os": "win10",
              "version": "1.2.3",
              "id": 110
            }
          },
          {
            "_index": "my-index-000003",
            "_id": "3",
            "_source": {
              "@timestamp": "1334567893",
              "event_type": "alert",
              "host": "farcry",
              "bool": true,
              "os": "win10",
              "id": 33
            }
          }
        ]
      }
    ]
  }
}
```

1. The events in the first sample have a value of `doom` for `host`.
2. This event matches the first filter.
3. This event matches the second filter.
4. This event matches the third filter.
5. The events in the second sample have a value of `farcry` for `host`.


You can specify multiple join keys:

```console
GET /my-index*/_eql/search
{
  "query": """
    sample by host
      [any where uptime > 0]   by os
      [any where port > 100]   by op_sys
      [any where bool == true] by os
  """
}
```

This query will return samples where each of the events shares the same value for `os` or `op_sys`, as well as for `host`. For example:

```console-result
{
  ...
  "hits": {
    "total": {
      "value": 2,
      "relation": "eq"
    },
    "sequences": [
      {
        "join_keys": [
          "doom",                                      <1>
          "redhat"
        ],
        "events": [
          {
            "_index": "my-index-000001",
            "_id": "7",
            "_source": {
              "@timestamp": "1234567897",
              "@timestamp_pretty": "17-12-2022",
              "missing_keyword": "yyy",
              "event_type": "failure",
              "host": "doom",
              "uptime": 15,
              "port": 1234,
              "bool": true,
              "os": "redhat",
              "version": "20.2.0",
              "id": 17
            }
          },
          {
            "_index": "my-index-000002",
            "_id": "6",
            "_source": {
              "@timestamp": "1234568996",
              "@timestamp_pretty": "2022-12-17",
              "ip": "10.0.0.5",
              "event_type": "alert",
              "host": "doom",
              "port": 65123,
              "bool": true,
              "op_sys": "redhat",
              "id": 26
            }
          },
          {
            "_index": "my-index-000001",
            "_id": "6",
            "_source": {
              "@timestamp": "1234568896",
              "@timestamp_pretty": "17-12-2022",
              "event_type": "alert",
              "host": "doom",
              "port": 65123,
              "bool": true,
              "os": "redhat",
              "version": "20.10.0",
              "id": 16
            }
          }
        ]
      },
      {
        "join_keys": [
          "farcry",
          "win10"
        ],
        "events": [
          {
            "_index": "my-index-000001",
            "_id": "3",
            "_source": {
              "@timestamp": "1234567893",
              "@timestamp_pretty": "12-12-2022",
              "event_type": "alert",
              "type_test": "abc",
              "host": "farcry",
              "uptime": 1,
              "port": 1234,
              "bool": false,
              "os": "win10",
              "version": "2.0.0",
              "id": 13
            }
          },
          {
            "_index": "my-index-000002",
            "_id": "3",
            "_source": {
              "@timestamp": "1234567993",
              "type_test": "abc",
              "@timestamp_pretty": "2022-12-17",
              "event_type": "alert",
              "host": "farcry",
              "uptime": 1,
              "port": 1234,
              "bool": false,
              "op_sys": "win10",
              "id": 23
            }
          },
          {
            "_index": "my-index-000001",
            "_id": "10",
            "_source": {
              "@timestamp": "1234567893",
              "missing_keyword": null,
              "ip": "10.0.0.5",
              "event_type": "alert",
              "host": "farcry",
              "uptime": 1,
              "port": 1234,
              "bool": true,
              "os": "win10",
              "version": "1.2.3",
              "id": 110
            }
          }
        ]
      }
    ]
  }
}
```

1. The events in this sample have a value of `doom` for `host` and a value of `redhat` for `os` or `op_sys`.


By default, the response of a sample query contains up to 10 samples, with one sample per unique set of join keys. Use the `size` parameter to get a smaller or larger set of samples. To retrieve more than one sample per set of join keys, use the `max_samples_per_key` parameter. Pipes are not supported for sample queries.

```console
GET /my-index*/_eql/search
{
  "max_samples_per_key": 2,     <1>
  "size": 20,                   <2>
  "query": """
    sample
      [any where uptime > 0]   by host,os
      [any where port > 100]   by host,op_sys
      [any where bool == true] by host,os
  """
}
```

1. Retrieve up to 2 samples per set of join keys.
2. Retrieve up to 20 samples in total.



## Retrieve selected fields [retrieve-selected-fields]

By default, each hit in the search response includes the document `_source`, which is the entire JSON object that was provided when indexing the document.

You can use the [`filter_path`](/reference/elasticsearch/rest-apis/common-options.md#common-options-response-filtering) query parameter to filter the API response. For example, the following search returns only the timestamp and PID from the `_source` of each matching event.

```console
GET /my-data-stream/_eql/search?filter_path=hits.events._source.@timestamp,hits.events._source.process.pid
{
  "query": """
    process where process.name == "regsvr32.exe"
  """
}
```

The API returns the following response.

```console-result
{
  "hits": {
    "events": [
      {
        "_source": {
          "@timestamp": "2099-12-07T11:07:09.000Z",
          "process": {
            "pid": 2012
          }
        }
      },
      {
        "_source": {
          "@timestamp": "2099-12-07T11:07:10.000Z",
          "process": {
            "pid": 2012
          }
        }
      }
    ]
  }
}
```

You can also use the `fields` parameter to retrieve and format specific fields in the response. This field is identical to the search API’s [`fields` parameter](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md).

Because it consults the index mappings, the `fields` parameter provides several advantages over referencing the `_source` directly. Specifically, the `fields` parameter:

* Returns each value in a standardized way that matches its mapping type
* Accepts [multi-fields](/reference/elasticsearch/mapping-reference/multi-fields.md) and [field aliases](/reference/elasticsearch/mapping-reference/field-alias.md)
* Formats dates and spatial data types
* Retrieves [runtime field values](docs-content://manage-data/data-store/mapping/retrieve-runtime-field.md)
* Returns fields calculated by a script at index time
* Returns fields from related indices using [lookup runtime fields](docs-content://manage-data/data-store/mapping/retrieve-runtime-field.md#lookup-runtime-fields)

The following search request uses the `fields` parameter to retrieve values for the `event.type` field, all fields starting with `process.`, and the `@timestamp` field. The request also uses the `filter_path` query parameter to exclude the `_source` of each hit.

```console
GET /my-data-stream/_eql/search?filter_path=-hits.events._source
{
  "query": """
    process where process.name == "regsvr32.exe"
  """,
  "fields": [
    "event.type",
    "process.*",                <1>
    {
      "field": "@timestamp",
      "format": "epoch_millis"  <2>
    }
  ]
}
```

1. Both full field names and wildcard patterns are accepted.
2. Use the `format` parameter to apply a custom format for the field’s values.


The response includes values as a flat list in the `fields` section for each hit.

```console-result
{
  ...
  "hits": {
    "total": ...,
    "events": [
      {
        "_index": ".ds-my-data-stream-2099.12.07-000001",
        "_id": "OQmfCaduce8zoHT93o4H",
        "fields": {
          "process.name": [
            "regsvr32.exe"
          ],
          "process.name.keyword": [
            "regsvr32.exe"
          ],
          "@timestamp": [
            "4100324829000"
          ],
          "process.command_line": [
            "regsvr32.exe  /s /u /i:https://...RegSvr32.sct scrobj.dll"
          ],
          "process.command_line.keyword": [
            "regsvr32.exe  /s /u /i:https://...RegSvr32.sct scrobj.dll"
          ],
          "process.executable.keyword": [
            "C:\\Windows\\System32\\regsvr32.exe"
          ],
          "process.pid": [
            2012
          ],
          "process.executable": [
            "C:\\Windows\\System32\\regsvr32.exe"
          ]
        }
      },
      ....
    ]
  }
}
```


## Use runtime fields [eql-use-runtime-fields]

Use the `runtime_mappings` parameter to extract and create [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md) during a search. Use the `fields` parameter to include runtime fields in the response.

The following search creates a `day_of_week` runtime field from the `@timestamp` and returns it in the response.

```console
GET /my-data-stream/_eql/search?filter_path=-hits.events._source
{
  "runtime_mappings": {
    "day_of_week": {
      "type": "keyword",
      "script": "emit(doc['@timestamp'].value.dayOfWeekEnum.toString())"
    }
  },
  "query": """
    process where process.name == "regsvr32.exe"
  """,
  "fields": [
    "@timestamp",
    "day_of_week"
  ]
}
```

The API returns:

```console-result
{
  ...
  "hits": {
    "total": ...,
    "events": [
      {
        "_index": ".ds-my-data-stream-2099.12.07-000001",
        "_id": "OQmfCaduce8zoHT93o4H",
        "fields": {
          "@timestamp": [
            "2099-12-07T11:07:09.000Z"
          ],
          "day_of_week": [
            "MONDAY"
          ]
        }
      },
      ....
    ]
  }
}
```


## Specify a timestamp or event category field [specify-a-timestamp-or-event-category-field]

The EQL search API uses the `@timestamp` and `event.category` fields from the [ECS](ecs://reference/index.md) by default. To specify different fields, use the `timestamp_field` and `event_category_field` parameters:

```console
GET /my-data-stream/_eql/search
{
  "timestamp_field": "file.accessed",
  "event_category_field": "file.type",
  "query": """
    file where (file.size > 1 and file.type == "file")
  """
}
```

The event category field must be mapped as a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family field type. The timestamp field should be mapped as a [`date`](/reference/elasticsearch/mapping-reference/date.md) field type. [`date_nanos`](/reference/elasticsearch/mapping-reference/date_nanos.md) timestamp fields are not supported. You cannot use a [`nested`](/reference/elasticsearch/mapping-reference/nested.md) field or the sub-fields of a `nested` field as the timestamp or event category field.


## Specify a sort tiebreaker [eql-search-specify-a-sort-tiebreaker]

By default, the EQL search API returns matching hits by timestamp. If two or more events share the same timestamp, {{es}} uses a tiebreaker field value to sort the events in ascending order. {{es}} orders events with no tiebreaker value after events with a value.

If you don’t specify a tiebreaker field or the events also share the same tiebreaker value, {{es}} considers the events concurrent and may not return them in a consistent sort order.

To specify a tiebreaker field, use the `tiebreaker_field` parameter. If you use the [ECS](ecs://reference/index.md), we recommend using `event.sequence` as the tiebreaker field.

```console
GET /my-data-stream/_eql/search
{
  "tiebreaker_field": "event.sequence",
  "query": """
    process where process.name == "cmd.exe" and stringContains(process.executable, "System32")
  """
}
```


## Filter using Query DSL [eql-search-filter-query-dsl]

The `filter` parameter uses [Query DSL](/reference/query-languages/querydsl.md) to limit the documents on which an EQL query runs.

```console
GET /my-data-stream/_eql/search
{
  "filter": {
    "range": {
      "@timestamp": {
        "gte": "now-1d/d",
        "lt": "now/d"
      }
    }
  },
  "query": """
    file where (file.type == "file" and file.name == "cmd.exe")
  """
}
```


## Run an async EQL search [eql-search-async]

By default, EQL search requests are synchronous and wait for complete results before returning a response. However, complete results can take longer for searches across large data sets or [frozen](docs-content://manage-data/lifecycle/data-tiers.md) data.

To avoid long waits, run an async EQL search. Set `wait_for_completion_timeout` to a duration you’d like to wait for synchronous results.

```console
GET /my-data-stream/_eql/search
{
  "wait_for_completion_timeout": "2s",
  "query": """
    process where process.name == "cmd.exe"
  """
}
```

If the request doesn’t finish within the timeout period, the search becomes async and returns a response that includes:

* A search ID
* An `is_partial` value of `true`, indicating the search results are incomplete
* An `is_running` value of `true`, indicating the search is ongoing

The async search continues to run in the background without blocking other requests.

```console-result
{
  "id": "FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=",
  "is_partial": true,
  "is_running": true,
  "took": 2000,
  "timed_out": false,
  "hits": ...
}
```

To check the progress of an async search, use the [get async EQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-search) with the search ID. Specify how long you’d like for complete results in the `wait_for_completion_timeout` parameter.

```console
GET /_eql/search/FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=?wait_for_completion_timeout=2s
```

If the response’s `is_running` value is `false`, the async search has finished. If the `is_partial` value is `false`, the returned search results are complete.

```console-result
{
  "id": "FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=",
  "is_partial": false,
  "is_running": false,
  "took": 2000,
  "timed_out": false,
  "hits": ...
}
```

Another more lightweight way to check the progress of an async search is to use the [get async EQL status API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-get-status) with the search ID.

```console
GET /_eql/search/status/FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=
```

```console-result
{
  "id": "FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=",
  "is_running": false,
  "is_partial": false,
  "expiration_time_in_millis": 1611690295000,
  "completion_status": 200
}
```


## Change the search retention period [eql-search-store-async-eql-search]

By default, the EQL search API stores async searches for five days. After this period, any searches and their results are deleted. Use the `keep_alive` parameter to change this retention period:

```console
GET /my-data-stream/_eql/search
{
  "keep_alive": "2d",
  "wait_for_completion_timeout": "2s",
  "query": """
    process where process.name == "cmd.exe"
  """
}
```

You can use the [get async EQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-search)'s `keep_alive` parameter to later change the retention period. The new retention period starts after the get request runs.

```console
GET /_eql/search/FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=?keep_alive=5d
```

Use the [delete async EQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-search) to manually delete an async EQL search before the `keep_alive` period ends. If the search is still ongoing, {{es}} cancels the search request.

```console
DELETE /_eql/search/FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=
```


## Store synchronous EQL searches [eql-search-store-sync-eql-search]

By default, the EQL search API only stores async searches. To save a synchronous search, set `keep_on_completion` to `true`:

```console
GET /my-data-stream/_eql/search
{
  "keep_on_completion": true,
  "wait_for_completion_timeout": "2s",
  "query": """
    process where process.name == "cmd.exe"
  """
}
```

The response includes a search ID. `is_partial` and `is_running` are `false`, indicating the EQL search was synchronous and returned complete results.

```console-result
{
  "id": "FjlmbndxNmJjU0RPdExBTGg0elNOOEEaQk9xSjJBQzBRMldZa1VVQ2pPa01YUToxMDY=",
  "is_partial": false,
  "is_running": false,
  "took": 52,
  "timed_out": false,
  "hits": ...
}
```

Use the [get async EQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-search) to get the same results later:

```console
GET /_eql/search/FjlmbndxNmJjU0RPdExBTGg0elNOOEEaQk9xSjJBQzBRMldZa1VVQ2pPa01YUToxMDY=
```

Saved synchronous searches are still subject to the `keep_alive` parameter’s retention period. When this period ends, the search and its results are deleted.

You can also check only the status of the saved synchronous search without results by using [get async EQL status API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-get-status).

You can also manually delete saved synchronous searches using the [delete async EQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-search).


## Run an EQL search across clusters [run-eql-search-across-clusters]

::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The EQL search API supports [cross-cluster search](docs-content://solutions/search/cross-cluster-search.md). However, the local and [remote clusters](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md) must use the same {{es}} version if they have versions prior to 7.17.7 (included) or prior to 8.5.1 (included).

The following [cluster update settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) request adds two remote clusters: `cluster_one` and `cluster_two`.

```console
PUT /_cluster/settings
{
  "persistent": {
    "cluster": {
      "remote": {
        "cluster_one": {
          "seeds": [
            "127.0.0.1:9300"
          ]
        },
        "cluster_two": {
          "seeds": [
            "127.0.0.1:9301"
          ]
        }
      }
    }
  }
}
```

To target a data stream or index on a remote cluster, use the `<cluster>:<target>` syntax.

```console
GET /cluster_one:my-data-stream,cluster_two:my-data-stream/_eql/search
{
  "query": """
    process where process.name == "regsvr32.exe"
  """
}
```


## EQL circuit breaker settings [eql-circuit-breaker]

The relevant circuit breaker settings can be found in the [Circuit Breakers page](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#circuit-breakers-page-eql).

