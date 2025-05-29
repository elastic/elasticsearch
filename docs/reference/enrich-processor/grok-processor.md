---
navigation_title: "Grok"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/grok-processor.html
---

# Grok processor [grok-processor]


Extracts structured fields out of a single text field within a document. You choose which field to extract matched fields from, as well as the grok pattern you expect will match. A grok pattern is like a regular expression that supports aliased expressions that can be reused.

This processor comes packaged with many [reusable patterns](https://github.com/elastic/elasticsearch/blob/master/libs/grok/src/main/resources/patterns).

If you need help building patterns to match your logs, you will find the [Grok Debugger](docs-content://explore-analyze/query-filter/tools/grok-debugger.md) tool quite useful! The [Grok Constructor](https://grokconstructor.appspot.com) is also a useful tool.

## Using the Grok Processor in a Pipeline [using-grok]

$$$grok-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to use for grok expression parsing |
| `patterns` | yes | - | An ordered list of grok expression to match and extract named captures with. Returns on the first expression in the list that matches. |
| `pattern_definitions` | no | - | A map of pattern-name and pattern tuples defining custom patterns to be used by the current processor. Patterns matching existing names will override the pre-existing definition. |
| `ecs_compatibility` | no | `disabled` | Must be `disabled` or `v1`. If `v1`, the processor uses patterns with [Elastic Common Schema (ECS)](ecs://reference/ecs-field-reference.md) field names. |
| `trace_match` | no | false | when true, `_ingest._grok_match_index` will be inserted into your matched document’s metadata with the index into the pattern found in `patterns` that matched. |
| `ignore_missing` | no | false | If `true` and `field` does not exist or is `null`, the processor quietly exits without modifying the document |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

Here is an example of using the provided patterns to extract out and name structured fields from a string field in a document.

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "description" : "...",
    "processors": [
      {
        "grok": {
          "field": "message",
          "patterns": ["%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes:int} %{NUMBER:duration:double}"]
        }
      }
    ]
  },
  "docs":[
    {
      "_source": {
        "message": "55.3.244.1 GET /index.html 15824 0.043"
      }
    }
  ]
}
```

This pipeline will insert these named captures as new fields within the document, like so:

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source" : {
          "duration" : 0.043,
          "request" : "/index.html",
          "method" : "GET",
          "bytes" : 15824,
          "client" : "55.3.244.1",
          "message" : "55.3.244.1 GET /index.html 15824 0.043"
        },
        "_ingest": {
          "timestamp": "2016-11-08T19:43:03.850+0000"
        }
      }
    }
  ]
}
```
% TESTRESPONSE[s/2016-11-08T19:43:03.850+0000/$body.docs.0.doc._ingest.timestamp/]


## Custom Patterns [custom-patterns]

The Grok processor comes pre-packaged with a base set of patterns. These patterns may not always have what you are looking for. Patterns have a very basic format. Each entry has a name and the pattern itself.

You can add your own patterns to a processor definition under the `pattern_definitions` option. Here is an example of a pipeline specifying custom pattern definitions:

```js
{
  "description" : "...",
  "processors": [
    {
      "grok": {
        "field": "message",
        "patterns": ["my %{FAVORITE_DOG:dog} is colored %{RGB:color}"],
        "pattern_definitions" : {
          "FAVORITE_DOG" : "beagle",
          "RGB" : "RED|GREEN|BLUE"
        }
      }
    }
  ]
}
```
% NOTCONSOLE


## Providing Multiple Match Patterns [trace-match]

Sometimes one pattern is not enough to capture the potential structure of a field. Let’s assume we want to match all messages that contain your favorite pet breeds of either cats or dogs. One way to accomplish this is to provide two distinct patterns that can be matched, instead of one really complicated expression capturing the same `or` behavior.

Here is an example of such a configuration executed against the simulate API:

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
  "description" : "parse multiple patterns",
  "processors": [
    {
      "grok": {
        "field": "message",
        "patterns": ["%{FAVORITE_DOG:pet}", "%{FAVORITE_CAT:pet}"],
        "pattern_definitions" : {
          "FAVORITE_DOG" : "beagle",
          "FAVORITE_CAT" : "burmese"
        }
      }
    }
  ]
},
"docs":[
  {
    "_source": {
      "message": "I love burmese cats!"
    }
  }
  ]
}
```

response:

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source": {
          "message": "I love burmese cats!",
          "pet": "burmese"
        },
        "_ingest": {
          "timestamp": "2016-11-08T19:43:03.850+0000"
        }
      }
    }
  ]
}
```
% TESTRESPONSE[s/2016-11-08T19:43:03.850+0000/$body.docs.0.doc._ingest.timestamp/]

Both patterns will set the field `pet` with the appropriate match, but what if we want to trace which of our patterns matched and populated our fields? We can do this with the `trace_match` parameter. Here is the output of that same pipeline, but with `"trace_match": true` configured:

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source": {
          "message": "I love burmese cats!",
          "pet": "burmese"
        },
        "_ingest": {
          "_grok_match_index": "1",
          "timestamp": "2016-11-08T19:43:03.850+0000"
        }
      }
    }
  ]
}
```
% TESTRESPONSE[s/2016-11-08T19:43:03.850+0000/$body.docs.0.doc._ingest.timestamp/]

In the above response, you can see that the index of the pattern that matched was `"1"`. This is to say that it was the second (index starts at zero) pattern in `patterns` to match.

This trace metadata enables debugging which of the patterns matched. This information is stored in the ingest metadata and will not be indexed.


## Retrieving patterns from REST endpoint [grok-processor-rest-get]

The Grok processor comes packaged with its own REST endpoint for retrieving the patterns included with the processor.

```console
GET _ingest/processor/grok
```

The above request will return a response body containing a key-value representation of the built-in patterns dictionary.

```js
{
  "patterns" : {
    "BACULA_CAPACITY" : "%{INT}{1,3}(,%{INT}{3})*",
    "PATH" : "(?:%{UNIXPATH}|%{WINPATH})",
    ...
}
```
% NOTCONSOLE

By default, the API returns a list of legacy Grok patterns. These legacy patterns predate the [Elastic Common Schema (ECS)](ecs://reference/ecs-field-reference.md) and don’t use ECS field names. To return patterns that extract ECS field names, specify `v1` in the optional `ecs_compatibility` query parameter.

```console
GET _ingest/processor/grok?ecs_compatibility=v1
```

By default, the API returns patterns in the order they are read from disk. This sort order preserves groupings of related patterns. For example, all patterns related to parsing Linux syslog lines stay grouped together.

You can use the optional boolean `s` query parameter to sort returned patterns by key name instead.

```console
GET _ingest/processor/grok?s
```

The API returns the following response.

```js
{
  "patterns" : {
    "BACULA_CAPACITY" : "%{INT}{1,3}(,%{INT}{3})*",
    "BACULA_DEVICE" : "%{USER}",
    "BACULA_DEVICEPATH" : "%{UNIXPATH}",
    ...
}
```
% NOTCONSOLE

This can be useful to reference as the built-in patterns change across versions.


## Grok watchdog [grok-watchdog]

Grok expressions that take too long to execute are interrupted and the grok processor then fails with an exception. The grok processor has a watchdog thread that determines when evaluation of a grok expression takes too long and is controlled by the following settings:

$$$grok-watchdog-options$$$

| Name | Default | Description |
| --- | --- | --- |
| `ingest.grok.watchdog.interval` | 1s | How often to check whether there are grok evaluations that take longer than the maximum allowed execution time. |
| `ingest.grok.watchdog.max_execution_time` | 1s | The maximum allowed execution of a grok expression evaluation. |


## Grok debugging [grok-debugging]

It is advised to use the [Grok Debugger](docs-content://explore-analyze/query-filter/tools/grok-debugger.md) to debug grok patterns. From there you can test one or more patterns in the UI against sample data. Under the covers it uses the same engine as ingest node processor.

Additionally, it is recommended to enable debug logging for Grok so that any additional messages may also be seen in the Elasticsearch server log.

```js
PUT _cluster/settings
{
  "persistent": {
    "logger.org.elasticsearch.ingest.common.GrokProcessor": "debug"
  }
}
```
% NOTCONSOLE


