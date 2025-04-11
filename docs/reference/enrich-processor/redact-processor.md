---
navigation_title: "Redact"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/redact-processor.html
---

# Redact processor [redact-processor]


The Redact processor uses the Grok rules engine to obscure text in the input document matching the given Grok patterns. The processor can be used to obscure Personal Identifying Information (PII) by configuring it to detect known patterns such as email or IP addresses. Text that matches a Grok pattern is replaced with a configurable string such as `<EMAIL>` where an email address is matched or simply replace all matches with the text `<REDACTED>` if preferred.

{{es}} comes packaged with a number of useful predefined [patterns](https://github.com/elastic/elasticsearch/blob/master/libs/grok/src/main/resources/patterns/ecs-v1) that can be conveniently referenced by the Redact processor. If one of those does not suit your needs, create a new pattern with a custom pattern definition. The Redact processor replaces every occurrence of a match. If there are multiple matches all will be replaced with the pattern name.

The Redact processor is compatible with [Elastic Common Schema (ECS)](ecs://reference/ecs-field-reference.md) patterns. Legacy Grok patterns are not supported.

## Using the Redact processor in a pipeline [using-redact]

$$$redact-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to be redacted |
| `patterns` | yes | - | A list of grok expressions to match and redact named captures with |
| `pattern_definitions` | no | - | A map of pattern-name and pattern tuples defining custom patterns to be used by the processor. Patterns matching existing names will override the pre-existing definition |
| `prefix` | no | < | Start a redacted section with this token |
| `suffix` | no | > | End a redacted section with this token |
| `ignore_missing` | no | `true` | If `true` and `field` does not exist or is `null`, the processor quietly exits without modifying the document |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |
| `skip_if_unlicensed` | no | `false` | If `true` and the current license does not support running redact processors, then the processor quietly exits without modifying the document |
| `trace_redact` | no | `false` | If `true` then ingest metadata `_ingest._redact._is_redacted` is set to `true` if the document has been redacted |

In this example the predefined `IP` Grok pattern is used to match and redact an IP addresses from the `message` text field. The pipeline is tested using the Simulate API.

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "description" : "Hide my IP",
    "processors": [
      {
        "redact": {
          "field": "message",
          "patterns": ["%{IP:client}"]
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

The document in the response still contains the `message` field but now the IP address `55.3.244.1` is replaced by the text `<client>`.

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source": {
          "message": "<client> GET /index.html 15824 0.043"
        },
        "_ingest": {
          "timestamp": "2023-02-01T16:08:39.419056008Z"
        }
      }
    }
  ]
}
```

The IP address is replaced with the word `client` because that is what is specified in the Grok pattern `%{IP:client}`. The `<` and `>` tokens which surround the pattern name are configurable using the `prefix` and `suffix` options.

The next example defines multiple patterns both of which are replaced with the word `REDACTED` and the prefix and suffix tokens are set to `*`

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "description": "Hide my IP",
    "processors": [
      {
        "redact": {
          "field": "message",
          "patterns": [
            "%{IP:REDACTED}",
            "%{EMAILADDRESS:REDACTED}"
          ],
          "prefix": "*",
          "suffix": "*"
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "message": "55.3.244.1 GET /index.html 15824 0.043 test@elastic.co"
      }
    }
  ]
}
```

In the response both the IP `55.3.244.1` and email address `test@elastic.co` have been replaced by `*REDACTED*`.

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source": {
          "message": "*REDACTED* GET /index.html 15824 0.043 *REDACTED*"
        },
        "_ingest": {
          "timestamp": "2023-02-01T16:53:14.560005377Z"
        }
      }
    }
  ]
}
```


## Custom patterns [redact-custom-patterns]

If one of the existing Grok [patterns](https://github.com/elastic/elasticsearch/blob/master/libs/grok/src/main/resources/patterns/ecs-v1) does not fit your requirements custom patterns can be added with the `pattern_definitions` option. New patterns definitions are composed of a pattern name and the pattern itself. The pattern may be a regular expression or reference existing Grok patterns.

This example defines the custom pattern `GITHUB_NAME` to match GitHub usernames. The pattern definition uses the existing `USERNAME` Grok [pattern](https://github.com/elastic/elasticsearch/blob/master/libs/grok/src/main/resources/patterns/ecs-v1/grok-patterns) prefixed by the literal `@`.

::::{note}
The [Grok Debugger](docs-content://explore-analyze/query-filter/tools/grok-debugger.md) is a really useful tool for building custom patterns.
::::


```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "redact": {
          "field": "message",
          "patterns": [
            "%{GITHUB_NAME:GITHUB_NAME}"
          ],
          "pattern_definitions": {
            "GITHUB_NAME": "@%{USERNAME}"
          }
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "message": "@elastic-data-management the PR is ready for review"
      }
    }
  ]
}
```

The username is redacted in the response.

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source": {
          "message": "<GITHUB_NAME> the PR is ready for review"
        },
        "_ingest": {
          "timestamp": "2023-02-01T16:53:14.560005377Z"
        }
      }
    }
  ]
}
```


## Grok watchdog [grok-watchdog-redact]

The watchdog interrupts expressions that take too long to execute. When interrupted, the Redact processor fails with an error. The same [settings](/reference/enrich-processor/grok-processor.md#grok-watchdog-options) that control the Grok Watchdog timeout also apply to the Redact processor.


## Licensing [redact-licensing]

The `redact` processor is a commercial feature that requires an appropriate license. For more information, refer to [https://www.elastic.co/subscriptions](https://www.elastic.co/subscriptions).

The `skip_if_unlicensed` option can be set on a redact processor to control behavior when the cluster’s license is not sufficient to run such a processor. `skip_if_unlicensed` defaults to `false`, and the redact processor will throw an exception if the cluster’s license is not sufficient. If you set the `skip_if_unlicensed` option to `true`, however, then the redact processor not throw an exception (it will do nothing at all) in the case of an insufficient license.


