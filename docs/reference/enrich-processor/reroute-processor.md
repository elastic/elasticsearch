---
navigation_title: "Reroute"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/reroute-processor.html
---

# Reroute processor [reroute-processor]


The `reroute` processor allows to route a document to another target index or data stream. It has two main modes:

When setting the `destination` option, the target is explicitly specified and the `dataset` and `namespace` options can’t be set.

When the `destination` option is not set, this processor is in a data stream mode. Note that in this mode, the `reroute` processor can only be used on data streams that follow the [data stream naming scheme](docs-content://reference/fleet/data-streams.md#data-streams-naming-scheme). Trying to use this processor on a data stream with a non-compliant name will raise an exception.

The name of a data stream consists of three parts: `<type>-<dataset>-<namespace>`. See the [data stream naming scheme](docs-content://reference/fleet/data-streams.md#data-streams-naming-scheme) documentation for more details.

This processor can use both static values or reference fields from the document to determine the `dataset` and `namespace` components of the new target. See [Table 40](#reroute-options) for more details.

::::{note}
It’s not possible to change the `type` of the data stream with the `reroute` processor.
::::


After a `reroute` processor has been executed, all the other processors of the current pipeline are skipped, including the final pipeline. If the current pipeline is executed in the context of a [Pipeline](/reference/enrich-processor/pipeline-processor.md), the calling pipeline will be skipped, too. This means that at most one `reroute` processor is ever executed within a pipeline, allowing to define mutually exclusive routing conditions, similar to a if, else-if, else-if, … condition.

The reroute processor ensures that the `data_stream.<type|dataset|namespace>` fields are set according to the new target. If the document contains a `event.dataset` value, it will be updated to reflect the same value as `data_stream.dataset`.

Note that the client needs to have permissions to the final target. Otherwise, the document will be rejected with a security exception which looks like this:

```js
{"type":"security_exception","reason":"action [indices:admin/auto_create] is unauthorized for API key id [8-dt9H8BqGblnY2uSI--] of user [elastic/fleet-server] on indices [logs-foo-default], this action is granted by the index privileges [auto_configure,create_index,manage,all]"}
```

$$$reroute-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `destination` | no | - | A static value for the target. Can’t be set when the `dataset` or `namespace` option is set. |
| `dataset` | no | `{{data_stream.dataset}}` | Field references or a static value for the dataset part of the data stream name. In addition to the criteria for [index names](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create), cannot contain `-` and must be no longer than 100 characters. Example values are `nginx.access` and `nginx.error`.<br><br>Supports field references with a mustache-like syntax (denoted as `{{double}}` or `{{{triple}}}` curly braces). When resolving field references, the processor replaces invalid characters with `_`. Uses the `<dataset>` part of the index name as a fallback if all field references resolve to a `null`, missing, or non-string value.<br> |
| `namespace` | no | `{{data_stream.namespace}}` | Field references or a static value for the namespace part of the data stream name. See the criteria for [index names](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) for allowed characters. Must be no longer than 100 characters.<br><br>Supports field references with a mustache-like syntax (denoted as `{{double}}` or `{{{triple}}}` curly braces). When resolving field references, the processor replaces invalid characters with `_`. Uses the `<namespace>` part of the index name as a fallback if all field references resolve to a `null`, missing, or non-string value.<br> |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

The `if` option can be used to define the condition in which the document should be rerouted to a new target.

```js
{
  "reroute": {
    "tag": "nginx",
    "if" : "ctx?.log?.file?.path?.contains('nginx')",
    "dataset": "nginx"
  }
}
```

The dataset and namespace options can contain either a single value or a list of values that are used as a fallback. If a field reference evaluates to `null`, is not present in the document, the next value or field reference is used. If a field reference evaluates to a non-`String` value, the processor fails.

In the following example, the processor would first try to resolve the value for the `service.name` field to determine the value for `dataset`. If that field resolves to `null`, is missing, or is a non-string value, it would try the next element in the list. In this case, this is the static value `"generic`". The `namespace` option is configured with just a single static value.

```js
{
  "reroute": {
    "dataset": [
        "{{service.name}}",
        "generic"
    ],
    "namespace": "default"
  }
}
```

