---
navigation_title: "Reroute"
applies_to:
  stack:
  serverless:
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/reroute-processor.html
---

# Reroute processor [reroute-processor]

The `reroute` processor routes a document to a different target index or data stream during ingestion. This allows you to dynamically direct documents to appropriate destinations based on their content or metadata.

## Operating modes [reroute-operating-modes]

The reroute processor has two main modes of operation.

### Explicit destination mode [reroute-explicit-destination]

When you set the `destination` option, the processor routes the document to an explicitly specified target. In this mode, you cannot use the `dataset` and `namespace` options.

### Data stream mode [reroute-data-stream-mode]

When the `destination` option is not set, the processor operates in data stream mode. This mode requires that your data stream follows the [data stream naming scheme](docs-content://reference/fleet/data-streams.md#data-streams-naming-scheme).

Data stream names consist of three parts: `<type>-<dataset>-<namespace>`. See the [data stream naming scheme](docs-content://reference/fleet/data-streams.md#data-streams-naming-scheme) documentation for details.

In data stream mode, you can use static values or field references to determine the `dataset` and `namespace` components of the new target. The processor will automatically construct the full data stream name.

::::{note}
You cannot change the `type` component of a data stream with the `reroute` processor. Attempting to use this processor on a data stream with a non-compliant name will raise an exception.
::::

## Pipeline execution behavior [reroute-pipeline-execution]

When a `reroute` processor executes, it immediately stops processing the current pipeline:

- All subsequent processors in the current pipeline are skipped, including the final pipeline
- If the current pipeline is called from a [Pipeline processor](/reference/enrich-processor/pipeline-processor.md), the calling pipeline is also skipped
- Only one `reroute` processor can execute per document, allowing you to define mutually exclusive routing conditions similar to if-else logic

## Document processing after rerouting [reroute-document-processing]

After a document is rerouted, it is processed through the ingest pipeline associated with the new target destination (or dataset and namespace). This enables the document to be transformed according to the rules specific to its new destination.

If the new ingest pipeline also contains a `reroute` processor, the document can be rerouted again to yet another target. This chaining continues as long as each successive pipeline contains a reroute processor that matches the document.

The system detects routing cycles automatically. If a document would be routed back to a target it has already been processed through, an exception is thrown and the document fails to be ingested.

## Field updates [reroute-field-updates]

The reroute processor automatically updates document fields to reflect the new target:

- Sets `data_stream.type`, `data_stream.dataset`, and `data_stream.namespace` according to the new target
- If the document contains an `event.dataset` field, updates it to match `data_stream.dataset`

## Permissions [reroute-permissions]

The client ingesting the document must have permissions to write to the final target destination. Without proper permissions, the document will be rejected with a security exception:

```js
{"type":"security_exception","reason":"action [indices:admin/auto_create] is unauthorized for API key id [8-dt9H8BqGblnY2uSI--] of user [elastic/fleet-server] on indices [logs-foo-default], this action is granted by the index privileges [auto_configure,create_index,manage,all]"}
```
% NOTCONSOLE

## Configuration options [reroute-options]

$$$reroute-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `destination` | no | - | A static value for the target. Cannot be set when the `dataset` or `namespace` option is set. |
| `dataset` | no | `{{data_stream.dataset}}` | Field references or a static value for the dataset part of the data stream name. In addition to the criteria for [index names](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create), cannot contain `-` and must be no longer than 100 characters. Example values are `nginx.access` and `nginx.error`.<br><br>Supports field references with a mustache-like syntax (denoted as `{{double}}` or `{{{triple}}}` curly braces). When resolving field references, the processor replaces invalid characters with `_`. Uses the `<dataset>` part of the index name as a fallback if all field references resolve to a `null`, missing, or non-string value.<br> |
| `namespace` | no | `{{data_stream.namespace}}` | Field references or a static value for the namespace part of the data stream name. See the criteria for [index names](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) for allowed characters. Must be no longer than 100 characters.<br><br>Supports field references with a mustache-like syntax (denoted as `{{double}}` or `{{{triple}}}` curly braces). When resolving field references, the processor replaces invalid characters with `_`. Uses the `<namespace>` part of the index name as a fallback if all field references resolve to a `null`, missing, or non-string value.<br> |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

## Examples [reroute-examples]

### Conditional routing [reroute-conditional-routing]

Use the `if` option to define conditions for when a document should be rerouted:

```js
{
  "reroute": {
    "tag": "nginx",
    "if" : "ctx?.log?.file?.path?.contains('nginx')",
    "dataset": "nginx"
  }
}
```
% NOTCONSOLE

### Fallback values [reroute-fallback-values]

The `dataset` and `namespace` options accept either a single value or a list of values used as fallbacks. If a field reference evaluates to `null` or is missing, the processor tries the next value in the list.

In this example, the processor first attempts to resolve `service.name` for the dataset. If that field is `null`, missing, or non-string, it falls back to the static value `"generic"`:

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
% NOTCONSOLE

If a field reference evaluates to a non-string value, the processor fails.