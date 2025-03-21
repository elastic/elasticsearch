---
navigation_title: "Trim"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/trim-processor.html
---

# Trim processor [trim-processor]


Trims whitespace from field. If the field is an array of strings, all members of the array will be trimmed.

::::{note}
This only works on leading and trailing whitespace.
::::


$$$trim-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The string-valued field to trim whitespace from |
| `target_field` | no | `field` | The field to assign the trimmed value to, by default `field` is updated in-place |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist, the processor quietly exits without modifying the document |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "trim": {
    "field": "foo"
  }
}
```

