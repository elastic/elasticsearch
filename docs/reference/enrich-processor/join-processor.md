---
navigation_title: "Join"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/join-processor.html
---

# Join processor [join-processor]


Joins each element of an array into a single string using a separator character between each element. Throws an error when the field is not an array.

$$$join-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | Field containing array values to join |
| `separator` | yes | - | The separator character |
| `target_field` | no | `field` | The field to assign the joined value to, by default `field` is updated in-place |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "join": {
    "field": "joined_array_field",
    "separator": "-"
  }
}
```

