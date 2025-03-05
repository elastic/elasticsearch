---
navigation_title: "Split"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/split-processor.html
---

# Split processor [split-processor]


Splits a field into an array using a separator character. Only works on string fields.

$$$split-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to split |
| `separator` | yes | - | A regex which matches the separator, eg `,` or `\s+` |
| `target_field` | no | `field` | The field to assign the split value to, by default `field` is updated in-place |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist, the processor quietly exits without modifying the document |
| `preserve_trailing` | no | `false` | Preserves empty trailing fields, if any. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "split": {
    "field": "my_field",
    "separator": "\\s+" <1>
  }
}
```

1. Treat all consecutive whitespace characters as a single separator


If the `preserve_trailing` option is enabled, any trailing empty fields in the input will be preserved. For example, in the configuration below, a value of `A,,B,,` in the `my_field` property will be split into an array of five elements `["A", "", "B", "", ""]` with two empty trailing fields. If the `preserve_trailing` property were not enabled, the two empty trailing fields would be discarded resulting in the three-element array `["A", "", "B"]`.

```js
{
  "split": {
    "field": "my_field",
    "separator": ",",
    "preserve_trailing": true
  }
}
```

