---
navigation_title: "CSV"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/csv-processor.html
---

# CSV processor [csv-processor]


Extracts fields from CSV line out of a single text field within a document. Any empty field in CSV will be skipped.

$$$csv-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to extract data from |
| `target_fields` | yes | - | The array of fields to assign extracted values to |
| `separator` | no | , | Separator used in CSV, has to be single character string |
| `quote` | no | " | Quote used in CSV, has to be single character string |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist, the processor quietly exits without modifying the document |
| `trim` | no | `false` | Trim whitespaces in unquoted fields |
| `empty_value` | no | - | Value used to fill empty fields, empty fields will be skipped if this is not provided.                                             Empty field is one with no value (2 consecutive separators) or empty quotes (`""`) |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "csv": {
    "field": "my_field",
    "target_fields": ["field1", "field2"]
  }
}
```

If the `trim` option is enabled then any whitespace in the beginning and in the end of each unquoted field will be trimmed. For example with configuration above, a value of `A, B` will result in field `field2` having value `{{nbsp}}B` (with space at the beginning). If `trim` is enabled `A, B` will result in field `field2` having value `B` (no whitespace). Quoted fields will be left untouched.

