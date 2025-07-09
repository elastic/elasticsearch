---
navigation_title: "Sort"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sort-processor.html
---

# Sort processor [sort-processor]


Sorts the elements of an array ascending or descending. Homogeneous arrays of numbers will be sorted numerically, while arrays of strings or heterogeneous arrays of strings + numbers will be sorted lexicographically. Throws an error when the field is not an array.

$$$sort-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to be sorted |
| `order` | no | `"asc"` | The sort order to use. Accepts `"asc"` or `"desc"`. |
| `target_field` | no | `field` | The field to assign the sorted value to, by default `field` is updated in-place |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "sort": {
    "field": "array_field_to_sort",
    "order": "desc"
  }
}
```

