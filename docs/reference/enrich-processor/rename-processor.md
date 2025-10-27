---
navigation_title: "Rename"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rename-processor.html
---

# Rename processor [rename-processor]


Renames an existing field. If the field doesn’t exist or the new name is already used, an exception will be thrown.

$$$rename-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to be renamed. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `target_field` | yes | - | The new name of the field. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist, the processor quietly exits without modifying the document. |
| `override` | no | `false` | If `true`, the processor will update pre-existing non-null-valued fields. When set to `false`, such fields will not be touched. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "rename": {
    "field": "provider",
    "target_field": "cloud.provider"
  }
}
```
% NOTCONSOLE

