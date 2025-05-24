---
navigation_title: "Remove"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/remove-processor.html
---

# Remove processor [remove-processor]


Removes existing fields. If one field doesn’t exist, an exception will be thrown.

$$$remove-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | Fields to be removed. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist or is `null`, the processor quietly exits without modifying the document |
| `keep` | no | - | Fields to be kept. When set, all fields other than those specified are removed. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

Here is an example to remove a single field:

```js
{
  "remove": {
    "field": "user_agent"
  }
}
```
% NOTCONSOLE

To remove multiple fields, you can use the following query:

```js
{
  "remove": {
    "field": ["user_agent", "url"]
  }
}
```
% NOTCONSOLE

You can also choose to remove all fields other than a specified list:

```js
{
  "remove": {
    "keep": ["url"]
  }
}
```
% NOTCONSOLE

