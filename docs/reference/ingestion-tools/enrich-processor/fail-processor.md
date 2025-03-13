---
navigation_title: "Fail"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/fail-processor.html
---

# Fail processor [fail-processor]


Raises an exception. This is useful for when you expect a pipeline to fail and want to relay a specific message to the requester.

$$$fail-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `message` | yes | - | The error message thrown by the processor. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "fail": {
    "if" : "ctx.tags.contains('production') != true",
    "message": "The production tag is not present, found tags: {{{tags}}}"
  }
}
```
% NOTCONSOLE

