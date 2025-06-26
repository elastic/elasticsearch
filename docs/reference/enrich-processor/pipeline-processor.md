---
navigation_title: "Pipeline"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/pipeline-processor.html
---

# Pipeline processor [pipeline-processor]


Executes another pipeline.

$$$pipeline-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `name` | yes | - | The name of the pipeline to execute. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `ignore_missing_pipeline` | no | false | Whether to ignore missing pipelines instead of failing. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "pipeline": {
    "name": "inner-pipeline"
  }
}
```

The name of the current pipeline can be accessed from the `_ingest.pipeline` ingest metadata key.

An example of using this processor for nesting pipelines would be:

Define an inner pipeline:

```console
PUT _ingest/pipeline/pipelineA
{
  "description" : "inner pipeline",
  "processors" : [
    {
      "set" : {
        "field": "inner_pipeline_set",
        "value": "inner"
      }
    }
  ]
}
```

Define another pipeline that uses the previously defined inner pipeline:

```console
PUT _ingest/pipeline/pipelineB
{
  "description" : "outer pipeline",
  "processors" : [
    {
      "pipeline" : {
        "name": "pipelineA"
      }
    },
    {
      "set" : {
        "field": "outer_pipeline_set",
        "value": "outer"
      }
    }
  ]
}
```

Now indexing a document while applying the outer pipeline will see the inner pipeline executed from the outer pipeline:

```console
PUT /my-index-000001/_doc/1?pipeline=pipelineB
{
  "field": "value"
}
```

Response from the index request:

```console-result
{
  "_index": "my-index-000001",
  "_id": "1",
  "_version": 1,
  "result": "created",
  "_shards": {
    "total": 2,
    "successful": 1,
    "failed": 0
  },
  "_seq_no": 66,
  "_primary_term": 1
}
```

Indexed document:

```js
{
  "field": "value",
  "inner_pipeline_set": "inner",
  "outer_pipeline_set": "outer"
}
```

