---
navigation_title: "Script"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/script-processor.html
---

# Script processor [script-processor]


Runs an inline or stored [script](docs-content://explore-analyze/scripting.md) on incoming documents. The script runs in the [`ingest`](/reference/scripting-languages/painless/painless-ingest-processor-context.md) context.

The script processor uses the [script cache](docs-content://explore-analyze/scripting/scripts-search-speed.md) to avoid recompiling the script for each incoming document. To improve performance, ensure the script cache is properly sized before using a script processor in production.

$$$script-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `lang` | no | "painless" | [Script language](docs-content://explore-analyze/scripting.md#scripting-available-languages). |
| `id` | no | - | ID of a [stored script](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-put-script).                                         If no `source` is specified, this parameter is required. |
| `source` | no | - | Inline script.                                         If no `id` is specified, this parameter is required. |
| `params` | no | - | Object containing parameters for the script. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |


## Access source fields [script-processor-access-source-fields]

The script processor parses each incoming document’s JSON source fields into a set of maps, lists, and primitives. To access these fields with a Painless script, use the [map access operator](/reference/scripting-languages/painless/painless-operators-reference.md#map-access-operator): `ctx['my-field']`. You can also use the shorthand `ctx.<my-field>` syntax.

::::{note}
The script processor does not support the `ctx['_source']['my-field']` or `ctx._source.<my-field>` syntaxes.
::::


The following processor uses a Painless script to extract the `tags` field from the `env` source field.

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "script": {
          "description": "Extract 'tags' from 'env' field",
          "lang": "painless",
          "source": """
            String[] envSplit = ctx['env'].splitOnToken(params['delimiter']);
            ArrayList tags = new ArrayList();
            tags.add(envSplit[params['position']].trim());
            ctx['tags'] = tags;
          """,
          "params": {
            "delimiter": "-",
            "position": 1
          }
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "env": "es01-prod"
      }
    }
  ]
}
```

The processor produces:

```console-result
{
  "docs": [
    {
      "doc": {
        ...
        "_source": {
          "env": "es01-prod",
          "tags": [
            "prod"
          ]
        }
      }
    }
  ]
}
```
% TESTRESPONSE[s/\.\.\./"_index":"_index","_id":"_id","_version":"-3","_ingest":{"timestamp":$body.docs.0.doc._ingest.timestamp},/]


## Access metadata fields [script-processor-access-metadata-fields]

You can also use a script processor to access metadata fields. The following processor uses a Painless script to set an incoming document’s `_index`.

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "script": {
          "description": "Set index based on `lang` field and `dataset` param",
          "lang": "painless",
          "source": """
            ctx['_index'] = ctx['lang'] + '-' + params['dataset'];
          """,
          "params": {
            "dataset": "catalog"
          }
        }
      }
    ]
  },
  "docs": [
    {
      "_index": "generic-index",
      "_source": {
        "lang": "fr"
      }
    }
  ]
}
```

The processor changes the document’s `_index` to `fr-catalog` from `generic-index`.

```console-result
{
  "docs": [
    {
      "doc": {
        ...
        "_index": "fr-catalog",
        "_source": {
          "lang": "fr"
        }
      }
    }
  ]
}
```
% TESTRESPONSE[s/\.\.\./"_id":"_id","_version":"-3","_ingest":{"timestamp":$body.docs.0.doc._ingest.timestamp},/]

