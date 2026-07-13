---
navigation_title: "Enrich"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/enrich-processor.html
---

# Enrich processor [enrich-processor]


The `enrich` processor can enrich documents with data from another index. See [enrich data](docs-content://manage-data/ingest/transform-enrich/data-enrichment.md) section for more information about how to set this up.

$$$enrich-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `policy_name` | yes | - | The name of the enrich policy to use. |
| `field` | yes | - | The field in the input document that matches the policies match_field used to retrieve the enrichment data. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `target_field` | yes | - | Field added to incoming documents to contain enrich data. This field contains both the `match_field` and `enrich_fields` specified in the [enrich policy](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-enrich-put-policy). Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `ignore_missing` | no | false | If `true` and `field` does not exist, the processor quietly exits without modifying the document |
| `override` | no | true | If processor will update fields with pre-existing non-null-valued field. When set to `false`, such fields will not be touched. |
| `max_matches` | no | 1 | The maximum number of matched documents to include under the configured target field. The `target_field` will be turned into a json array if `max_matches` is higher than 1, otherwise `target_field` will become a json object. In order to avoid documents getting too large, the maximum allowed value is 128. |
| `shape_relation` | no | `INTERSECTS` | A spatial relation operator used to match the [geoshape](/reference/elasticsearch/mapping-reference/geo-shape.md) of incoming documents to documents in the enrich index. This option is only used for `geo_match` enrich policy types. See [Spatial Relations](/reference/query-languages/query-dsl/query-dsl-shape-query.md#_spatial_relations) for operators and more information. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

