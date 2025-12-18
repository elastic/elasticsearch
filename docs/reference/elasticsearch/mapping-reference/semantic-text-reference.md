---
navigation_title: "Reference"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Semantic text field type reference  [semantic-text]

This page provides reference content for the `semantic_text` field type, including parameter descriptions, {{infer}} endpoint configuration options, chunking behavior, update operations, querying options, and limitations.

## Parameters for `semantic_text` [semantic-text-params]

The `semantic_text` field type uses default indexing settings based on the [{{infer}} endpoint](#configuring-inference-endpoints) specified, enabling you to get started without providing additional configuration details. You can override these defaults by customizing the parameters described below.

`inference_id`
:   (Optional, string) {{infer-cap}} endpoint that will be used to generate
embeddings for the field. If `search_inference_id` is specified, the {{infer}}
endpoint will only be used at index time. Learn more about [configuring this parameter](#configuring-inference-endpoints).

**Updating the `inference_id` parameter**

::::{applies-switch}

:::{applies-item} { "stack": "ga 9.0" }
This parameter cannot be updated.
:::

:::{applies-item} { "stack": "ga 9.3" }

You can update this parameter by using
the [Update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
You can update the {{infer}} endpoint if no values have been indexed or if the new endpoint is compatible with the current one.

::::{important}
When updating an `inference_id` it is important to ensure the new {{infer}} endpoint produces embeddings compatible with those already indexed. This typically means using the same underlying model.
::::

:::

::::

`search_inference_id`
:   (Optional, string) The {{infer}} endpoint that will be used to generate
embeddings at query time. Use the [Create {{infer}} API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) to create the endpoint. If not specified, the {{infer}} endpoint defined by
`inference_id` will be used at both index and query time. 
    
    You can update this parameter by using
the [Update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).     
    
    Learn how to [use dedicated endpoints for ingestion and search](./semantic-text-setup-configuration.md#dedicated-endpoints-for-ingestion-and-search).

`index_options` {applies_to}`stack: ga 9.1`
:   (Optional, object) Specifies the index options to override default values
for the field. Currently, `dense_vector` and `sparse_vector` index options are supported. For text embeddings, `index_options` may match any allowed.

- [dense_vector index options](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-index-options)

- [sparse_vector index options](/reference/elasticsearch/mapping-reference/sparse-vector.md#sparse-vectors-params) {applies_to}`stack: ga 9.2`

`chunking_settings` {applies_to}`stack: ga 9.1`
:   (Optional, object) Settings for chunking text into smaller passages.
If specified, these will override the chunking settings set in the {{infer-cap}}
endpoint associated with `inference_id`.

    If chunking settings are updated, they will not be applied to existing documents
until they are reindexed.  Defaults to the optimal chunking settings for [Elastic Rerank](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-rerank.md).

    To completely disable chunking, use the `none` chunking strategy.

    ::::{important}
    When using the `none` chunking strategy, if the input exceeds the maximum token limit of the underlying model,
    some services (such as OpenAI) may return an error. In contrast, the `elastic` and `elasticsearch` services will
    automatically truncate the input to fit within the model's limit.
     ::::

### Customizing semantic_text indexing

The following example shows how to configure `inference_id`, `index_options` and `chunking_settings` for a `semantic_text` field type:

```console
PUT my-index-000004
{
  "mappings": {
    "properties": {
      "inference_field": {
        "type": "semantic_text",
        "inference_id": "my-text-embedding-endpoint", <1>
        "index_options": { <2>
          "dense_vector": {
            "type": "int4_flat"
          }
        },
        "chunking_settings": { <3>
          "type": "none"
        }
      }
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]

1. The `inference_id` of the {{infer}} endpoint to use for generating embeddings.
2. Overrides default index options by specifying `int4_flat` quantization for dense vector embeddings.
3. Disables automatic chunking by setting the chunking strategy to `none`.

::::{note}
{applies_to}`stack: ga 9.1`  Newly created indices with `semantic_text` fields using dense embeddings will be
[quantized](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization)
to `bbq_hnsw` automatically as long as they have a minimum of 64 dimensions.
::::

## {{infer-cap}} endpoints [configuring-inference-endpoints]

The `semantic_text` field type specifies an {{infer}} endpoint identifier (`inference_id`) that is used to generate embeddings.

The following {{infer}} endpoint configurations are available:

- [Default and preconfigured endpoints](./semantic-text-setup-configuration.md#default-and-preconfigured-endpoints): Use `semantic_text` without creating an {{infer}} endpoint manually. 

- [ELSER on EIS](./semantic-text-setup-configuration.md#using-elser-on-eis): Use the ELSER model through the Elastic {{infer-cap}} Service. 

- [Custom endpoints](./semantic-text-setup-configuration.md#using-custom-endpoint): Create your own {{infer}} endpoint using the [Create {{infer}} API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) to use custom models or third-party services.

If you use a [custom {{infer}} endpoint](./semantic-text-setup-configuration.md#using-custom-endpoint) through your ML node and not through Elastic {{infer-cap}} Service (EIS), the recommended method is to [use dedicated endpoints for ingestion and search](./semantic-text-setup-configuration.md#dedicated-endpoints-for-ingestion-and-search). 

{applies_to}`stack: ga 9.1.0` If you use EIS, you don't have to set up dedicated endpoints.

::::{warning}
Removing an {{infer}} endpoint will cause ingestion of documents and semantic
queries to fail on indices that define `semantic_text` fields with that
{{infer}} endpoint as their `inference_id`. Trying
to [delete an {{infer}} endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-delete)
that is used on a `semantic_text` field will result in an error.
::::

## Chunking [chunking-behavior]

{{infer-cap}} endpoints have a limit on the amount of text they can process. To
allow for large amounts of text to be used in semantic search, `semantic_text`
automatically generates smaller passages if needed, called chunks.

Each chunk refers to a passage of the text and the corresponding embedding
generated from it. When querying, the individual passages will be automatically
searched for each document, and the most relevant passage will be used to
compute a score.

Chunks are stored as start and end character offsets rather than as separate
text strings. These offsets point to the exact location of each chunk within the
original input text.

You can [pre-chunk content](./semantic-text-ingestions.md#pre-chunking) by providing text as arrays before indexing.

Refer to the [{{infer-cap}} API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put#operation-inference-put-body-application-json-chunking_settings) for values for `chunking_settings` and to [Configuring chunking](docs-content://explore-analyze/elastic-inference/inference-api.md#infer-chunking-config) to learn about different chunking strategies.


## Automatic pre-filtering for dense vector embeddings [auto-prefiltering]
```{applies_to}
stack: ga 9.3
serverless: ga
```

Querying `semantic_text` fields that have dense vector embeddings automatically applies
filters found in the Query DSL tree or [ES|QL](/reference/query-languages/esql.md) query
as pre-filters in order to ensure the requested number of results is returned.

In the following example we are using a `match` query against a `semantic_text` field
in order to find the 10 most relevant documents that match the query "quick drying t-shirts".
However, we are only interested in green t-shirts, so we add a filter query to exclude other colors.

In Query DSL we would express this query as follows:


```console
POST my-index/_search
{
  "size" : 10,
  "query" : {
    "bool" : {
      "must" : {
        "match": {
          "dense_semantic_text_field": {
            "query": "quick drying t-shirts"
          }
        }
      },
      "filter" : {
        "term" : {
          "color": {
            "value": "green"
          }
        }
      }
    }
  }
}
```
% TEST[skip:Requires {{infer}} endpoint]


The `term` query will be applied as a pre-filter, meaning that when the *knn* search executes on
`dense_semantic_text_field`, only documents that matched the `term` query will be searched.

If the `term` query was applied as a post-filter, which is the default behavior for such filters,
the *knn* search would execute against all documents, and then the `term` query would filter out
documents that did not match. This could mean that fewer than 10 documents are returned if there
are more relevant documents that are not green.

::::{note}
The queries in Query DSL that are used as pre-filters to `semantic_text` queries are all `must`,
 `filter`, and `must_not` queries that are within parent `bool` queries.
::::

::::{important}
Querying a `semantic_text` field directly with a [kNN query](/reference/query-languages/query-dsl/query-dsl-knn-query.md#knn-query-with-semantic-text)
does not apply automatic pre-filtering. `knn` queries provide a direct parameter for defining pre-filters
as explained in [knn-query-filtering](/reference/query-languages/query-dsl/query-dsl-knn-query.md#knn-query-filtering).
::::

Similarly, in ES|QL we would express this query as follows:

```console
POST /_query
{
  "query": """
          FROM my-index METADATA _score
          | WHERE MATCH(dense_semantic_text_field, "quick drying t-shirts")
          | WHERE color == "green"
          | SORT _score DESC
          | LIMIT 10
   """
}
```
% TEST[skip:Requires {{infer}} endpoint]

The `WHERE color == "green"` clause will be applied as a pre-filter to similar effect.


## Limitations [limitations]

`semantic_text` field types have the following limitations:

* `semantic_text` fields are not currently supported as elements
  of [nested fields](/reference/elasticsearch/mapping-reference/nested.md).
* `semantic_text` fields can't currently be set as part
  of [dynamic templates](docs-content://manage-data/data-store/mapping/dynamic-templates.md).
* `semantic_text` fields are not supported in indices created prior to 8.11.0.
* `semantic_text` fields do not support [Cross-Cluster Search (CCS)](docs-content://explore-analyze/cross-cluster-search.md) when [`ccs_minimize_roundtrips`](docs-content://explore-analyze/cross-cluster-search.md#ccs-network-delays) is set to `false`.
* `semantic_text` fields do not support [Cross-Cluster Search (CCS)](docs-content://explore-analyze/cross-cluster-search.md) in [ES|QL](/reference/query-languages/esql.md).
* `semantic_text` fields do not support [Cross-Cluster Replication (CCR)](docs-content://deploy-manage/tools/cross-cluster-replication.md).
* automatic pre-filtering in Query DSL does not apply on [Nested queries](/reference/query-languages/query-dsl/query-dsl-nested-query.md). Such queries will be applied as post-filters.
* automatic pre-filtering in ES|QL does not apply on filters that are not translatable to Lucene. Such filters will be applied as post-filters.


## Document count discrepancy in `_cat/indices` [document-count-discrepancy]

When an index contains a `semantic_text` field, the `docs.count` value returned by the [`_cat/indices`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-indices) API may be higher than the number of documents you indexed. 
This occurs because `semantic_text` stores embeddings in [nested documents](/reference/elasticsearch/mapping-reference/nested.md), one per chunk. The `_cat/indices` API counts all documents in the Lucene index, including these hidden nested documents.

To count only top-level documents, excluding the nested documents that store embeddings, use one of the following APIs:

* `GET /<index>/_count`
* `GET _cat/count/<index>`