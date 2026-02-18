---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-contexts.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Painless contexts [painless-contexts]

In Painless, a context defines where and how your script runs in {{es}}. Each context determines three key aspects: which parameters are available to your script (such as `doc`, `ctx`, or `_source`), which Java classes and methods your script can access, and what type of value your script should return.

## What are contexts?

Contexts are runtime environments that determine script behavior within specific {{es}} operations. Unlike traditional scripting languages, where code runs consistently everywhere, Painless scripts are context-aware: the same syntax can access different variables and produce different results depending on the {{es}} operation that calls it.

## How contexts work

Painless supports scripting across numerous {{es}} operations, from search scoring to document processing to aggregation calculations. Each operation requires different capabilities; search scripts need read-only field access, while ingest scripts need document modification abilities. This diversity of requirements creates the need for specialized contexts.

Contexts ensure appropriate access and capabilities for each {{es}} operation while maintaining security and performance by providing exactly the inputs and APIs needed for a specific task through fine-grained allowlists and optimized data access patterns.

For example, search scoring contexts provide document fields as input and generate numerical scores as output, while ingest contexts provide documents as input for modification and generate transformed documents as output. Even if some contexts appear to be similar, each has tools and restrictions designed for a specific purpose.

## Context and data access

Contexts determine how you can access document data in Painless scripts:

:::{note}
The parameters listed here might not be available in all contexts. Some or even none might be available depending on the context that the script is written for.
:::

**`doc` values:**
:   Read-only field access using columnar storage for search, aggregation, and sorting context

**`ctx` access:**
:   Document modification capabilities in update, ingest, and reindex context

**`_source` access:**
:   Complete document JSON for runtime fields and transformations

**`params`**
:   User-defined parameters passed into scripts, available across all contexts

For detailed data access patterns and examples, refer to [Painless syntax-context bridge](docs-content://explore-analyze/scripting/painless-syntax-context-bridge.md).

## Context Categories overview

While Painless supports more than twenty specific contexts, they operate within four categories.

**Document Processing**
:   Transform and extract data from existing documents without modifying the original content. Use runtime fields, ingest processors, and field scripts to create computed fields or parse log data.

**Document Modification**
:   Change or update document context permanently in the index. Use update scripts, update by query, and reindex scripts to modify fields or restructure documents.  

**Search Enhancement**
:   Customize search behavior, scoring, or sorting without changing the indexed documents. Use filter scripts, and sort scripts for custom relevance or dynamic filtering.

**Advanced Operations**
:   Implement specialized functionality such as custom aggregations or monitoring conditions. Use metric aggregation, bucket aggregation, and Watcher scripts for complex calculations and alerting.

## Painless context reference

| Name | Painless documentation | {{es}} documentation |
| :---- | :---- | :---- |
| Runtime field | [Painless documentation](/reference/scripting-languages/painless/painless-runtime-fields-context.md) | [{{es}} documentation](docs-content://manage-data/data-store/mapping/runtime-fields.md) |
| Field | [Painless documentation](/reference/scripting-languages/painless/painless-field-context.md) | [{{es}} documentation](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#script-fields) |
| Ingest processor | [Painless documentation](/reference/scripting-languages/painless/painless-ingest-processor-context.md) | [{{es}} documentation](/reference/enrich-processor/script-processor.md) |
| Filter | [Painless documentation](/reference/scripting-languages/painless/painless-filter-context.md) | [{{es}} documentation](/reference/query-languages/query-dsl/query-dsl-script-query.md) |
| Score | [Painless documentation](/reference/scripting-languages/painless/painless-score-context.md) | [{{es}} documentation](/reference/query-languages/query-dsl/query-dsl-function-score-query.md) |
| Sort | [Painless documentation](/reference/scripting-languages/painless/painless-sort-context.md) | [{{es}} documentation](/reference/elasticsearch/rest-apis/sort-search-results.md) |
| Update | [Painless documentation](/reference/scripting-languages/painless/painless-update-context.md) | [{{es}} documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) |
| Update by query | [Painless documentation](/reference/scripting-languages/painless/painless-update-by-query-context.md) | [{{es}} documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query) |
| Reindex | [Painless documentation](/reference/scripting-languages/painless/painless-reindex-context.md) | [{{es}} documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex) |
| Similarity | [Painless documentation](/reference/scripting-languages/painless/painless-similarity-context.md) | [{{es}} documentation](/reference/elasticsearch/index-settings/similarity.md) |
| Weight | [Painless documentation](/reference/scripting-languages/painless/painless-weight-context.md) | [{{es}} documentation](/reference/elasticsearch/index-settings/similarity.md) |
| Minimum should match | [Painless documentation](/reference/scripting-languages/painless/painless-min-should-match-context.md) | [{{es}} documentation](/reference/query-languages/query-dsl/query-dsl-terms-set-query.md) |
| Metric aggregation initialization | [Painless documentation](/reference/scripting-languages/painless/painless-metric-agg-init-context.md) | [{{es}} documentation](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) |
| Metric aggregation map | [Painless documentation](/reference/scripting-languages/painless/painless-metric-agg-map-context.md) | [{{es}} documentation](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) |
| Metric aggregation combine | [Painless documentation](/reference/scripting-languages/painless/painless-metric-agg-combine-context.md) | [{{es}} documentation](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) |
| Metric aggregation reduce | [Painless documentation](/reference/scripting-languages/painless/painless-metric-agg-reduce-context.md) | [{{es}} documentation](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) |
| Bucket script aggregation | [Painless documentation](/reference/scripting-languages/painless/painless-bucket-script-agg-context.md) | [{{es}} documentation](/reference/aggregations/search-aggregations-pipeline-bucket-script-aggregation.md) |
| Bucket selector aggregation | [Painless documentation](/reference/scripting-languages/painless/painless-bucket-selector-agg-context.md) | [{{es}} documentation](/reference/aggregations/search-aggregations-pipeline-bucket-selector-aggregation.md) |
| Watcher condition | [Painless documentation](/reference/scripting-languages/painless/painless-watcher-condition-context.md) | [{{es}} documentation](docs-content://explore-analyze/alerts-cases/watcher/condition-script.md) |
| Watcher transform | [Painless documentation](/reference/scripting-languages/painless/painless-watcher-transform-context.md) | [{{es}} documentation](docs-content://explore-analyze/alerts-cases/watcher/transform-script.md) |

## Next steps

* **Most common context:** Start with [Field context](/reference/scripting-languages/painless/painless-field-context.md) or [Runtime field context](/reference/scripting-languages/painless/painless-runtime-fields-context.md) for data extraction and transformation.  
* **Data access patterns:** Review Painless syntax-context bridge for `doc`, `ctx`, and `_source` usage examples.  
* **Step-by-step tutorials**: refer to [How to write Painless scripts](docs-content://explore-analyze/scripting/modules-scripting-using.md) and our [Painless tutorials](docs-content://explore-analyze/scripting/common-script-uses.md) in the Explore and Analyze section.

Before using a Painless context, [configure the example data](/reference/scripting-languages/painless/painless-context-examples.md). Each context example is configured to operate on this data.


















