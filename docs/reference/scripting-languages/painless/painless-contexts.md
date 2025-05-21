---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-contexts.html
---

# Painless contexts [painless-contexts]

A Painless script is evaluated within a context. Each context has values that are available as local variables, an allowlist that controls the available classes, and the methods and fields within those classes (API), and if and what type of value is returned.

Painless scripts typically run within one of the contexts in the following table. Before using a Painless context, [configure the example data](/reference/scripting-languages/painless/painless-context-examples.md). Each context example is configured to operate on this data.

| Name | Painless Documentation | Elasticsearch Documentation |
| --- | --- | --- |
| Runtime field | [Painless Documentation](/reference/scripting-languages/painless/painless-runtime-fields-context.md) | [Elasticsearch Documentation](docs-content://manage-data/data-store/mapping/runtime-fields.md) |
| Ingest processor | [Painless Documentation](/reference/scripting-languages/painless/painless-ingest-processor-context.md) | [Elasticsearch Documentation](/reference/enrich-processor/script-processor.md) |
| Update | [Painless Documentation](/reference/scripting-languages/painless/painless-update-context.md) | [Elasticsearch Documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) |
| Update by query | [Painless Documentation](/reference/scripting-languages/painless/painless-update-by-query-context.md) | [Elasticsearch Documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query) |
| Reindex | [Painless Documentation](/reference/scripting-languages/painless/painless-reindex-context.md) | [Elasticsearch Documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex) |
| Sort | [Painless Documentation](/reference/scripting-languages/painless/painless-sort-context.md) | [Elasticsearch Documentation](/reference/elasticsearch/rest-apis/sort-search-results.md) |
| Similarity | [Painless Documentation](/reference/scripting-languages/painless/painless-similarity-context.md) | [Elasticsearch Documentation](/reference/elasticsearch/index-settings/similarity.md) |
| Weight | [Painless Documentation](/reference/scripting-languages/painless/painless-weight-context.md) | [Elasticsearch Documentation](/reference/elasticsearch/index-settings/similarity.md) |
| Score | [Painless Documentation](/reference/scripting-languages/painless/painless-score-context.md) | [Elasticsearch Documentation](/reference/query-languages/query-dsl/query-dsl-function-score-query.md) |
| Field | [Painless Documentation](/reference/scripting-languages/painless/painless-field-context.md) | [Elasticsearch Documentation](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#script-fields) |
| Filter | [Painless Documentation](/reference/scripting-languages/painless/painless-filter-context.md) | [Elasticsearch Documentation](/reference/query-languages/query-dsl/query-dsl-script-query.md) |
| Minimum should match | [Painless Documentation](/reference/scripting-languages/painless/painless-min-should-match-context.md) | [Elasticsearch Documentation](/reference/query-languages/query-dsl/query-dsl-terms-set-query.md) |
| Metric aggregation initialization | [Painless Documentation](/reference/scripting-languages/painless/painless-metric-agg-init-context.md) | [Elasticsearch Documentation](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) |
| Metric aggregation map | [Painless Documentation](/reference/scripting-languages/painless/painless-metric-agg-map-context.md) | [Elasticsearch Documentation](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) |
| Metric aggregation combine | [Painless Documentation](/reference/scripting-languages/painless/painless-metric-agg-combine-context.md) | [Elasticsearch Documentation](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) |
| Metric aggregation reduce | [Painless Documentation](/reference/scripting-languages/painless/painless-metric-agg-reduce-context.md) | [Elasticsearch Documentation](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) |
| Bucket script aggregation | [Painless Documentation](/reference/scripting-languages/painless/painless-bucket-script-agg-context.md) | [Elasticsearch Documentation](/reference/aggregations/search-aggregations-pipeline-bucket-script-aggregation.md) |
| Bucket selector aggregation | [Painless Documentation](/reference/scripting-languages/painless/painless-bucket-selector-agg-context.md) | [Elasticsearch Documentation](/reference/aggregations/search-aggregations-pipeline-bucket-selector-aggregation.md) |
| Watcher condition | [Painless Documentation](/reference/scripting-languages/painless/painless-watcher-condition-context.md) | [Elasticsearch Documentation](docs-content://explore-analyze/alerts-cases/watcher/condition-script.md) |
| Watcher transform | [Painless Documentation](/reference/scripting-languages/painless/painless-watcher-transform-context.md) | [Elasticsearch Documentation](docs-content://explore-analyze/alerts-cases/watcher/transform-script.md) |























