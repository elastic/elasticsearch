---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html
---

# Aggregations

:::{note}
This section provides detailed **reference information** for aggregations.

Refer to [Aggregations](docs-content://explore-analyze/query-filter/aggregations.md) in the **Explore and analyze** section for overview, getting started and conceptual information.
:::

Aggregations are a powerful framework that enables you to perform complex data analysis and summarization over indexed documents. They enable you to extract and compute statistics, trends, and patterns from large datasets.

{{es}} organizes aggregations into three categories:

* Metric aggregations that calculate metrics, such as a sum or average, from field values.
* Bucket aggregations that group documents into buckets, also called bins, based on field values, ranges, or other criteria.
* Pipeline aggregations that take input from other aggregations instead of documents or fields.