---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics.html
---

# Metrics [search-aggregations-metrics]

The aggregations in this family compute metrics based on values extracted in one way or another from the documents that are being aggregated. The values are typically extracted from the fields of the document (using the field data), but can also be generated using scripts.

Numeric metrics aggregations are a special type of metrics aggregation which output numeric values. Some aggregations output a single numeric metric (e.g. `avg`) and are called `single-value numeric metrics aggregation`, others generate multiple metrics (e.g. `stats`) and are called `multi-value numeric metrics aggregation`. The distinction between single-value and multi-value numeric metrics aggregations plays a role when these aggregations serve as direct sub-aggregations of some bucket aggregations (some bucket aggregations enable you to sort the returned buckets based on the numeric metrics in each bucket).


























