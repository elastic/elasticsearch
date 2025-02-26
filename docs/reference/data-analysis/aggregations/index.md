---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html
---

# Aggregations

% What needs to be done: Refine

% Scope notes: need to scope the page down to just reference content

% Use migrated content from existing pages that map to this page:

% - [ ] ./raw-migrated-files/elasticsearch/elasticsearch-reference/search-aggregations.md

Aggregations are a powerful framework that enables you to perform complex data analysis and summarization over indexed documents. They enable you to extract and compute statistics, trends, and patterns from large datasets.

{{es}} organizes aggregations into three categories:

* Metric aggregations that calculate metrics, such as a sum or average, from field values.
* Bucket aggregations that group documents into buckets, also called bins, based on field values, ranges, or other criteria.
* Pipeline aggregations that take input from other aggregations instead of documents or fields.