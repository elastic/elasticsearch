---
applies_to:
  stack:
  serverless:
navigation_title: "{{esql}}"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-language.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-getting-started.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-using.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-examples.html
products:
  - id: elasticsearch
---

# {{esql}} reference [esql-language]

**Elasticsearch Query Language ({{esql}})** is a piped query language for filtering, transforming, and analyzing data.

## What's {{esql}}? [_the_esql_compute_engine]

You can author {{esql}} queries to find specific events, perform statistical analysis, and create visualizations. It supports a wide range of commands, functions, and operators to perform various data operations, such as filter, aggregation, time-series analysis, and more. It initially supported a subset of the features available in Query DSL, but it is rapidly evolving with every {{serverless-full}} and Stack release.

{{esql}} is designed to be easy to read and write, making it accessible for users with varying levels of technical expertise. It is particularly useful for data analysts, security professionals, and developers who need to work with large datasets in Elasticsearch.

## How does it work? [search-analyze-data-esql]

{{esql}} uses pipes (`|`) to manipulate and transform data in a step-by-step fashion. This approach allows you to compose a series of operations, where the output of one operation becomes the input for the next, enabling complex data transformations and analysis.

Here's a simple example of an {{esql}} query:

```esql
FROM sample_data
| SORT @timestamp DESC
| LIMIT 3
```

Note that each line in the query represents a step in the data processing pipeline:
- The `FROM` clause specifies the index or data stream to query
- The `SORT` clause sorts the data by the `@timestamp` field in descending order
- The `LIMIT` clause restricts the output to the top 3 results

### User interfaces

You can interact with {{esql}} in two ways:

- **Programmatic access**: Use {{esql}} syntax with the {{es}} `_query` endpoint.
  - Refer to [](esql/esql-rest.md)

- **Interactive interfaces**: Work with {{esql}} through Elastic user interfaces including Kibana Discover, Dashboards, Dev Tools, and analysis tools in Elastic Security and Observability.
  - Refer to [Using {{esql}} in {{kib}}](docs-content://explore-analyze/query-filter/languages/esql-kibana.md).

