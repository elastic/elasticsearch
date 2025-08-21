---
navigation_title: "{{esql}}"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-language.html
---

# {{esql}} reference [esql-language]

{{esql}} is a piped query language for exploring and analyzing data in {{es}}. It is designed to be easy to use and understand, while also being powerful enough to handle complex data processing.

This reference section provides detailed technical information about {{esql}} features, syntax, and behavior:

* [Syntax reference](esql/esql-syntax-reference.md): Learn the basic syntax of commands, functions, and operators
* [Advanced workflows](esql/esql-advanced.md): Learn how to handle more complex tasks with these guides, including how to extract, transform, and combine data from multiple indices
* [Types and fields](esql/esql-types-and-fields.md): Learn about how {{esql}} handles different data types and special fields
* [Limitations](esql/limitations.md): Learn about the current limitations of {{esql}}
* [Examples](esql/esql-examples.md): Explore some example queries
* [Troubleshooting](esql/esql-troubleshooting.md): Learn how to diagnose and resolve issues with {{esql}}


## PORTED FROM E&A

http://localhost:3000docs-content://explore-analyze/query-filter/languages/esql


---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-getting-started.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-using.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-examples.html
  - https://www.elastic.co/guide/en/kibana/current/esql.html
products:
  - id: elasticsearch
  - id: kibana
---


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

- **Interactive interfaces**: Work with {{esql}} through Elastic user interfaces including Kibana Discover, Dashboards, Dev Tools, and analysis tools in Elastic Security and Observability.

## Documentation

### Usage guides
- **Get started**
  - [Get started in docs](docs-content://explore-analyze/query-filter/languages/esql-getting-started.md)
  - [Training course](https://www.elastic.co/training/introduction-to-esql)
- **{{esql}} interfaces**
  - [Use the query API](docs-content://explore-analyze/query-filter/languages/esql-rest.md)
  - [Use {{esql}} in Kibana](docs-content://explore-analyze/query-filter/languages/esql-kibana.md)
  - [Use {{esql}} in Elastic Security](docs-content://explore-analyze/query-filter/languages/esql-elastic-security.md)
- **{{esql}} for search use cases**
  - [{{esql}} for search landing page](docs-content://solutions/search/esql-for-search.md)
  - [{{esql}} for search tutorial](docs-content://solutions/search/esql-search-tutorial.md)
- **Query multiple sources**
  - [Query multiple indices](docs-content://explore-analyze/query-filter/languages/esql-multi-index.md)
  - [Query across clusters](docs-content://explore-analyze/query-filter/languages/esql-cross-clusters.md)

### Reference documentation

#### Core references
* [{{esql}} syntax](esql/esql-syntax.md)

#### Commands, functions, and operators
* [Commands](esql/esql-commands.md)
* [Functions and operators](esql/esql-functions-operators.md)

#### Field types
* [Metadata fields](esql/esql-metadata-fields.md)
* [Multivalued fields](esql/esql-multivalued-fields.md)

#### Advanced features
* [DISSECT and GROK](esql/esql-process-data-with-dissect-grok.md)
* [ENRICH](esql/esql-enrich-data.md)
* [LOOKUP JOIN](esql/esql-lookup-join.md)

#### Limitations
* [Limitations](esql/limitations.md)

