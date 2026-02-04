# Elasticsearch

{{es}} is a distributed search and analytics engine, scalable data store, and vector database built on Apache Lucene. It’s optimized for speed and relevance on production-scale workloads. Use Elasticsearch to search, index, store, and analyze data of all shapes and sizes in near real time.

## Quick links

:::{dropdown} Useful links

- [REST API Reference](./rest-apis/index.md)
- [API Conventions](./rest-apis/api-conventions.md)
- [Settings Reference](https://www.elastic.co/guide/en/elasticsearch/reference/current/settings.html)
- [Breaking Changes](https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes.html)
- [Compatibility](./rest-apis/compatibility.md)
- [Glossary](https://www.elastic.co/guide/en/elasticsearch/reference/current/glossary.html)
- [Plugin Development](https://www.elastic.co/guide/en/elasticsearch/plugins/current/index.html)
- [Supported Platforms](https://www.elastic.co/support/matrix#matrix_jvm)
:::

## Setup and architecture

- [Set up Elasticsearch](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md)
- [Secure the Elastic Stack](docs-content://deploy-manage/security.md)
- [Upgrade Elasticsearch](docs-content://deploy-manage/upgrade/deployment-or-cluster.md)
- [Set up a cluster for high availability](docs-content://deploy-manage/tools.md)
- [Stack monitoring](docs-content://deploy-manage/monitor/stack-monitoring.md)
- [Troubleshooting](docs-content://troubleshoot/elasticsearch.md)
- [Optimizations](docs-content://deploy-manage/production-guidance/optimize-performance.md)

## Working with data

- [Adding data to Elasticsearch](docs-content://manage-data/ingest.md)
- [Connectors](https://www.elastic.co/docs/reference/search-connectors)
- [Web crawler](https://www.elastic.co/search-labs/blog/elastic-open-crawler-release)
- [Data streams](docs-content://manage-data/data-store/data-streams.md)
- [Ingest pipelines](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md)
- [Mapping](docs-content://manage-data/data-store/mapping.md)
- [Data management](docs-content://manage-data/lifecycle.md)
- [Downsampling](docs-content://manage-data/lifecycle.md)
- [Snapshot and restore](docs-content://deploy-manage/tools/snapshot-and-restore.md)

## Search and analytics

{{es}} is the search and analytics engine that powers the {{stack}}.

- [Get started](docs-content://get-started/index.md)
- [Learn how to search your data](docs-content://solutions/search/querying-for-search.md)
- Query data programmatically: use query languages to run advanced search, filtering, or analytics
  - [Query DSL](docs-content://explore-analyze/query-filter/languages/querydsl.md): full JSON-based query language
  - [ES|QL](/reference/query-languages/esql.md): fast, SQL-like language with piped syntax
  - [EQL](docs-content://explore-analyze/query-filter/languages/eql.md): for event-based time series data, such as logs, metrics, and traces
  - [SQL](docs-content://explore-analyze/query-filter/languages/sql.md): SQL-style queries on Elasticsearch data
- [Search applications](docs-content://solutions/search/search-applications.md)
- [Aggregations](docs-content://explore-analyze/query-filter/aggregations.md)
- [Geospatial analysis](docs-content://explore-analyze/geospatial-analysis.md)
- [Machine Learning](docs-content://explore-analyze/machine-learning.md)
- [Alerting](docs-content://explore-analyze/alerts-cases.md)

## APIs and developer docs

- [REST APIs](https://www.elastic.co/docs/reference/elasticsearch/rest-apis)
- [{{es}} Clients](https://www.elastic.co/docs/reference/elasticsearch-clients)
- [Painless](https://www.elastic.co/docs/reference/scripting-languages/painless/painless)
- [Plugins and integrations](https://www.elastic.co/docs/reference/elasticsearch/plugins)
- [Search Labs](https://www.elastic.co/search-labs)
- [Notebook examples](https://www.elastic.co/search-labs/tutorials/examples)
