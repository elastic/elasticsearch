# {{es}}

{{es}} is a distributed search and analytics engine, scalable data store, and vector database built on Apache Lucene. It’s optimized for speed and relevance on production-scale workloads. Use {{es}} to search, index, store, and analyze data of all shapes and sizes in near real time.

This section contains technical reference information for {{es}}. For guides, tutorials, and concepts, go to the [{{es}} documentation hub](docs-content://products/elasticsearch.md).

## In this section

Look up technical details about {{es}}, including settings, field types, REST API conventions, and security privileges.

- [](rest-apis/index.md): Conventions and concepts for working with the {{es}} REST APIs.
  - [](rest-apis/api-conventions.md)
  - [](rest-apis/compatibility.md)
- [](security-privileges.md): The cluster, index, run as, and application permissions that a role can grant.
- [](roles.md): Predefined roles, such as `superuser` and `viewer`, that you can assign to users.
- [](elasticsearch-audit-events.md): Security events, such as granted and denied access requests, that {{es}} can write to its audit log.
- [](jvm-settings.md): Customize Java heap size and other JVM options for {{es}}.
- [](configuration-reference/index.md): Cluster and node settings, such as networking, discovery, security, snapshot repositories, and thread pools.
- [](index-settings/index.md): Settings that control how individual indices behave, such as shard allocation, merging, slow logs, and index sorting.
- [](mapping-reference/index.md): Field data types, metadata fields, and mapping parameters that define how documents are stored and indexed.
- [](columnar/index.md): Store index data in a column-oriented format for analytical and search workloads.
- [](index-lifecycle-actions/index.md): Steps that a lifecycle policy can run on an index, such as rollover, shrink, downsample, and delete.
- [](command-line-tools/index.md): Tools in the {{es}} `bin` directory, for tasks such as generating certificates and configuring security.

## Key topics for working with {{es}}

Find key documentation about configuring, indexing into, and querying the {{es}} engine, across the Elastic docs. For {{ecloud}}, solutions, and other platform features, go to the [{{es}} documentation hub](docs-content://products/elasticsearch.md).

### Quick links

Frequently used reference pages, release information, and supported platforms.

- [](/release-notes/index.md), including [breaking changes](/release-notes/breaking-changes.md)
- [Glossary](docs-content://reference/glossary/index.md)
- [Supported platforms](https://www.elastic.co/support/matrix#matrix_jvm)

### Deploy and manage

Install, secure, upgrade, monitor, and tune {{es}}.

- [Install a self-managed {{es}} cluster](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md)
- [Secure the Elastic Stack](docs-content://deploy-manage/security.md)
- [Upgrade {{es}}](docs-content://deploy-manage/upgrade/deployment-or-cluster.md)
- [Set up a cluster for high availability](docs-content://deploy-manage/tools.md)
- [Stack monitoring](docs-content://deploy-manage/monitor/stack-monitoring.md)
- [Troubleshooting](docs-content://troubleshoot/elasticsearch.md)
- [Optimizations](docs-content://deploy-manage/production-guidance/optimize-performance.md)

### Prepare and index data

Add data to {{es}}, map fields, and manage data over its lifecycle.

- [Adding data to {{es}}](docs-content://manage-data/ingest.md)
- [Connectors](https://www.elastic.co/docs/reference/search-connectors)
- [Web crawler](https://www.elastic.co/search-labs/blog/elastic-open-crawler-release)
- [Data streams](docs-content://manage-data/data-store/data-streams.md)
- [Ingest pipelines](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md)
- [Mapping](docs-content://manage-data/data-store/mapping.md)
- [Data management](docs-content://manage-data/lifecycle.md)
- [Downsampling](docs-content://manage-data/data-store/data-streams/downsampling-time-series-data-stream.md)
- [Snapshot and restore](docs-content://deploy-manage/tools/snapshot-and-restore.md)

### Search and analyze data

Search, query, and analyze your data with {{es}}.

- [Get started](docs-content://get-started/index.md)
- [Learn how to search your data](docs-content://solutions/search/querying-for-search.md)
- Query data programmatically: use query languages to run advanced search, filtering, or analytics
  - [Query DSL](docs-content://explore-analyze/query-filter/languages/querydsl.md): full JSON-based query language
  - [](/reference/query-languages/esql.md): fast, SQL-like language with piped syntax
  - [EQL](docs-content://explore-analyze/query-filter/languages/eql.md): for event-based time series data, such as logs, metrics, and traces
  - [SQL](docs-content://explore-analyze/query-filter/languages/sql.md): SQL-style queries on {{es}} data
- [Search applications](docs-content://solutions/elasticsearch-solution-project/search-applications.md)
- [Aggregations](docs-content://explore-analyze/query-filter/aggregations.md)
- [Geospatial analysis](docs-content://explore-analyze/geospatial-analysis.md)
- [Machine learning](docs-content://explore-analyze/machine-learning.md)
- [Alerting](docs-content://explore-analyze/alerting.md)

### Build applications

Work with {{es}} from your code and extend it with plugins.

- [](/reference/elasticsearch/rest-apis/index.md)
- [{{es}} clients](https://www.elastic.co/docs/reference/elasticsearch-clients)
- [](/reference/scripting-languages/painless/painless.md)
- [](/reference/elasticsearch-plugins/index.md)

### Learn from examples

Find tutorials, code examples, and articles about building with {{es}}.

- [Search Labs](https://www.elastic.co/search-labs)
- [Notebook examples](https://www.elastic.co/search-labs/tutorials/examples)
