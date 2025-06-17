---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/rest-apis.html
  - https://www.elastic.co/guide/en/serverless/current/elasticsearch-differences.html
---

# REST APIs

Elasticsearch exposes REST APIs that are used by the UI components and can be called directly to configure and access Elasticsearch features.

### Search APIs
- [Search API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/search/)
- [Multi Search API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/msearch/)
- [Search Template API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/render-search-template/)
- [Explain API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/explain/)
- [Validate Query API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/validate-query/)

### Document APIs
- [Index API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/index/)
- [Get API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/get/)
- [Delete API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/delete/)
- [Update API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/update/)
- [Bulk API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/bulk/)

### Index APIs
- [Create Index API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/indices-create-index/)
- [Delete Index API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/indices-delete-index/)
- [Get Index API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/indices-get-index/)
- [Put Mapping API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/indices-put-mapping/)
- [Index Settings API](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/indices-put-settings/)

### Machine Learning APIs
- [Anomaly Detection Jobs](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/ml-put-job/)
- [Datafeeds](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/ml-put-datafeed/)
- [Model Management](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/ml-put-trained-model/)

### Cat APIs (Compact, human-readable)
- [Cat Indices](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/cat/cat-indices/)
- [Cat Nodes](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/cat/cat-nodes/)
- [Cat Shards](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/cat/cat-shards/)
- [Cat Health](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/cat/cat-health/)

### Cluster APIs
- [Cluster Health](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/cluster-health/)
- [Cluster Stats](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/cluster-stats/)
- [Cluster Reroute](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/cluster-reroute/)

### Security APIs
- [User Management](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/security-put-user/)
- [Role Management](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/security-put-role/)
- [API Keys](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/security-api-create-api-key/)

### Ingest APIs
- [Ingest Pipelines](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/ingest-put-pipeline/)
- [Simulate Pipeline](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/ingest-simulate-pipeline/)

This section includes:

- [API conventions](/reference/elasticsearch/rest-apis/api-conventions.md)
- [Common options](/reference/elasticsearch/rest-apis/common-options.md)
- [Compatibility](/reference/elasticsearch/rest-apis/compatibility.md)
- [Examples](/reference/elasticsearch/rest-apis/api-examples.md)
