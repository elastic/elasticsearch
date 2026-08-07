---
applies_to:
  stack:
  serverless:
products:
  - id: elasticsearch
navigation_title: "Troubleshooting"
description: "Resources for troubleshooting ES|QL issues, including query logging, task management, circuit breaker settings, and support case details."
---

# Troubleshooting {{esql}} [esql-troubleshooting]

This section provides some useful resource for troubleshooting {{esql}} issues:

- {applies_to}`stack: preview 9.4` {applies_to}`serverless: unavailable` [Query logging](docs-content://deploy-manage/monitor/logging-configuration/query-logs.md): Log all query types, including {{esql}}, with a managed data stream (recommended)
- [Legacy {{esql}} query log](esql-query-log.md): Log {{esql}} queries using the older `esql.querylog` settings
- [Task management API](esql-task-management.md): Learn how to diagnose issues like long-running queries.
- [Circuit breaker settings](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#circuit-breakers-page-esql): Learn how {{esql}} uses the circuit breaker and how to configure it. For circuit breaker errors, see [Circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

::::{include} _snippets/common/query-performance-tip.md
::::

## Filing a support case

When filing a support case for a slow or failing {{esql}} query, include the following information so the support team can investigate quickly:

- The full {{esql}} query text
- The cluster version, available from `GET /`
- The index pattern and approximate document count
- The time range covered by the query
- The fields and field types across all indices participating in the query, available from the [field capabilities API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-field-caps)
- The `took` value from the response
- Any error messages
- A query log entry, if [query logging](/reference/query-languages/esql/esql-query-log.md) is enabled
