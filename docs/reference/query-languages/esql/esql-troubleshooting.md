---
applies_to:
  stack:
  serverless:
navigation_title: "Troubleshooting"
---

# Troubleshooting {{esql}} [esql-troubleshooting]

This section provides some useful resource for troubleshooting {{esql}} issues:

- {applies_to}`stack: preview 9.4` {applies_to}`serverless: unavailable` [Query logging](docs-content://deploy-manage/monitor/logging-configuration/query-logs.md): Log all query types, including {{esql}}, with a managed data stream (recommended)
- [Legacy {{esql}} query log](esql-query-log.md): Log {{esql}} queries using the older `esql.querylog` settings
- [Task management API](esql-task-management.md): Learn how to diagnose issues like long-running queries.
- [Circuit breaker settings](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#circuit-breakers-page-esql): Learn how {{esql}} uses the circuit breaker and how to configure it. For circuit breaker errors, see [Circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).
