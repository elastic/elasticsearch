---
navigation_title: "KQL"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-kql-query.html
applies_to:
  stack: preview 9.0, ga 9.1
  serverless: all
---

# KQL query [query-dsl-kql-query]

Returns documents matching a provided KQL expression. The value of the `query` parameter is parsed using the
[Kibana Query Language (KQL)](/reference/query-languages/kql.md) and rewritten
into standard Query DSL.

Use this query when you want to accept KQL input (for example from a Kibana search bar) directly in Elasticsearch.

## Example request [kql-query-example]

The following example returns documents where `service.name` is `"checkout-service"` and `http.response.status_code` is `200`.

```console
GET /_search
{
  "query": {
    "kql": {
      "query": "service.name: \"checkout-service\" AND http.response.status_code: 200"
    }
  }
}
```

## Top-level parameters for `kql` [kql-top-level-params]

`query`
:   (Required, string) The KQL expression to parse. See the
[KQL language reference](/reference/query-languages/kql.md) for supported
syntax.

`case_insensitive`
:   (Optional, boolean) If `true`, performs case-insensitive matching for field names and keyword / text terms.
    Defaults to `false`.

`default_field`
:   (Optional, string) Default field (or field pattern with wildcards) to target when a bare term in the query
    string does not specify a field. Supports wildcards (`*`).

    Defaults to the [`index.query.default_field`](/reference/elasticsearch/index-settings/index-modules.md#index-query-default-field)
    index setting (default value is `*`). The value `*` expands to all fields that are eligible for term queries
    (metadata fields excluded). Searching *all* eligible fields can be expensive on mappings with many fields and
    is subject to the `indices.query.bool.max_clause_count` limit.

    Like other queries, this implicit expansion does not traverse [nested](/reference/elasticsearch/mapping-reference/nested.md)
    documents.

`time_zone`
:   (Optional, string) [UTC offset](https://en.wikipedia.org/wiki/List_of_UTC_time_offsets) or
    [IANA time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) used to interpret date literals
    (for example `@timestamp > now-2d`). Does not change the value of `now` (which is always system UTC) but affects
    rounding and the interpretation of explicit date strings.

`boost`
:   (Optional, float) Standard query *boost*. Defaults to `1.0`.

`_name`
:   (Optional, string) A name to identify this query in the response's `matched_queries`.

## Syntax reference

The `kql` query accepts a single KQL expression string in the `query` parameter. All language elements (field matching,
wildcards, boolean logic, ranges, nested scoping) are defined in the
[KQL language reference](/reference/query-languages/kql.md). This page does not
duplicate that syntax.

## Additional examples [kql-examples]

### Case-insensitive matching (log level)

```console
GET /_search
{
  "query": {
    "kql": {
      "query": "log.level: ErRoR",
      "case_insensitive": true
    }
  }
}
```

### Using a default field pattern

Search any field under the `logs` object for the term `timeout`:

```console
GET /_search
{
  "query": {
    "kql": {
      "query": "timeout",
      "default_field": "logs.*"
    }
  }
}
```

### Date range with time zone

```console
GET /_search
{
  "query": {
    "kql": {
      "query": "@timestamp >= ""2025-10-01"" AND @timestamp < ""2025-10-02""",
      "time_zone": "Europe/Paris"
    }
  }
}
```

### Nested field query
```console
GET /_search
{
  "query": {
    "kql": {
      "query": "events.stack:{ file: \"app.js\" AND line: 42 }"
    }
  }
}
```

## When to use `kql` vs `query_string`

Use `kql` for user-facing search boxes where you want a concise, filter-oriented syntax and to avoid Lucene’s
advanced operators (fuzzy, regexp, proximity, inline boosting). Use [`query_string`](./query-dsl-query-string-query.md)
or [`simple_query_string`](./query-dsl-simple-query-string-query.md) when those advanced features are required.

## Notes and limitations

* The parsed KQL expression is rewritten into standard Query DSL and participates in scoring unless wrapped in a
  filter context (for example inside a `bool.filter`). Adjust relevance with `boost` if needed.
* Large wildcard expansions (in field names or terms) can hit the `indices.query.bool.max_clause_count` safeguard.
* Nested documents require the KQL nested syntax (`path:{ ... }`); terms are not correlated across separate nested
  objects automatically.
* Unsupported syntax (such as fuzzy operators) results in a parse error.

## See also [kql-see-also]

* [KQL language reference](/reference/query-languages/kql.md)
* [Default field setting](/reference/elasticsearch/index-settings/index-modules.md#index-query-default-field)
