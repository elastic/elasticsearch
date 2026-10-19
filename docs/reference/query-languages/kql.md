---
description: Syntax reference for KQL (Kibana Query Language), with examples for field values, ranges, wildcards, combined conditions, and nested fields.
mapped_pages:
  - https://www.elastic.co/guide/en/kibana/current/kuery-query.html
---

# {{kib}} Query Language (KQL) syntax reference [kuery-query]

This page shows the KQL syntax for each kind of condition, with examples you can adapt. To learn what KQL is and where to use it, refer to [{{kib}} Query Language (KQL) overview](docs-content://explore-analyze/query-filter/languages/kql.md).


## Filter for documents where a field exists [_filter_for_documents_where_a_field_exists]

To match documents that have an indexed value for a field, use `*` as the value. For example, documents where `http.request.method` exists:

```yaml
http.request.method: *
```

This matches any indexed value, including an empty string.


## Filter for documents that match a value [_filter_for_documents_that_match_a_value]

To match a number, text, date, or boolean value, enter the field name, a colon, and the value. For example, documents where `http.request.method` is GET:

```yaml
http.request.method: GET
```

On keyword, numeric, date, or boolean fields, the value must match exactly, including punctuation and case.

On `text` fields, {{es}} analyzes the value using the [field's mapping settings](docs-content://solutions/search/full-text/text-analysis-during-search.md). For example, documents where `http.request.body.content` contains "null pointer":

```yaml
http.request.body.content: null pointer
```

Because this is a `text` field, the order of the search terms doesn't matter. Documents that contain "pointer null" also match. To match the terms in that order, surround the value in quotation marks:

```yaml
http.request.body.content: "null pointer"
```

### Search all fields without a field name

The field name is optional. If you leave it out, KQL searches all fields for the value. For example, to search all fields for "Hello":

```yaml
Hello
```

### Escape special characters

To use one of these characters in a value, escape it with a backslash, unless you surround the value with quotation marks:

```yaml
\():<>"*
```

For example, either of these queries matches documents where `http.request.referrer` is `https://example.com`:

```yaml
http.request.referrer: "https://example.com"
http.request.referrer: https\://example.com
```


## Filter for documents within a range [_filter_for_documents_within_a_range]

To match values in a range, use `<`, `<=`, `>`, or `>=`. For example, documents where `http.response.bytes` is less than 10000:

```yaml
http.response.bytes < 10000
```

For an inclusive range, combine two conditions. For example, documents where `http.response.bytes` is greater than 10000 and less than or equal to 20000:

```yaml
http.response.bytes > 10000 and http.response.bytes <= 20000
```

On multi-value fields, KQL tests each condition against every value in the array. `number > 300 AND number < 400` matches `"number": [500, 10]`. The value 500 matches the first condition and 10 matches the second. If one value must satisfy every condition, use [Query DSL](/reference/query-languages/query-dsl/query-dsl-range-query.md).

You can also use range syntax with strings, IP addresses, and timestamps. For example, documents with a timestamp earlier than two weeks ago:

```yaml
@timestamp < now-2w
```

For more examples of acceptable date formats, refer to [Date Math](/reference/elasticsearch/rest-apis/common-options.md#date-math).


## Filter for documents using wildcards [_filter_for_documents_using_wildcards]

To match a pattern, use a wildcard. Wildcards work on keyword, text, and wildcard fields. They don't work on numeric, date, or boolean fields. KQL supports only `*`, which matches zero or more characters.

For example, documents where `machine.os` begins with "win":

```yaml
machine.os: win*
```

By default, you can put `*` at the start of a pattern. For example, documents where `url` contains `elastic`:

```yaml
url: *elastic*
```

Queries that start with `*` can slow searches.

{applies_to}`stack: ga` {applies_to}`serverless: unavailable` In {{kib}}, to avoid slow searches, turn leading wildcards off with the [`query:allowLeadingWildcards`](kibana://reference/advanced-settings.md#query-allowleadingwildcards) advanced setting.


## Exclude documents with a negated query [_negating_a_query]

To exclude documents, use the `not` keyword (not case-sensitive). For example, documents where `http.request.method` is not GET:

```yaml
NOT http.request.method: GET
```


## Combine multiple queries [_combining_multiple_queries]

To combine queries, use `and` or `or` (not case-sensitive). For example, documents where `http.request.method` is GET **or** `http.response.status_code` is 400:

```yaml
http.request.method: GET OR http.response.status_code: 400
```

To require both conditions, use `and`. For example, documents where `http.request.method` is GET **and** `http.response.status_code` is 400:

```yaml
http.request.method: GET AND http.response.status_code: 400
```

To set precedence, use parentheses. For example, documents where `http.request.method` is GET and `http.response.status_code` is 200, **or** `http.request.method` is POST and `http.response.status_code` is 400:

```yaml
(http.request.method: GET AND http.response.status_code: 200) OR
(http.request.method: POST AND http.response.status_code: 400)
```

You can also use parentheses to match several values on one field. For example, documents where `http.request.method` is GET, POST, **or** DELETE:

```yaml
http.request.method: (GET OR POST OR DELETE)
```


## Match multiple fields [_matching_multiple_fields]

To query several fields at once, use a wildcard in the field name. For example, documents where any sub-field of `datastream` contains "logs":

```yaml
datastream.*: logs
```

The query can fail if the matching fields have different types. For example, if `datastream.*` matches both numeric and string fields, `datastream.*: logs` returns an error because you can't query numeric fields for string values.


## Query nested fields [_querying_nested_fields]

[Nested fields](/reference/elasticsearch/mapping-reference/nested.md) use a special syntax. In this document, `user` is a nested field:

```json
{
  "user" : [
    {
      "first" : "John",
      "last" :  "Smith"
    },
    {
      "first" : "Alice",
      "last" :  "White"
    }
  ]
}
```

To find documents where a single value in the `user` array has the first name "Alice" **and** the last name "White":

```yaml
user:{ first: "Alice" and last: "White" }
```

If nested fields contain other nested fields, use the full path of the field you want to query. In this document, `user` and `names` are both nested fields:

```json
{
  "user": [
    {
      "names": [
        {
          "first": "John",
          "last": "Smith"
        },
        {
          "first": "Alice",
          "last": "White"
        }
      ]
    }
  ]
}
```

To find documents where a single value in the `user.names` array has the first name "Alice" **and** the last name "White":

```yaml
user.names:{ first: "Alice" and last: "White" }
```
