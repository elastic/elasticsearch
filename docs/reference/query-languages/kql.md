---
description: Syntax reference for KQL (Kibana Query Language), with examples for field values, ranges, wildcards, combined conditions, and nested fields.
navigation_title: KQL
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
  - id: kibana
---

# {{kib}} Query Language (KQL) syntax reference [kuery-query]

KQL (Kibana Query Language) is a text-based language for filtering documents by field values, ranges, wildcards, and combined conditions. You can use KQL syntax in:

* The {{kib}} query bar, for example in **Discover** and in dashboards.
* The Query DSL [`kql` clause](/reference/query-languages/query-dsl/query-dsl-kql-query.md), for example in a `_search` request.
* The {{esql}} [`KQL` function](/reference/query-languages/esql/functions-operators/search-functions/kql.md).

This page is the syntax reference, with examples you can adapt. The syntax is the same in all three places, with a few differences listed in [Differences between {{kib}} and {{es}}](#kql-differences). To learn what KQL is and where to use it, refer to the [{{kib}} Query Language (KQL) overview](docs-content://explore-analyze/query-filter/languages/kql.md).


## Syntax at a glance [kql-syntax-at-a-glance]

Find the kind of condition you need in the table, then follow the link for details and examples.

| To find documents | Syntax | Example |
| --- | --- | --- |
| [Where a field exists](#_filter_for_documents_where_a_field_exists) | `field: *` | `http.request.method: *` |
| [That match a value](#_filter_for_documents_that_match_a_value) | `field: value` | `http.request.method: GET` |
| [That match an exact phrase in a `text` field](#_filter_for_documents_that_match_a_value) | `field: "value"` | `http.request.body.content: "null pointer"` |
| [With a value in any field](#kql-search-without-field-name) | `value` | `Hello` |
| [Within a range](#_filter_for_documents_within_a_range) | `<`, `<=`, `>`, `>=` | `http.response.bytes < 10000` |
| [That match a pattern](#_filter_for_documents_using_wildcards) | `*` | `machine.os: win*` |
| [That don't match a condition](#_negating_a_query) | `NOT` | `NOT http.request.method: GET` |
| [That match several conditions](#_combining_multiple_queries) | `AND`, `OR`, `( )` | `http.request.method: (GET OR POST OR DELETE)` |
| [With a value in one of several fields](#_matching_multiple_fields) | `field.*: value` | `datastream.*: logs` |
| [With a matching nested field](#_querying_nested_fields) | `field:{ condition }` | `user:{ first: "Alice" AND last: "White" }` |


## Match documents where a field exists [_filter_for_documents_where_a_field_exists]

To match documents that have an indexed value for a field, use `*` as the value. For example, documents where `http.request.method` exists:

```yaml
http.request.method: *
```

This matches any indexed value, including an empty string.


## Match documents by value [_filter_for_documents_that_match_a_value]

To match a number, text, date, or boolean value, enter the field name, a colon, and the value. For example, documents where `http.request.method` is GET:

```yaml
http.request.method: GET
```

On `keyword`, numeric, `date`, or `boolean` fields, the value must match exactly, including punctuation and case. To ignore case in `keyword` values, refer to [Differences between {{kib}} and {{es}}](#kql-differences).

On `text` fields, {{es}} analyzes the value using the [field's mapping settings](docs-content://solutions/search/full-text/text-analysis-during-search.md). For example, documents where `http.request.body.content` contains `null pointer`:

```yaml
http.request.body.content: null pointer
```

Because this is a `text` field, the order of the search terms doesn't matter. Documents that contain `pointer null` also match. To match the terms in that order, surround the value with quotation marks:

```yaml
http.request.body.content: "null pointer"
```

### Search without a field name [kql-search-without-field-name]

The field name is optional. If you leave it out, KQL searches a default set of fields. By default, that is every field that supports term queries, except metadata fields and nested fields. To change the fields, refer to [Differences between {{kib}} and {{es}}](#kql-differences). For example, to search for `Hello` in the default fields:

```yaml
Hello
```

### Escape special characters [kql-escape-special-characters]

To use one of these characters in a value, escape it with a backslash, unless you surround the value with quotation marks:

```yaml
\():<>"*{}
```

For example, either of these queries matches documents where `http.request.referrer` is `https://example.com`:

```yaml
http.request.referrer: "https://example.com"
http.request.referrer: https\://example.com
```

Inside quotation marks, escape only the quotation mark and the backslash, as `\"` and `\\`. An asterisk inside quotation marks is a literal asterisk, not a wildcard.

The words `AND`, `OR`, and `NOT` are keywords (not case-sensitive). To use one as a value, escape it with a backslash, as in `\AND`, or surround it with quotation marks. Field names follow the same escaping rules as values.


## Match documents within a range [_filter_for_documents_within_a_range]

To match values in a range, use `<`, `<=`, `>`, or `>=`. For example, documents where `http.response.bytes` is less than 10000:

```yaml
http.response.bytes < 10000
```

For an inclusive range, combine two conditions. For example, documents where `http.response.bytes` is greater than 10000 and less than or equal to 20000:

```yaml
http.response.bytes > 10000 AND http.response.bytes <= 20000
```

On multi-value fields, KQL tests each condition against every value in the array. `number > 300 AND number < 400` matches `"number": [500, 10]`. The value 500 matches the first condition and 10 matches the second. If one value must satisfy every condition, use [Query DSL](/reference/query-languages/query-dsl/query-dsl-range-query.md).

You can also use range syntax with strings, IP addresses, and timestamps. For example, documents with a timestamp earlier than two weeks ago:

```yaml
@timestamp < now-2w
```

A date that includes a time contains a colon, so surround it with quotation marks. For example, documents on or after 10:00 AM on January 1, 2021:

```yaml
@timestamp >= "2021-01-01T10:00:00"
```

To set the time zone that KQL uses to interpret explicit dates, refer to [Differences between {{kib}} and {{es}}](#kql-differences).

For more examples of acceptable date formats, refer to [Date Math](/reference/elasticsearch/rest-apis/common-options.md#date-math).


## Match documents using wildcards [_filter_for_documents_using_wildcards]

To match a pattern, use a wildcard. Wildcards work on string-based fields, such as `keyword`, `text`, `match_only_text`, `constant_keyword`, `flattened`, and `wildcard` fields. On numeric, `date`, `boolean`, and `ip` fields, a wildcard makes the search fail with an error. KQL supports only `*`, which matches zero or more characters.

For example, documents where `machine.os` begins with `win`:

```yaml
machine.os: win*
```

By default, you can put `*` at the start of a pattern. For example, documents where `url` contains `elastic`:

```yaml
url: *elastic*
```

Queries that start with `*` can slow searches.

{applies_to}`serverless: unavailable` In {{kib}}, to avoid slow searches, turn leading wildcards off with the [`query:allowLeadingWildcards`](kibana://reference/advanced-settings.md#query-allowleadingwildcards) advanced setting.


## Exclude documents with a negated query [_negating_a_query]

To exclude documents, use the `NOT` keyword (not case-sensitive). For example, documents where `http.request.method` is not GET:

```yaml
NOT http.request.method: GET
```

To exclude a group of conditions, put them in parentheses. For example, documents where `http.request.method` is neither GET nor POST:

```yaml
NOT (http.request.method: GET OR http.request.method: POST)
```


## Combine multiple queries [_combining_multiple_queries]

To combine queries, use `AND` or `OR` (not case-sensitive). For example, documents where `http.request.method` is GET or `http.response.status_code` is 400:

```yaml
http.request.method: GET OR http.response.status_code: 400
```

To require both conditions, use `AND`. For example, documents where `http.request.method` is GET and `http.response.status_code` is 400:

```yaml
http.request.method: GET AND http.response.status_code: 400
```

### Set the order of conditions [kql-condition-order]

To set precedence, use parentheses. For example, documents where `http.request.method` is GET and `http.response.status_code` is 200, or `http.request.method` is POST and `http.response.status_code` is 400:

```yaml
(http.request.method: GET AND http.response.status_code: 200) OR
(http.request.method: POST AND http.response.status_code: 400)
```

In {{kib}}, without parentheses, `NOT` applies first, then `AND`, then `OR`. For example, this query matches GET requests, and also POST requests with status 400:

```yaml
http.request.method: GET OR http.request.method: POST AND http.response.status_code: 400
```

In the Query DSL `kql` clause and the {{esql}} `KQL` function, `AND` and `OR` have the same precedence and group from right to left. In those places, `a: 1 AND b: 2 OR c: 3` means `a: 1 AND (b: 2 OR c: 3)`. In {{kib}}, it means `(a: 1 AND b: 2) OR c: 3`. To get the same result everywhere, use parentheses.

### Match several values on one field [kql-multiple-values]

To match several values on one field, put them in parentheses. For example, documents where `http.request.method` is GET, POST, or DELETE:

```yaml
http.request.method: (GET OR POST OR DELETE)
```

Inside the parentheses, you can also use `AND` and `NOT`. For example, documents where `tags` contains `production` and doesn't contain `deprecated`:

```yaml
tags: (production AND NOT deprecated)
```


## Match multiple fields [_matching_multiple_fields]

To query several fields at once, use a wildcard in the field name. For example, documents where any subfield of `datastream` contains `logs`:

```yaml
datastream.*: logs
```

The query can fail if the matching fields have different types. For example, if `datastream.*` matches both numeric and string fields, `datastream.*: logs` returns an error because you can't query numeric fields for string values.


## Query nested fields [_querying_nested_fields]

[Nested fields](/reference/elasticsearch/mapping-reference/nested.md) use a special syntax. In this JSON document, `user` is a nested field:

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

To find documents where a single value in the `user` array has the first name `Alice` and the last name `White`:

```yaml
user:{ first: "Alice" AND last: "White" }
```

If nested fields contain other nested fields, use the full path of the field you want to query. In this JSON document, `user` and `names` are both nested fields:

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

To find documents where a single value in the `user.names` array has the first name `Alice` and the last name `White`:

```yaml
user.names:{ first: "Alice" AND last: "White" }
```


## Differences between {{kib}} and {{es}} [kql-differences]

The {{kib}} query bar, the Query DSL `kql` clause, and the {{esql}} `KQL` function accept the same syntax. The following behaviors differ. Operator precedence also differs, as described in [Combine multiple queries](#_combining_multiple_queries).

| Behavior | {{kib}} query bar | Query DSL `kql` clause and {{esql}} `KQL` function |
| --- | --- | --- |
| Ignore case in `keyword` values | Not available | Use the `case_insensitive` option. |
| Time zone for dates | The **Time zone** (`dateFormat:tz`) advanced setting | Use the `time_zone` option. |
| Fields searched when you omit the field name | The [`index.query.default_field`](/reference/elasticsearch/index-settings/index-modules.md#index-query-default-field) index setting | The `default_field` option. If you don't set it, the `index.query.default_field` index setting. |

{applies_to}`stack: ga 9.3+` The {{esql}} `KQL` function accepts these options in its `options` parameter. For the Query DSL `kql` clause, set them as [parameters of the clause](/reference/query-languages/query-dsl/query-dsl-kql-query.md).
