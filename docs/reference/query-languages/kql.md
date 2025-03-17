---
mapped_pages:
  - https://www.elastic.co/guide/en/kibana/current/kuery-query.html
---

# {{kib}} Query Language [kuery-query]

:::{note}
This section provides detailed **reference information**.

Refer to [KQL overview](docs-content://explore-analyze/query-filter/languages/kql.md) in the **Explore and analyze** section for overview and conceptual information about the SQL query language.
:::


The {{kib}} Query Language (KQL) is a simple text-based query language for filtering data.

* KQL only filters data, and has no role in aggregating, transforming, or sorting data.
* KQL is not to be confused with the [Lucene query language](docs-content://explore-analyze/query-filter/languages/lucene-query-syntax.md), which has a different feature set.

Use KQL to filter documents where a value for a field exists, matches a given value, or is within a given range.


## Filter for documents where a field exists [_filter_for_documents_where_a_field_exists]

To filter documents for which an indexed value exists for a given field, use the `*` operator. For example, to filter for documents where the `http.request.method` field exists, use the following syntax:

```yaml
http.request.method: *
```

This checks for any indexed value, including an empty string.


## Filter for documents that match  a value [_filter_for_documents_that_match_a_value]

Use KQL to filter for documents that match a specific number, text, date, or boolean value. For example, to filter for documents where the `http.request.method` is GET, use the following query:

```yaml
http.request.method: GET
```

The field parameter is optional. If not provided, all fields are searched for the given value. For example, to search all fields for “Hello”, use the following:

```yaml
Hello
```

When querying keyword, numeric, date, or boolean fields, the value must be an exact match, including punctuation and case. However, when querying text fields, {{es}} analyzes the value provided according to the [field’s mapping settings](docs-content://manage-data/data-store/text-analysis.md). For example, to search for documents where `http.request.body.content` (a `text` field) contains the text “null pointer”:

```yaml
http.request.body.content: null pointer
```

Because this is a `text` field, the order of these search terms does not matter, and even documents containing “pointer null” are returned. To search `text` fields where the terms are in the order provided, surround the value in quotation marks, as follows:

```yaml
http.request.body.content: "null pointer"
```

Certain characters must be escaped by a backslash (unless surrounded by quotes). For example, to search for documents where `http.request.referrer` is [https://example.com](https://example.com), use either of the following queries:

```yaml
http.request.referrer: "https://example.com"
http.request.referrer: https\://example.com
```

You must escape following characters:

```yaml
\():<>"*
```


## Filter for documents within a range [_filter_for_documents_within_a_range]

To search documents that contain terms within a provided range, use KQL’s range syntax. For example, to search for all documents for which `http.response.bytes` is less than 10000, use the following syntax:

```yaml
http.response.bytes < 10000
```

To search for an inclusive range, combine multiple range queries. For example, to search for documents where `http.response.bytes` is greater than 10000 but less than or equal to 20000, use the following syntax:

```yaml
http.response.bytes > 10000 and http.response.bytes <= 20000
```

You can also use range syntax for string values, IP addresses, and timestamps. For example, to search for documents earlier than two weeks ago, use the following syntax:

```yaml
@timestamp < now-2w
```

For more examples on acceptable date formats, refer to [Date Math](/reference/elasticsearch/rest-apis/common-options.md#date-math).


## Filter for documents using wildcards [_filter_for_documents_using_wildcards]

To search for documents matching a pattern, use the wildcard syntax. For example, to find documents where `http.response.status_code` begins with a 4, use the following syntax:

```yaml
http.response.status_code: 4*
```

By default, leading wildcards are not allowed for performance reasons. You can modify this with the [`query:allowLeadingWildcards`](kibana://reference/advanced-settings.md#query-allowleadingwildcards) advanced setting.

::::{note}
Only `*` is currently supported. This matches zero or more characters.
::::



## Negating a query [_negating_a_query]

To negate or exclude a set of documents, use the `not` keyword (not case-sensitive). For example, to filter documents where the `http.request.method` is **not** GET, use the following query:

```yaml
NOT http.request.method: GET
```


## Combining multiple queries [_combining_multiple_queries]

To combine multiple queries, use the `and`/`or` keywords (not case-sensitive). For example, to find documents where the `http.request.method` is GET **or** the `http.response.status_code` is 400, use the following query:

```yaml
http.request.method: GET OR http.response.status_code: 400
```

Similarly, to find documents where the `http.request.method` is GET **and** the `http.response.status_code` is 400, use this query:

```yaml
http.request.method: GET AND http.response.status_code: 400
```

To specify precedence when combining multiple queries, use parentheses. For example, to find documents where the `http.request.method` is GET **and** the `http.response.status_code` is 200, **or** the `http.request.method` is POST **and** `http.response.status_code` is 400, use the following:

```yaml
(http.request.method: GET AND http.response.status_code: 200) OR
(http.request.method: POST AND http.response.status_code: 400)
```

You can also use parentheses for shorthand syntax when querying multiple values for the same field. For example, to find documents where the `http.request.method` is GET, POST, **or** DELETE, use the following:

```yaml
http.request.method: (GET OR POST OR DELETE)
```


## Matching multiple fields [_matching_multiple_fields]

Wildcards can also be used to query multiple fields. For example, to search for documents where any sub-field of `datastream` contains “logs”, use the following:

```yaml
datastream.*: logs
```

::::{note}
When using wildcards to query multiple fields, errors might occur if the fields are of different types. For example, if `datastream.*` matches both numeric and string fields, the above query will result in an error because numeric fields cannot be queried for string values.
::::



## Querying nested fields [_querying_nested_fields]

Querying [nested fields](/reference/elasticsearch/mapping-reference/nested.md) requires a special syntax. Consider the following document, where `user` is a nested field:

```yaml
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

To find documents where a single value inside the `user` array contains a first name of “Alice” and last name of “White”, use the following:

```yaml
user:{ first: "Alice" and last: "White" }
```

Because nested fields can be inside other nested fields, you must specify the full path of the nested field you want to query. For example, consider the following document where `user` and `names` are both nested fields:

```yaml
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

To find documents where a single value inside the `user.names` array contains a first name of “Alice” **and** last name of “White”, use the following:

```yaml
user.names:{ first: "Alice" and last: "White" }
```

