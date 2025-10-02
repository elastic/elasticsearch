---
navigation_title: "Prefix"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html
---

# Prefix query [query-dsl-prefix-query]


Returns documents that contain a specific prefix in a provided field.

## Example request [prefix-query-ex-request]

The following search returns documents where the `user.id` field contains a term that begins with `ki`.

```console
GET /_search
{
  "query": {
    "prefix": {
      "user.id": {
        "value": "ki"
      }
    }
  }
}
```


## Top-level parameters for `prefix` [prefix-query-top-level-params]

`<field>`
:   (Required, object) Field you wish to search.


## Parameters for `<field>` [prefix-query-field-params]

`value`
:   (Required, string) Beginning characters of terms you wish to find in the provided `<field>`.

`rewrite`
:   (Optional, string) Method used to rewrite the query. For valid values and more information, see the [`rewrite` parameter](/reference/query-languages/query-dsl/query-dsl-multi-term-rewrite.md).

`case_insensitive`
:   :::{admonition} Added in 7.10.0
    This parameter was added in 7.10.0.
    :::

    (Optional, Boolean) Allows ASCII case insensitive matching of the value with the indexed field values when set to true. Default is false which means the case sensitivity of matching depends on the underlying field’s mapping.


## Notes [prefix-query-notes]

### Short request example [prefix-query-short-ex]

You can simplify the `prefix` query syntax by combining the `<field>` and `value` parameters. For example:

```console
GET /_search
{
  "query": {
    "prefix" : { "user" : "ki" }
  }
}
```


### Speed up prefix queries [prefix-query-index-prefixes]

You can speed up prefix queries using the [`index_prefixes`](/reference/elasticsearch/mapping-reference/index-prefixes.md) mapping parameter. If enabled, {{es}} indexes prefixes in a separate field, according to the configuration settings. This lets {{es}} run prefix queries more efficiently at the cost of a larger index.


### Allow expensive queries [prefix-query-allow-expensive-queries]

Prefix queries will not be executed if [`search.allow_expensive_queries`](/reference/query-languages/querydsl.md#query-dsl-allow-expensive-queries) is set to false. However, if [`index_prefixes`](/reference/elasticsearch/mapping-reference/index-prefixes.md) are enabled, an optimised query is built which is not considered slow, and will be executed in spite of this setting.



