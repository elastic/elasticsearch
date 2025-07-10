---
navigation_title: "Exists"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html
---

# Exists query [query-dsl-exists-query]


Returns documents that contain an indexed value for a field.

An indexed value may not exist for a document’s field due to a variety of reasons:

* The field in the source JSON is `null` or `[]`
* The field has `"index" : false` and `"doc_values" : false` set in the mapping
* The length of the field value exceeded an `ignore_above` setting in the mapping
* The field value was malformed and `ignore_malformed` was defined in the mapping

## Example request [exists-query-ex-request]

```console
GET /_search
{
  "query": {
    "exists": {
      "field": "user"
    }
  }
}
```


## Top-level parameters for `exists` [exists-query-top-level-params]

`field`
:   (Required, string) Name of the field you wish to search.

    While a field is deemed non-existent if the JSON value is `null` or `[]`, these values will indicate the field does exist:

    * Empty strings, such as `""` or `"-"`
    * Arrays containing `null` and another value, such as `[null, "foo"]`
    * A custom [`null-value`](/reference/elasticsearch/mapping-reference/null-value.md), defined in field mapping



## Notes [exists-query-notes]

### Find documents missing indexed values [find-docs-null-values]

To find documents that are missing an indexed value for a field, use the `must_not` [boolean query](/reference/query-languages/query-dsl/query-dsl-bool-query.md) with the `exists` query.

The following search returns documents that are missing an indexed value for the `user.id` field.

```console
GET /_search
{
  "query": {
    "bool": {
      "must_not": {
        "exists": {
          "field": "user.id"
        }
      }
    }
  }
}
```



