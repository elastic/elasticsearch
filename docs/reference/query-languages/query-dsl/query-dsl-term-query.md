---
navigation_title: "Term"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html
---

# Term query [query-dsl-term-query]


Returns documents that contain an **exact** term in a provided field.

You can use the `term` query to find documents based on a precise value such as a price, a product ID, or a username.

::::{warning}
Avoid using the `term` query for [`text`](/reference/elasticsearch/mapping-reference/text.md) fields.

By default, {{es}} changes the values of `text` fields as part of [analysis](docs-content://manage-data/data-store/text-analysis.md). This can make finding exact matches for `text` field values difficult.

To search `text` field values, use the [`match`](/reference/query-languages/query-dsl/query-dsl-match-query.md) query instead.

::::


## Example request [term-query-ex-request]

```console
GET /_search
{
  "query": {
    "term": {
      "user.id": {
        "value": "kimchy",
        "boost": 1.0
      }
    }
  }
}
```


## Top-level parameters for `term` [term-top-level-params]

`<field>`
:   (Required, object) Field you wish to search.


## Parameters for `<field>` [term-field-params]

`value`
:   (Required, string) Term you wish to find in the provided `<field>`. To return a document, the term must exactly match the field value, including whitespace and capitalization.

`boost`
:   (Optional, float) Floating point number used to decrease or increase the [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of a query. Defaults to `1.0`.

    You can use the `boost` parameter to adjust relevance scores for searches containing two or more queries.

    Boost values are relative to the default value of `1.0`. A boost value between `0` and `1.0` decreases the relevance score. A value greater than `1.0` increases the relevance score.


`case_insensitive`
:   :::{admonition} Added in 7.10.0
    This parameter was added in 7.10.0.
    :::

    (Optional, Boolean) Allows ASCII case insensitive matching of the value with the indexed field values when set to true. Default is false which means the case sensitivity of matching depends on the underlying field’s mapping.


## Notes [term-query-notes]

### Avoid using the `term` query for `text` fields [avoid-term-query-text-fields]

By default, {{es}} changes the values of `text` fields during analysis. For example, the default [standard analyzer](/reference/text-analysis/analysis-standard-analyzer.md) changes `text` field values as follows:

* Removes most punctuation
* Divides the remaining content into individual words, called [tokens](/reference/text-analysis/tokenizer-reference.md)
* Lowercases the tokens

To better search `text` fields, the `match` query also analyzes your provided search term before performing a search. This means the `match` query can search `text` fields for analyzed tokens rather than an exact term.

The `term` query does **not** analyze the search term. The `term` query only searches for the **exact** term you provide. This means the `term` query may return poor or no results when searching `text` fields.

To see the difference in search results, try the following example.

1. Create an index with a `text` field called `full_text`.

    ```console
    PUT my-index-000001
    {
      "mappings": {
        "properties": {
          "full_text": { "type": "text" }
        }
      }
    }
    ```

2. Index a document with a value of `Quick Brown Foxes!` in the `full_text` field.

    ```console
    PUT my-index-000001/_doc/1
    {
      "full_text":   "Quick Brown Foxes!"
    }
    ```

    Because `full_text` is a `text` field, {{es}} changes `Quick Brown Foxes!` to `[quick, brown, fox]` during analysis.

3. Use the `term` query to search for `Quick Brown Foxes!` in the `full_text` field. Include the `pretty` parameter so the response is more readable.

    ```console
    GET my-index-000001/_search?pretty
    {
      "query": {
        "term": {
          "full_text": "Quick Brown Foxes!"
        }
      }
    }
    ```

    Because the `full_text` field no longer contains the **exact** term `Quick Brown Foxes!`, the `term` query search returns no results.

4. Use the `match` query to search for `Quick Brown Foxes!` in the `full_text` field.

    ```console
    GET my-index-000001/_search?pretty
    {
      "query": {
        "match": {
          "full_text": "Quick Brown Foxes!"
        }
      }
    }
    ```

    Unlike the `term` query, the `match` query analyzes your provided search term, `Quick Brown Foxes!`, before performing a search. The `match` query then returns any documents containing the `quick`, `brown`, or `fox` tokens in the `full_text` field.

    Here’s the response for the `match` query search containing the indexed document in the results.

    ```console-result
    {
      "took" : 1,
      "timed_out" : false,
      "_shards" : {
        "total" : 1,
        "successful" : 1,
        "skipped" : 0,
        "failed" : 0
      },
      "hits" : {
        "total" : {
          "value" : 1,
          "relation" : "eq"
        },
        "max_score" : 0.8630463,
        "hits" : [
          {
            "_index" : "my-index-000001",
            "_id" : "1",
            "_score" : 0.8630463,
            "_source" : {
              "full_text" : "Quick Brown Foxes!"
            }
          }
        ]
      }
    }
    ```




