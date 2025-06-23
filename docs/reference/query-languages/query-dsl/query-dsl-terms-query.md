---
navigation_title: "Terms"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
---

# Terms query [query-dsl-terms-query]


Returns documents that contain one or more **exact** terms in a provided field.

The `terms` query is the same as the [`term` query](/reference/query-languages/query-dsl/query-dsl-term-query.md), except you can search for multiple values. A document will match if it contains at least one of the terms. To search for documents that contain more than one matching term, use the [`terms_set` query](/reference/query-languages/query-dsl/query-dsl-terms-set-query.md).

## Example request [terms-query-ex-request]

The following search returns documents where the `user.id` field contains `kimchy` or `elkbee`.

```console
GET /_search
{
  "query": {
    "terms": {
      "user.id": [ "kimchy", "elkbee" ],
      "boost": 1.0
    }
  }
}
```


## Top-level parameters for `terms` [terms-top-level-params]

`<field>`
:   (Optional, object) Field you wish to search.

The value of this parameter is an array of terms you wish to find in the provided field. To return a document, one or more terms must exactly match a field value, including whitespace and capitalization.

By default, {{es}} limits the `terms` query to a maximum of 65,536 terms. You can change this limit using the [`index.max_terms_count`](/reference/elasticsearch/index-settings/index-modules.md#index-max-terms-count) setting.

::::{note}
To use the field values of an existing document as search terms, use the [terms lookup](#query-dsl-terms-lookup) parameters.
::::



`boost`
:   (Optional, float) Floating point number used to decrease or increase the [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of a query. Defaults to `1.0`.

You can use the `boost` parameter to adjust relevance scores for searches containing two or more queries.

Boost values are relative to the default value of `1.0`. A boost value between `0` and `1.0` decreases the relevance score. A value greater than `1.0` increases the relevance score.



## Notes [terms-query-notes]

### Highlighting `terms` queries [query-dsl-terms-query-highlighting]

[Highlighting](/reference/elasticsearch/rest-apis/highlighting.md) is best-effort only. {{es}} may not return highlight results for `terms` queries depending on:

* Highlighter type
* Number of terms in the query


### Terms lookup [query-dsl-terms-lookup]

Terms lookup fetches the field values of an existing document. {{es}} then uses those values as search terms. This can be helpful when searching for a large set of terms.

To run a terms lookup, the field’s [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) must be enabled. You cannot use {{ccs}} to run a terms lookup on a remote index.

::::{note}
By default, {{es}} limits the `terms` query to a maximum of 65,536 terms. This includes terms fetched using terms lookup. You can change this limit using the [`index.max_terms_count`](/reference/elasticsearch/index-settings/index-modules.md#index-max-terms-count) setting.
::::


To reduce network traffic, a terms lookup will fetch the document’s values from a shard on a local data node if possible. If the your terms data is not large, consider using an index with a single primary shard that’s fully replicated across all applicable data nodes to minimize network traffic.

To perform a terms lookup, use the following parameters.

#### Terms lookup parameters [query-dsl-terms-lookup-params]

`index`
:   (Required, string) Name of the index from which to fetch field values.

`id`
:   (Required, string) [ID](/reference/elasticsearch/mapping-reference/mapping-id-field.md) of the document from which to fetch field values.

`path`
:   (Required, string) Name of the field from which to fetch field values. {{es}} uses these values as search terms for the query.

If the field values include an array of nested inner objects, you can access those objects using dot notation syntax.


`routing`
:   (Optional, string) Custom [routing value](/reference/elasticsearch/mapping-reference/mapping-routing-field.md) of the document from which to fetch term values. If a custom routing value was provided when the document was indexed, this parameter is required.


#### Terms lookup example [query-dsl-terms-lookup-example]

To see how terms lookup works, try the following example.

1. Create an index with a `keyword` field named `color`.

    ```console
    PUT my-index-000001
    {
      "mappings": {
        "properties": {
          "color": { "type": "keyword" }
        }
      }
    }
    ```

2. Index a document with an ID of 1 and values of `["blue", "green"]` in the `color` field.

    ```console
    PUT my-index-000001/_doc/1
    {
      "color":   ["blue", "green"]
    }
    ```

3. Index another document with an ID of 2 and value of `blue` in the `color` field.

    ```console
    PUT my-index-000001/_doc/2
    {
      "color":   "blue"
    }
    ```

4. Use the `terms` query with terms lookup parameters to find documents containing one or more of the same terms as document 2. Include the `pretty` parameter so the response is more readable.

    ```console
    GET my-index-000001/_search?pretty
    {
      "query": {
        "terms": {
            "color" : {
                "index" : "my-index-000001",
                "id" : "2",
                "path" : "color"
            }
        }
      }
    }
    ```

    Because document 2 and document 1 both contain `blue` as a value in the `color` field, {{es}} returns both documents.

    ```console-result
    {
      "took" : 17,
      "timed_out" : false,
      "_shards" : {
        "total" : 1,
        "successful" : 1,
        "skipped" : 0,
        "failed" : 0
      },
      "hits" : {
        "total" : {
          "value" : 2,
          "relation" : "eq"
        },
        "max_score" : 1.0,
        "hits" : [
          {
            "_index" : "my-index-000001",
            "_id" : "1",
            "_score" : 1.0,
            "_source" : {
              "color" : [
                "blue",
                "green"
              ]
            }
          },
          {
            "_index" : "my-index-000001",
            "_id" : "2",
            "_score" : 1.0,
            "_source" : {
              "color" : "blue"
            }
          }
        ]
      }
    }
    ```





