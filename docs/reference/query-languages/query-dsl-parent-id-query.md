---
navigation_title: "Parent ID"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-parent-id-query.html
---

# Parent ID query [query-dsl-parent-id-query]


Returns child documents [joined](/reference/elasticsearch/mapping-reference/parent-join.md) to a specific parent document. You can use a [join](/reference/elasticsearch/mapping-reference/parent-join.md) field mapping to create parent-child relationships between documents in the same index.

## Example request [parent-id-query-ex-request]

### Index setup [parent-id-index-setup]

To use the `parent_id` query, your index must include a [join](/reference/elasticsearch/mapping-reference/parent-join.md) field mapping. To see how you can set up an index for the `parent_id` query, try the following example.

1. Create an index with a [join](/reference/elasticsearch/mapping-reference/parent-join.md) field mapping.

    ```console
    PUT /my-index-000001
    {
      "mappings": {
        "properties": {
          "my-join-field": {
            "type": "join",
            "relations": {
              "my-parent": "my-child"
            }
          }
        }
      }
    }
    ```

2. Index a parent document with an ID of `1`.

    ```console
    PUT /my-index-000001/_doc/1?refresh
    {
      "text": "This is a parent document.",
      "my-join-field": "my-parent"
    }
    ```

3. Index a child document of the parent document.

    ```console
    PUT /my-index-000001/_doc/2?routing=1&refresh
    {
      "text": "This is a child document.",
      "my-join-field": {
        "name": "my-child",
        "parent": "1"
      }
    }
    ```



### Example query [parent-id-query-ex-query]

The following search returns child documents for a parent document with an ID of `1`.

```console
GET /my-index-000001/_search
{
  "query": {
      "parent_id": {
          "type": "my-child",
          "id": "1"
      }
  }
}
```



## Top-level parameters for `parent_id` [parent-id-top-level-params]

`type`
:   (Required, string) Name of the child relationship mapped for the [join](/reference/elasticsearch/mapping-reference/parent-join.md) field.

`id`
:   (Required, string) ID of the parent document. The query will return child documents of this parent document.

`ignore_unmapped`
:   (Optional, Boolean) Indicates whether to ignore an unmapped `type` and not return any documents instead of an error. Defaults to `false`.

If `false`, {{es}} returns an error if the `type` is unmapped.

You can use this parameter to query multiple indices that may not contain the `type`.



