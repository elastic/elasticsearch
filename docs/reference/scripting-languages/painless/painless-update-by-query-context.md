---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-update-by-query-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Update by query context [painless-update-by-query-context]

Use a Painless script in an [update by query](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query) operation to add, modify, or delete fields within each of a set of documents collected as the result of query.

Check out the [document update tutorial](docs-content://explore-analyze/scripting/modules-scripting-document-update-tutorial.md) for related examples.

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`ctx['op']` (`String`)
:   The name of the operation.

[`ctx['_routing']`](/reference/elasticsearch/mapping-reference/mapping-routing-field.md) (`String`, read-only)
:   The value used to select a shard for document storage.

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md) (`String`, read-only)
:   The name of the index.

[`ctx['_id']`](/reference/elasticsearch/mapping-reference/mapping-id-field.md) (`String`, read-only)
:   The unique document id.

`ctx['_version']` (`int`, read-only)
:   The current version of the document.

[`ctx['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) (`Map`)
:   Contains extracted JSON in a `Map` and `List` structure for the fields existing in a stored document.

## Side Effects

`ctx['op']`
:   Use the default of `index` to update a document. Set to `none` to specify no operation or `delete` to delete the current document from the index.

[`ctx['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md)
:   Modify the values in the `Map/List` structure to add, modify, or delete the fields of a document.

## Return

`void`
:   No expected return value.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

The following request uses the `_update_by_query` API to apply a discount to products that meet two conditions:

1. The product’s `base_price` is greater than or equal to 20\.  
2. The product currently has a `discount_percentage` of 0 (no discount applied).

```json
POST /kibana_sample_data_ecommerce/_update_by_query
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "products.base_price": {
              "gte": 20
            }
          }
        },
        {
          "match": {
            "products.discount_percentage": 0
          }
        }
      ]
    }
  },
  "script": {
    "lang": "painless",
    "source": """
        for (product in ctx._source.products) { 
            // If product has no discount applied 
            if (product.discount_percentage == 0) { 
                product.discount_percentage = params.discount_rate;  // Assigns the discount rate
                product.discount_amount = product.base_price * (params.discount_rate / 100) ; // Calculates and assigns the total discount

                product.price = product.base_price - product.discount_amount; // Calculates and assigns the product price with discount
            } 
        }
    """,
    "params": {
      "discount_rate": 15
    }
  }
}
```
