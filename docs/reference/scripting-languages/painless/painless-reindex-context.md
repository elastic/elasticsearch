---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-reindex-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Reindex context [painless-reindex-context]

Use a Painless script in a [reindex](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex) operation to add, modify, or delete fields within each document in an original index as its reindexed into a target index.

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`ctx['op']` (`String`)
:   The name of the operation.

[`ctx['_routing']`](/reference/elasticsearch/mapping-reference/mapping-routing-field.md) (`String`)
:   The value used to select a shard for document storage.

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md) (`String`)
:   The name of the index.

[`ctx['_id']`](/reference/elasticsearch/mapping-reference/mapping-id-field.md) (`String`)
:   The unique document id.

`ctx['_version']` (`int`)
:   The current version of the document.

[`ctx['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) (`Map`)
:   Contains extracted JSON in a `Map` and `List` structure for the fields existing in a stored document.

## Side Effects

`ctx['op']`
:   Use the default of `index` to update a document. Set to `noop` to specify no operation or `delete` to delete the current document from the index.

[`ctx['_routing']`](/reference/elasticsearch/mapping-reference/mapping-routing-field.md)
:   Modify this to change the routing value for the current document.

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md)
:   Modify this to change the destination index for the current document.

[`ctx['_id']`](/reference/elasticsearch/mapping-reference/mapping-id-field.md)
:   Modify this to change the id for the current document.

`ctx['_version']` (`int`)
:   Modify this to modify the version for the current document.

[`ctx['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md)
:   Modify the values in the `Map/List` structure to add, modify, or delete the fields of a document.

## Return

`void`
:   No expected return value.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

The following request copies all documents from the `kibana_sample_data_ecommerce` index to a new index called `data_ecommerce_usd`. During this process, a script is used to convert all monetary values to a new currency using a specified exchange rate in the `params`.

```json
POST /_reindex
{
  "source": {
    "index": "kibana_sample_data_ecommerce"
  },
  "dest": {
    "index": "data_ecommerce_usd"
  },
  "script": {
    "source": """
      float er = (float) params.exchange_rate; // Cast parameter to float to ensure proper decimal calculations
      ctx._source.currency = params.target_currency;

      // Convert all prices to dollar
      ctx._source.taxful_total_price = Math.round(ctx._source.taxful_total_price * er * 100) / 100;
      ctx._source.taxless_total_price = Math.round(ctx._source.taxless_total_price * er * 100) / 100;
      
      for (product in ctx._source.products) {
        product.price = Math.round(product.price * er * 100) / 100;
        product.base_price = Math.round(product.base_price * er * 100) / 100;
        product.taxful_price = Math.round(product.taxful_price * er * 100) / 100;
        product.taxless_price = Math.round(product.taxless_price * er * 100) / 100;
      }
    """,
    "lang": "painless",
    "params": {
      "exchange_rate": 1.10,
      "target_currency": "USD"
    }
  }
}
```
