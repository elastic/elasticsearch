---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-update-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Update context [painless-update-context]

Use a Painless script in an [update](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) operation to add, modify, or delete fields within a single document.

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

`ctx['_now']` (`long`, read-only)
:   The current timestamp in milliseconds.

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

The following example demonstrates how to apply a promotional discount to an order. 

Find a valid document ID:

```json
GET /kibana_sample_data_ecommerce/_search
{
  "size": 1,
  "_source": false
}
```

The script performs two key actions:

1. Calculates a discounted price by subtracting $5.00 from the original `taxful_total_price.`  
2. Adds the flag `discount_applied` to identify orders that received promotional pricing.

```json
POST /kibana_sample_data_ecommerce/_update/YOUR_DOCUMENT_ID
{
  "script": {
    "lang": "painless",
    "source": """
      ctx._source.discount_applied = true; 
      ctx._source.final_price = ctx._source.taxful_total_price - params.discount_amount;
      """,
    "params": {
      "discount_amount": 5.00
    }
  }
}
```
