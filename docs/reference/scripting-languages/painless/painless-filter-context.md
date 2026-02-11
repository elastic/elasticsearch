---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-filter-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Filter context [painless-filter-context]

Use a Painless script as a [filter](/reference/query-languages/query-dsl/query-dsl-script-query.md) in a query to include and exclude documents.

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the current document where each field is a `List` of values.

## Return

`boolean`
:   Return `true` if the current document should be returned as a result of the query, and `false` otherwise.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

This example filters documents where the average price per item in an order exceeds a minimum threshold of 30\.

:::{tip}
Filters answer the question “Does this document match?” with a simple “Yes or No” response. Use filter context for all conditions that don’t need scoring. Refer to [Query and filter context](/reference/query-languages/query-dsl/query-filter-context.md) to learn more.
:::

```json
GET kibana_sample_data_ecommerce/_search
{
  "query": {
    "bool": {
      "filter": {
        "script": {
          "script": {
            "source": """
                (doc['taxful_total_price'].value / doc['total_quantity'].value) > params.min_avg_price
            """,
            "params": {
              "min_avg_price": 30
            }
          }
        }
      }
    }
  }
}
```
