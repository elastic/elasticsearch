---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-sort-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Sort context [painless-sort-context]

Use a Painless script to [sort](/reference/elasticsearch/rest-apis/sort-search-results.md) the documents in a query.

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the current document. For single-valued fields, the value can be accessed via `doc['fieldname'].value`. For multi-valued fields, this returns the first value; other values can be accessed via `doc['fieldname'].get(index)`

`_score` (`double` read-only)
:   The similarity score of the current document.

## Return

`double` or `String`
:   The sort key. The return type depends on the value of the `type` parameter in the script sort config (`"number"` or `"string"`).

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

The following request sorts documents by the average price per item, calculated by dividing the `taxful_total_price` by the `total_quantity`.

Documents with a higher average item price appear at the top of the results.

```json
GET /kibana_sample_data_ecommerce/_search
{
  "sort": {
    "_script": {
      "type": "number",
      "script": {
        "lang": "painless",
        "source": """
            doc['taxful_total_price'].value / doc['total_quantity'].value
        """
      },
      "order": "desc"
    }
  }
}
```
