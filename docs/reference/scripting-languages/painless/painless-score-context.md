---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-score-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Score context [painless-score-context]

Use a Painless script in a [function score](/reference/query-languages/query-dsl/query-dsl-function-score-query.md) to apply a new score to documents returned from a query.

For help debugging errors in this context, refer to [Debug script score calculation errors in Painless](docs-content://explore-analyze/scripting/painless-script-score-calculation-errors.md).

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the current document. For single-valued fields, the value can be accessed via `doc['fieldname'].value`. For multi-valued fields, this returns the first value; other values can be accessed via `doc['fieldname'].get(index)`

`_score` (`double` read-only)
:   The similarity score of the current document.

## Return

`double`
:   The score for the current document.

## API

Both the standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) and specialized [Field API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-field.html) are available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

The following request boosts results for customers in New York, helping highlight local preferences or regional inventory. It uses a `function_score` query to match products with "jeans" in the name and increase their score when the city is New York.

```json
GET kibana_sample_data_ecommerce/_search
{
  "query": {
    "function_score": {
      "query": {
        "match": {
          "products.product_name": "jeans"
        }
      },
      "script_score": {
        "script": {
          "source": """
            double baseScore = _score;
            def city = doc['geoip.city_name'];

            if (city.size() > 0 && city.value == 'New York') {
              baseScore *= 1.5;
            }

            return baseScore;
          """
        }
      }
    }
  }
}                        
```
