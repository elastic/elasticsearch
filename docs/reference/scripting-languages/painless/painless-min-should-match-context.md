---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-min-should-match-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Minimum should match context [painless-min-should-match-context]

Use a Painless script to specify the [minimum](/reference/query-languages/query-dsl/query-dsl-terms-set-query.md) number of terms that a specified field needs to match with for a document to be part of the query results.

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`params['num_terms']` (`int`, read-only)
:   The number of terms specified to match with.

`doc` (`Map`, read-only)
:   Contains the fields of the current document where each field is a `List` of values.

## Return

`int`
:   The minimum number of terms required to match the current document.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

This example shows conditional matching requirements. The script checks if a document contains both “Men’s Clothing” and “Men’s Shoes” categories. If this combination exists, only two terms need to match. Otherwise, three terms must match. This creates different qualification thresholds based on document content.

```json
GET kibana_sample_data_ecommerce/_search
{
  "query": {
    "terms_set": {
      "category.keyword": {
        "terms": [
          "Men's Clothing",
          "Men's Shoes", 
          "Men's Accessories",
          "Women's Clothing"
        ],
        "minimum_should_match_script": {
          "source": """
            boolean hasMensClothing = false;
            boolean hasMensShoes = false;
            
            for (def category : doc['category.keyword']) {
              if (category.equals("Men's Clothing")) {
                hasMensClothing = true;
              }

              if (category.equals("Men's Shoes")) {
                hasMensShoes = true;
              }
            }
            
            if (hasMensClothing && hasMensShoes) {
              return 2;
            }
            
            return 3;
          """
        }
      }
    }
  }
}
```
