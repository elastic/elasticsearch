---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-analysis-predicate-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Analysis Predicate Context [painless-analysis-predicate-context]

Use a painless script to determine whether or not the current token in an analysis chain matches a predicate.

:::{tip}
This is an advanced feature for customizing text analysis and token filtering. To learn more, refer to [Text analysis](docs-content://manage-data/data-store/text-analysis.md).
:::

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`token.term` (`CharSequence`, read-only)
:   The characters of the current token

`token.position` (`int`, read-only)
:   The position of the current token

`token.positionIncrement` (`int`, read-only)
:   The position increment of the current token

`token.positionLength` (`int`, read-only)
:   The position length of the current token

`token.startOffset` (`int`, read-only)
:   The start offset of the current token

`token.endOffset` (`int`, read-only)
:   The end offset of the current token

`token.type` (`String`, read-only)
:   The type of the current token

`token.keyword` (`boolean`, read-only)
:   Whether or not the current token is marked as a keyword

## Return

`boolean`
:   Whether or not the current token matches the predicate

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

This example ensures that only properly formatted SKUs are processed. The analyzer checks that SKU codes start with "ZO" and are exactly thirteen characters long, filtering out malformed or invalid product codes that could break inventory systems or search functionality.

```json
PUT sku_analysis
{
  "settings": {
    "analysis": {
      "analyzer": {
        "sku_search_analyzer": {
          "tokenizer": "whitespace",
          "filter": [
            "sku_validator"
          ]
        }
      },
      "filter": {
        "sku_validator": {
          "type": "predicate_token_filter",
          "script": {
            "source": """
              return token.term.length() == 12 && token.term.toString().startsWith('ZO');
            """
          }
        }
      }
    }
  }
}
```

Test it using the following request:

```json
GET sku_analysis/_analyze
{
  "analyzer": "sku_search_analyzer",
  "text": "ZO0240302403 ZO0236402364 INVALID123 SHORT AB1234567890123"
}
```

Result:

```json
{
  "tokens": [
    {
      "token": "ZO0240302403",
      "start_offset": 0,
      "end_offset": 12,
      "type": "word",
      "position": 0
    },
    {
      "token": "ZO0236402364",
      "start_offset": 13,
      "end_offset": 25,
      "type": "word",
      "position": 1
    }
  ]
}
```

