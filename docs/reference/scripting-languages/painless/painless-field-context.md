---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-field-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Field context [painless-field-context]

Use a Painless script to create a [script field](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#script-fields) to return a customized value for each document in the results of a query.

:::{tip}
To create dynamic fields with more capabilities, consider using [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md) instead. Runtime fields can be used in the query phase to select documents and in aggregations, while script fields only work during the fetch phase to decorate already selected results. Note, however, that runtime fields are typically processed much more slowly than script fields.
:::

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the specified document where each field is a `List` of values.

[`params['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) (`Map`, read-only)
:   Contains extracted JSON in a `Map` and `List` structure for the fields existing in a stored document.

## Return

`Object`
:   The customized value for each document.

## API

Both the standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) and specialized [Field API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-field.html) are available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

The first script extracts the day index from the `day_of_week_i` field to determine whether the order was placed on a weekday or during the weekend:

```java
doc['day_of_week_i'].value >= 5 ? 'Weekend' : 'Weekday'
```

The second script evaluates the `total_quantity` field to classify the order size:

```java
long tq = doc['total_quantity'].value;

if (tq == 1) return 'Single Item';
if (tq <= 3) return 'Small Order';
if (tq <= 6) return 'Medium Order';

return 'Large Order';
```

The following request uses both scripts to categorize each order by the day it was placed and by its size:

```json
GET kibana_sample_data_ecommerce/_search
{
  "size": 1,
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "is_weekend_shopper": {
      "script": {
        "source": "doc['day_of_week_i'].value >= 5 ? 'Weekend' : 'Weekday'"
      }
    },
    "order_size_category": {
      "script": {
        "source": """
          long qty = doc['total_quantity'].value;

          if (qty == 1) return 'Single Item';
          if (qty <= 3) return 'Small Order';
          if (qty <= 6) return 'Medium Order';

          return 'Large Order';
        """
      }
    }
  },
  "fields": ["day_of_week_i", "total_quantity"]
}
```

Response:

```json

 {
  "took": 0,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 4675,
      "relation": "eq"
    },
    "max_score": 1,
    "hits": [
      {
        "_index": "kibana_sample_data_ecommerce",
        "_id": "ZkUZjJgBMSQotAoT_Jcg",
        "_score": 1,
        "fields": {
          "order_size_category": [
            "Small Order"
          ],
          "is_weekend_shopper": [
            "Weekday"
          ],
          "day_of_week_i": [
            4
          ],
          "total_quantity": [
            2
          ]
        }
      }
    ]
  }
}
```

