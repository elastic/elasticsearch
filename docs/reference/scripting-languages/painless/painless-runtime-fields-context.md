---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-runtime-fields-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Runtime fields context [painless-runtime-fields-context]

Use a Painless script to calculate and emit [runtime field](/reference/scripting-languages/painless/use-painless-scripts-in-runtime-fields.md) values.

Runtime fields are dynamic fields that are calculated at query time rather than being indexed. This approach provides flexibility for data exploration and field creation without requiring reindexing, though it comes with performance trade-offs compared to indexed fields.

For comprehensive information about runtime field implementation and use cases, refer to the [runtime fields documentation](docs-content://manage-data/data-store/mapping/runtime-fields.md). You can also check the troubleshooting guide for help with runtime field exceptions.

## Methods

$$$runtime-emit-method$$$

`emit`
:   (Required) Accepts the values from the script valuation. Scripts can call the `emit` method multiple times to emit multiple values.

    The `emit` method applies only to scripts used in a [runtime fields context](/reference/scripting-languages/painless/painless-api-examples.md#painless-execute-runtime-context).

    ::::{important}
    The `emit` method cannot accept `null` values. Do not call this method if the referenced fields do not have any values.
    ::::


  ::::{dropdown} Signatures of emit
  The signature for `emit` depends on the `type` of the field.

  `boolean`
  :   `emit(boolean)`

  `date`
  :   `emit(long)`

  `double`
  :   `emit(double)`

  `geo_point`
  :   `emit(double lat, double lon)`

  `ip`
  :   `emit(String)`

  `long`
  :   `emit(long)`

  `keyword`
  :   `emit(String)`

  ::::


`grok`
:   Defines a [grok pattern](/reference/enrich-processor/grok-processor.md) to extract structured fields out of a single text field within a document. A grok pattern is like a regular expression that supports aliased expressions that can be reused. See [Define a runtime field with a grok pattern](docs-content://manage-data/data-store/mapping/explore-data-with-runtime-fields.md#runtime-examples-grok).

    ::::{dropdown} Properties of grok
    `extract`
    :   Indicates the values to return. This method applies only to `grok` and `dissect` methods.
    ::::


`dissect`
:   Defines a [dissect pattern](/reference/enrich-processor/dissect-processor.md). Dissect operates much like grok, but does not accept regular expressions. See [Define a runtime field with a dissect pattern](docs-content://manage-data/data-store/mapping/explore-data-with-runtime-fields.md#runtime-examples-dissect).

    ::::{dropdown} Properties of dissect
    `extract`
    :   Indicates the values to return. This method applies only to `grok` and `dissect` methods.
    ::::


## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the specified document where each field is a `List` of values.

[`params['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) (`Map`, read-only)
:   Contains extracted JSON in a `Map` and `List` structure for the fields existing in a stored document.

## Return

`void`
:   No expected return value.

## API

Both the standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) and specialized [Field API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-field.html) are available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

Run the following request to define a runtime field named `full_day_name`. This field contains a script that extracts the day of the week from the `order_date` field and assigns the full day name using the `dayOfWeekEnum` enumeration. The script uses the `emit` function, which is required for runtime fields.

Because `full_day_name` is a runtime field, it isn’t indexed, and the script runs dynamically at query time:

```json
PUT kibana_sample_data_ecommerce/_mapping
{
  "runtime": {
    "full_day_name": {
      "type": "keyword",
      "script": {
        "source": """emit(doc['order_date'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT));
        """
      }
    }
  }
}
```

After defining the runtime field, you can run a query that includes a `terms` aggregation for `full_day_name`. At search time, {{es}} executes the script to dynamically calculate the value for each document:

```json
GET kibana_sample_data_ecommerce/_search
{
  "size": 0,
  "aggs": {
    "full_day_name": {
      "terms": {
        "field": "full_day_name",
        "size": 10
      }
    }
  }
}
```

The response includes an aggregation bucket for each time period. {{es}} calculates the value of the `full_day_name` field dynamically at search time, based on the `order_date` field in each document.

Response:

```json
{
  ...
  "hits": {
    "total": {
      "value": 4675,
      "relation": "eq"
    },
    "max_score": null,
    "hits": []
  },
  "aggregations": {
    "full_day_name": {
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 0,
      "buckets": [
        {
          "key": "Thu",
          "doc_count": 775
        },
        {
          "key": "Fri",
          "doc_count": 770
        },
        {
          "key": "Sat",
          "doc_count": 736
        },
        {
          "key": "Sun",
          "doc_count": 614
        },
        {
          "key": "Tue",
          "doc_count": 609
        },
        {
          "key": "Wed",
          "doc_count": 592
        },
        {
          "key": "Mon",
          "doc_count": 579
        }
      ]
    }
  }
}
```
