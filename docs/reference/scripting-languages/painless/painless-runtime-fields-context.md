---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-runtime-fields-context.html
---

# Runtime fields context [painless-runtime-fields-context]

Use a Painless script to calculate and emit [runtime field](/reference/scripting-languages/painless/use-painless-scripts-in-runtime-fields.md) values.

See the [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md) documentation for more information about how to use runtime fields.

**Methods**

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


**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
:   Contains the fields of the specified document where each field is a `List` of values.

[`params['_source']`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) (`Map`, read-only)
:   Contains extracted JSON in a `Map` and `List` structure for the fields existing in a stored document.

**Return**

`void`
:   No expected return value.

**API**

Both the standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) and [Specialized Field API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-field.html) are available.

**Example**

To run the examples, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

Then, run the following request to define a runtime field named `day_of_week`. This field contains a script with the same `source` defined in [Field context](/reference/scripting-languages/painless/painless-field-context.md), but also uses an `emit` function that runtime fields require when defining a Painless script.

Because `day_of_week` is a runtime field, it isn’t indexed, and the included script only runs at query time:

```console
PUT seats/_mapping
{
  "runtime": {
    "day_of_week": {
      "type": "keyword",
      "script": {
        "source": "emit(doc['datetime'].value.getDayOfWeekEnum().toString())"
      }
    }
  }
}
```

After defining the runtime field and script in the mappings, you can run a query that includes a terms aggregation for `day_of_week`. When the query runs, {{es}} evaluates the included Painless script and dynamically generates a value based on the script definition:

```console
GET seats/_search
{
  "size": 0,
  "fields": [
    "time",
    "day_of_week"
    ],
    "aggs": {
      "day_of_week": {
        "terms": {
          "field": "day_of_week",
          "size": 10
        }
      }
    }
}
```

The response includes `day_of_week` for each hit. {{es}} calculates the value for this field dynamically at search time by operating on the `datetime` field defined in the mappings.

```console-result
{
  ...
  "hits" : {
    "total" : {
      "value" : 11,
      "relation" : "eq"
    },
    "max_score" : null,
    "hits" : [ ]
  },
  "aggregations" : {
    "day_of_week" : {
      "doc_count_error_upper_bound" : 0,
      "sum_other_doc_count" : 0,
      "buckets" : [
        {
          "key" : "TUESDAY",
          "doc_count" : 5
        },
        {
          "key" : "THURSDAY",
          "doc_count" : 4
        },
        {
          "key" : "MONDAY",
          "doc_count" : 1
        },
        {
          "key" : "SUNDAY",
          "doc_count" : 1
        }
      ]
    }
  }
}
```

