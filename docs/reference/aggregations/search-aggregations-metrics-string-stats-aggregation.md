---
navigation_title: "String stats"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-string-stats-aggregation.html
---

# String stats aggregation [search-aggregations-metrics-string-stats-aggregation]


A `multi-value` metrics aggregation that computes statistics over string values extracted from the aggregated documents. These values can be retrieved either from specific `keyword` fields.

The string stats aggregation returns the following results:

* `count` - The number of non-empty fields counted.
* `min_length` - The length of the shortest term.
* `max_length` - The length of the longest term.
* `avg_length` - The average length computed over all terms.
* `entropy` - The [Shannon Entropy](https://en.wikipedia.org/wiki/Entropy_(information_theory)) value computed over all terms collected by the aggregation. Shannon entropy quantifies the amount of information contained in the field. It is a very useful metric for measuring a wide range of properties of a data set, such as diversity, similarity, randomness etc.

For example:

```console
POST /my-index-000001/_search?size=0
{
  "aggs": {
    "message_stats": { "string_stats": { "field": "message.keyword" } }
  }
}
```

The above aggregation computes the string statistics for the `message` field in all documents. The aggregation type is `string_stats` and the `field` parameter defines the field of the documents the stats will be computed on. The above will return the following:

```console-result
{
  ...

  "aggregations": {
    "message_stats": {
      "count": 5,
      "min_length": 24,
      "max_length": 30,
      "avg_length": 28.8,
      "entropy": 3.94617750050791
    }
  }
}
```

The name of the aggregation (`message_stats` above) also serves as the key by which the aggregation result can be retrieved from the returned response.

## Character distribution [_character_distribution]

The computation of the Shannon Entropy value is based on the probability of each character appearing in all terms collected by the aggregation. To view the probability distribution for all characters, we can add the `show_distribution` (default: `false`) parameter.

```console
POST /my-index-000001/_search?size=0
{
  "aggs": {
    "message_stats": {
      "string_stats": {
        "field": "message.keyword",
        "show_distribution": true  <1>
      }
    }
  }
}
```

1. Set the `show_distribution` parameter to `true`, so that probability distribution for all characters is returned in the results.


```console-result
{
  ...

  "aggregations": {
    "message_stats": {
      "count": 5,
      "min_length": 24,
      "max_length": 30,
      "avg_length": 28.8,
      "entropy": 3.94617750050791,
      "distribution": {
        " ": 0.1527777777777778,
        "e": 0.14583333333333334,
        "s": 0.09722222222222222,
        "m": 0.08333333333333333,
        "t": 0.0763888888888889,
        "h": 0.0625,
        "a": 0.041666666666666664,
        "i": 0.041666666666666664,
        "r": 0.041666666666666664,
        "g": 0.034722222222222224,
        "n": 0.034722222222222224,
        "o": 0.034722222222222224,
        "u": 0.034722222222222224,
        "b": 0.027777777777777776,
        "w": 0.027777777777777776,
        "c": 0.013888888888888888,
        "E": 0.006944444444444444,
        "l": 0.006944444444444444,
        "1": 0.006944444444444444,
        "2": 0.006944444444444444,
        "3": 0.006944444444444444,
        "4": 0.006944444444444444,
        "y": 0.006944444444444444
      }
    }
  }
}
```

The `distribution` object shows the probability of each character appearing in all terms. The characters are sorted by descending probability.


## Script [_script_13]

If you need to get the `string_stats` for something more complex than a single field, run the aggregation on a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md).

```console
POST /my-index-000001/_search
{
  "size": 0,
  "runtime_mappings": {
    "message_and_context": {
      "type": "keyword",
      "script": """
        emit(doc['message.keyword'].value + ' ' + doc['context.keyword'].value)
      """
    }
  },
  "aggs": {
    "message_stats": {
      "string_stats": { "field": "message_and_context" }
    }
  }
}
```


## Missing value [_missing_value_16]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

```console
POST /my-index-000001/_search?size=0
{
  "aggs": {
    "message_stats": {
      "string_stats": {
        "field": "message.keyword",
        "missing": "[empty message]" <1>
      }
    }
  }
}
```

1. Documents without a value in the `message` field will be treated as documents that have the value `[empty message]`.



