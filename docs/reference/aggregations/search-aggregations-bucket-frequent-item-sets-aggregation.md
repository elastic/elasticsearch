---
navigation_title: "Frequent item sets"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-frequent-item-sets-aggregation.html
---

# Frequent item sets aggregation [search-aggregations-bucket-frequent-item-sets-aggregation]


A bucket aggregation which finds frequent item sets. It is a form of association rules mining that identifies items that often occur together. Items that are frequently purchased together or log events that tend to co-occur are examples of frequent item sets. Finding frequent item sets helps to discover relationships between different data points (items).

The aggregation reports closed item sets. A frequent item set is called closed if no superset exists with the same ratio of documents (also known as its [support value](#frequent-item-sets-minimum-support)). For example, we have the two following candidates for a frequent item set, which have the same support value: 1. `apple, orange, banana` 2. `apple, orange, banana, tomato`. Only the second item set (`apple, orange, banana, tomato`) is returned, and the first set – which is a subset of the second one – is skipped. Both item sets might be returned if their support values are different.

The runtime of the aggregation depends on the data and the provided parameters. It might take a significant time for the aggregation to complete. For this reason, it is recommended to use [async search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-async-search-submit) to run your requests asynchronously.

## Syntax [_syntax_2]

A `frequent_item_sets` aggregation looks like this in isolation:

```js
"frequent_item_sets": {
  "minimum_set_size": 3,
  "fields": [
    {"field": "my_field_1"},
    {"field": "my_field_2"}
  ]
}
```

|     |     |     |     |
| --- | --- | --- | --- |
| Parameter Name | Description | Required | Default Value |
| `fields` | (array) Fields to analyze. | Required |  |
| `minimum_set_size` | (integer) The [minimum size](#frequent-item-sets-minimum-set-size) of one item set. | Optional | `1` |
| `minimum_support` | (integer) The [minimum support](#frequent-item-sets-minimum-support) of one item set. | Optional | `0.1` |
| `size` | (integer) The number of top item sets to return. | Optional | `10` |
| `filter` | (object) Query that filters documents from the analysis | Optional | `match_all` |


### Fields [frequent-item-sets-fields]

Supported field types for the analyzed fields are keyword, numeric, ip, date, and arrays of these types. You can also add runtime fields to your analyzed fields.

If the combined cardinality of the analyzed fields are high, the aggregation might require a significant amount of system resources.

You can filter the values for each field by using the `include` and `exclude` parameters. The parameters can be regular expression strings or arrays of strings of exact terms. The filtered values are removed from the analysis and therefore reduce the runtime. If both `include` and `exclude` are defined, `exclude` takes precedence; it means `include` is evaluated first and then `exclude`.


### Minimum set size [frequent-item-sets-minimum-set-size]

The minimum set size is the minimum number of items the set needs to contain. A value of 1 returns the frequency of single items. Only item sets that contain at least the number of `minimum_set_size` items are returned. For example, the item set `orange, banana, apple` is returned only if the minimum set size is 3 or lower.


### Minimum support [frequent-item-sets-minimum-support]

The minimum support value is the ratio of documents that an item set must exist in to be considered "frequent". In particular, it is a normalized value between 0 and 1. It is calculated by dividing the number of documents containing the item set by the total number of documents.

For example, if a given item set is contained by five documents and the total number of documents is 20, then the support of the item set is 5/20 = 0.25. Therefore, this set is returned only if the minimum support is 0.25 or lower. As a higher minimum support prunes more items, the calculation is less resource intensive. The `minimum_support` parameter has an effect on the required memory and the runtime of the aggregation.


### Size [frequent-item-sets-size]

This parameter defines the maximum number of item sets to return. The result contains top-k item sets; the item sets with the highest support values. This parameter has a significant effect on the required memory and the runtime of the aggregation.


### Filter [frequent-item-sets-filter]

A query to filter documents to use as part of the analysis. Documents that don’t match the filter are ignored when generating the item sets, however still count when calculating the support of an item set.

Use the filter if you want to narrow the item set analysis to fields of interest. Use a top-level query to filter the data set.


### Examples [frequent-item-sets-example]

In the following examples, we use the e-commerce {{kib}} sample data set.


### Aggregation with two analyzed fields and an `exclude` parameter [_aggregation_with_two_analyzed_fields_and_an_exclude_parameter]

In the first example, the goal is to find out based on transaction data (1.) from what product categories the customers purchase products frequently together and (2.) from which cities they make those purchases. We want to exclude results where location information is not available (where the city name is `other`). Finally, we are interested in sets with three or more items, and want to see the first three frequent item sets with the highest support.

Note that we use the [async search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-async-search-submit) endpoint in this first example.

```console
POST /kibana_sample_data_ecommerce/_async_search
{
   "size":0,
   "aggs":{
      "my_agg":{
         "frequent_item_sets":{
            "minimum_set_size":3,
            "fields":[
               {
                  "field":"category.keyword"
               },
               {
                  "field":"geoip.city_name",
                  "exclude":"other"
               }
            ],
            "size":3
         }
      }
   }
}
```

The response of the API call above contains an identifier (`id`) of the async search request. You can use the identifier to retrieve the search results:

```console
GET /_async_search/<id>
```

The API returns a response similar to the following one:

```console-result
(...)
"aggregations" : {
    "my_agg" : {
      "buckets" : [ <1>
        {
          "key" : { <2>
            "category.keyword" : [
              "Women's Clothing",
              "Women's Shoes"
            ],
            "geoip.city_name" : [
              "New York"
            ]
          },
          "doc_count" : 217, <3>
          "support" : 0.04641711229946524 <4>
        },
        {
          "key" : {
            "category.keyword" : [
              "Women's Clothing",
              "Women's Accessories"
            ],
            "geoip.city_name" : [
              "New York"
            ]
          },
          "doc_count" : 135,
          "support" : 0.028877005347593583
        },
        {
          "key" : {
            "category.keyword" : [
              "Men's Clothing",
              "Men's Shoes"
            ],
            "geoip.city_name" : [
              "Cairo"
            ]
          },
          "doc_count" : 123,
          "support" : 0.026310160427807486
        }
      ],
    (...)
  }
}
```

1. The array of returned item sets.
2. The `key` object contains one item set. In this case, it consists of two values of the `category.keyword` field and one value of the `geoip.city_name`.
3. The number of documents that contain the item set.
4. The support value of the item set. It is calculated by dividing the number of documents containing the item set by the total number of documents.


The response shows that the categories customers purchase from most frequently together are `Women's Clothing` and `Women's Shoes` and customers from New York tend to buy items from these categories frequently together. In other words, customers who buy products labelled `Women's Clothing` more likely buy products also from the `Women's Shoes` category and customers from New York most likely buy products from these categories together. The item set with the second highest support is `Women's Clothing` and `Women's Accessories` with customers mostly from New York. Finally, the item set with the third highest support is `Men's Clothing` and `Men's Shoes` with customers mostly from Cairo.


### Aggregation with two analyzed fields and a filter [_aggregation_with_two_analyzed_fields_and_a_filter]

We take the first example, but want to narrow the item sets to places in Europe. For that, we add a filter, and this time, we don’t use the `exclude` parameter:

```console
POST /kibana_sample_data_ecommerce/_async_search
{
  "size": 0,
  "aggs": {
    "my_agg": {
      "frequent_item_sets": {
        "minimum_set_size": 3,
        "fields": [
          { "field": "category.keyword" },
          { "field": "geoip.city_name" }
        ],
        "size": 3,
        "filter": {
          "term": {
            "geoip.continent_name": "Europe"
          }
        }
      }
    }
  }
}
```

The result will only show item sets that created from documents matching the filter, namely purchases in Europe. Using `filter`, the calculated `support` still takes all purchases into acount. That’s different than specifying a query at the top-level, in which case `support` gets calculated only from purchases in Europe.


### Analyzing numeric values by using a runtime field [_analyzing_numeric_values_by_using_a_runtime_field]

The frequent items aggregation enables you to bucket numeric values by using [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md). The next example demonstrates how to use a script to add a runtime field to your documents called `price_range`, which is calculated from the taxful total price of the individual transactions. The runtime field then can be used in the frequent items aggregation as a field to analyze.

```console
GET kibana_sample_data_ecommerce/_search
{
  "runtime_mappings": {
    "price_range": {
      "type": "keyword",
      "script": {
        "source": """
           def bucket_start = (long) Math.floor(doc['taxful_total_price'].value / 50) * 50;
           def bucket_end = bucket_start + 50;
           emit(bucket_start.toString() + "-" + bucket_end.toString());
        """
      }
    }
  },
  "size": 0,
  "aggs": {
    "my_agg": {
      "frequent_item_sets": {
        "minimum_set_size": 4,
        "fields": [
          {
            "field": "category.keyword"
          },
          {
            "field": "price_range"
          },
          {
            "field": "geoip.city_name"
          }
        ],
        "size": 3
      }
    }
  }
}
```

The API returns a response similar to the following one:

```console-result
(...)
"aggregations" : {
    "my_agg" : {
      "buckets" : [
        {
          "key" : {
            "category.keyword" : [
              "Women's Clothing",
              "Women's Shoes"
            ],
            "price_range" : [
              "50-100"
            ],
            "geoip.city_name" : [
              "New York"
            ]
          },
          "doc_count" : 100,
          "support" : 0.0213903743315508
        },
        {
          "key" : {
            "category.keyword" : [
              "Women's Clothing",
              "Women's Shoes"
            ],
            "price_range" : [
              "50-100"
            ],
            "geoip.city_name" : [
              "Dubai"
            ]
          },
          "doc_count" : 59,
          "support" : 0.012620320855614974
        },
        {
          "key" : {
            "category.keyword" : [
              "Men's Clothing",
              "Men's Shoes"
            ],
            "price_range" : [
              "50-100"
            ],
            "geoip.city_name" : [
              "Marrakesh"
            ]
          },
          "doc_count" : 53,
          "support" : 0.011336898395721925
        }
      ],
    (...)
    }
  }
```

The response shows the categories that customers purchase from most frequently together, the location of the customers who tend to buy items from these categories, and the most frequent price ranges of these purchases.


