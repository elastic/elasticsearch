---
navigation_title: "Nested"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-nested-aggregation.html
---

# Nested aggregation [search-aggregations-bucket-nested-aggregation]


A special single bucket aggregation that enables aggregating nested documents.

For example, lets say we have an index of products, and each product holds the list of resellers - each having its own price for the product. The mapping could look like:

$$$nested-aggregation-example$$$

```console
PUT /products
{
  "mappings": {
    "properties": {
      "resellers": { <1>
        "type": "nested",
        "properties": {
          "reseller": {
            "type": "keyword"
          },
          "price": {
            "type": "double"
          }
        }
      }
    }
  }
}
```

1. `resellers` is an array that holds nested documents.


The following request adds a product with two resellers:

```console
PUT /products/_doc/0?refresh
{
  "name": "LED TV", <1>
  "resellers": [
    {
      "reseller": "companyA",
      "price": 350
    },
    {
      "reseller": "companyB",
      "price": 500
    }
  ]
}
```
% TEST[continued]

1. We are using a dynamic mapping for the `name` attribute.


The following request returns the minimum price a product can be purchased for:

```console
GET /products/_search?size=0
{
  "query": {
    "match": {
      "name": "led tv"
    }
  },
  "aggs": {
    "resellers": {
      "nested": {
        "path": "resellers"
      },
      "aggs": {
        "min_price": {
          "min": {
            "field": "resellers.price"
          }
        }
      }
    }
  }
}
```
% TEST[s/size=0/size=0&filter_path=aggregations/]
% TEST[continued]

As you can see above, the nested aggregation requires the `path` of the nested documents within the top level documents. Then one can define any type of aggregation over these nested documents.

Response:

```console-result
{
  ...
  "aggregations": {
    "resellers": {
      "doc_count": 2,
      "min_price": {
        "value": 350.0
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\.//]

You can use a [`filter`](/reference/aggregations/search-aggregations-bucket-filter-aggregation.md) sub-aggregation to return results for a specific reseller.

```console
GET /products/_search?size=0
{
  "query": {
    "match": {
      "name": "led tv"
    }
  },
  "aggs": {
    "resellers": {
      "nested": {
        "path": "resellers"
      },
      "aggs": {
        "filter_reseller": {
          "filter": {
            "bool": {
              "filter": [
                {
                  "term": {
                    "resellers.reseller": "companyB"
                  }
                }
              ]
            }
          },
          "aggs": {
            "min_price": {
              "min": {
                "field": "resellers.price"
              }
            }
          }
        }
      }
    }
  }
}
```
% TEST[s/size=0/size=0&filter_path=aggregations/]
% TEST[continued]

The search returns:

```console-result
{
  ...
  "aggregations": {
    "resellers": {
      "doc_count": 2,
      "filter_reseller": {
        "doc_count": 1,
        "min_price": {
          "value": 500.0
        }
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\.//]

