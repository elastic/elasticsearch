---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-field-note.html
---

# Subtleties of bucketing range fields [search-aggregations-bucket-range-field-note]

## Documents are counted for each bucket they land in [_documents_are_counted_for_each_bucket_they_land_in]

Since a range represents multiple values, running a bucket aggregation over a range field can result in the same document landing in multiple buckets. This can lead to surprising behavior, such as the sum of bucket counts being higher than the number of matched documents. For example, consider the following index:

```console
PUT range_index
{
  "settings": {
    "number_of_shards": 2
  },
  "mappings": {
    "properties": {
      "expected_attendees": {
        "type": "integer_range"
      },
      "time_frame": {
        "type": "date_range",
        "format": "yyyy-MM-dd||epoch_millis"
      }
    }
  }
}

PUT range_index/_doc/1?refresh
{
  "expected_attendees" : {
    "gte" : 10,
    "lte" : 20
  },
  "time_frame" : {
    "gte" : "2019-10-28",
    "lte" : "2019-11-04"
  }
}
```

The range is wider than the interval in the following aggregation, and thus the document will land in multiple buckets.

$$$range-field-aggregation-example$$$

```console
POST /range_index/_search?size=0
{
  "aggs": {
    "range_histo": {
      "histogram": {
        "field": "expected_attendees",
        "interval": 5
      }
    }
  }
}
```

Since the interval is `5` (and the offset is `0` by default), we expect buckets `10`, `15`, and `20`. Our range document will fall in all three of these buckets.

```console-result
{
  ...
  "aggregations" : {
    "range_histo" : {
      "buckets" : [
        {
          "key" : 10.0,
          "doc_count" : 1
        },
        {
          "key" : 15.0,
          "doc_count" : 1
        },
        {
          "key" : 20.0,
          "doc_count" : 1
        }
      ]
    }
  }
}
```

A document cannot exist partially in a bucket; For example, the above document cannot count as one-third in each of the above three buckets. In this example, since the document’s range landed in multiple buckets, the full value of that document would also be counted in any sub-aggregations for each bucket as well.


## Query bounds are not aggregation filters [_query_bounds_are_not_aggregation_filters]

Another unexpected behavior can arise when a query is used to filter on the field being aggregated. In this case, a document could match the query but still have one or both of the endpoints of the range outside the query. Consider the following aggregation on the above document:

$$$range-field-aggregation-query-bounds-example$$$

```console
POST /range_index/_search?size=0
{
  "query": {
    "range": {
      "time_frame": {
        "gte": "2019-11-01",
        "format": "yyyy-MM-dd"
      }
    }
  },
  "aggs": {
    "november_data": {
      "date_histogram": {
        "field": "time_frame",
        "calendar_interval": "day",
        "format": "yyyy-MM-dd"
      }
    }
  }
}
```

Even though the query only considers days in November, the aggregation generates 8 buckets (4 in October, 4 in November) because the aggregation is calculated over the ranges of all matching documents.

```console-result
{
  ...
  "aggregations" : {
    "november_data" : {
      "buckets" : [
              {
          "key_as_string" : "2019-10-28",
          "key" : 1572220800000,
          "doc_count" : 1
        },
        {
          "key_as_string" : "2019-10-29",
          "key" : 1572307200000,
          "doc_count" : 1
        },
        {
          "key_as_string" : "2019-10-30",
          "key" : 1572393600000,
          "doc_count" : 1
        },
        {
          "key_as_string" : "2019-10-31",
          "key" : 1572480000000,
          "doc_count" : 1
        },
        {
          "key_as_string" : "2019-11-01",
          "key" : 1572566400000,
          "doc_count" : 1
        },
        {
          "key_as_string" : "2019-11-02",
          "key" : 1572652800000,
          "doc_count" : 1
        },
        {
          "key_as_string" : "2019-11-03",
          "key" : 1572739200000,
          "doc_count" : 1
        },
        {
          "key_as_string" : "2019-11-04",
          "key" : 1572825600000,
          "doc_count" : 1
        }
      ]
    }
  }
}
```

Depending on the use case, a `CONTAINS` query could limit the documents to only those that fall entirely in the queried range. In this example, the one document would not be included and the aggregation would be empty. Filtering the buckets after the aggregation is also an option, for use cases where the document should be counted but the out of bounds data can be safely ignored.


