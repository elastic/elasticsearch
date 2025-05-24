---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/normalizer.html
---

# normalizer [normalizer]

The `normalizer` property of [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) fields is similar to [`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md) except that it guarantees that the analysis chain produces a single token.

The `normalizer` is applied prior to indexing the keyword, as well as at search-time when the `keyword` field is searched via a query parser such as the [`match`](/reference/query-languages/query-dsl/query-dsl-match-query.md) query or via a term-level query such as the [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md) query.

A simple normalizer called `lowercase` ships with elasticsearch and can be used. Custom normalizers can be defined as part of analysis settings as follows.

```console
PUT index
{
  "settings": {
    "analysis": {
      "normalizer": {
        "my_normalizer": {
          "type": "custom",
          "char_filter": [],
          "filter": ["lowercase", "asciifolding"]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "foo": {
        "type": "keyword",
        "normalizer": "my_normalizer"
      }
    }
  }
}

PUT index/_doc/1
{
  "foo": "BÀR"
}

PUT index/_doc/2
{
  "foo": "bar"
}

PUT index/_doc/3
{
  "foo": "baz"
}

POST index/_refresh

GET index/_search
{
  "query": {
    "term": {
      "foo": "BAR"
    }
  }
}

GET index/_search
{
  "query": {
    "match": {
      "foo": "BAR"
    }
  }
}
```

The above queries match documents 1 and 2 since `BÀR` is converted to `bar` at both index and query time.

```console-result
{
  "took": $body.took,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 2,
        "relation": "eq"
    },
    "max_score": 0.4700036,
    "hits": [
      {
        "_index": "index",
        "_id": "1",
        "_score": 0.4700036,
        "_source": {
          "foo": "BÀR"
        }
      },
      {
        "_index": "index",
        "_id": "2",
        "_score": 0.4700036,
        "_source": {
          "foo": "bar"
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took".*/"took": "$body.took",/]

Also, the fact that keywords are converted prior to indexing also means that aggregations return normalized values:

```console
GET index/_search
{
  "size": 0,
  "aggs": {
    "foo_terms": {
      "terms": {
        "field": "foo"
      }
    }
  }
}
```
% TEST[continued]

returns

```console-result
{
  "took": 43,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 3,
        "relation": "eq"
    },
    "max_score": null,
    "hits": []
  },
  "aggregations": {
    "foo_terms": {
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 0,
      "buckets": [
        {
          "key": "bar",
          "doc_count": 2
        },
        {
          "key": "baz",
          "doc_count": 1
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/"took".*/"took": "$body.took",/]

