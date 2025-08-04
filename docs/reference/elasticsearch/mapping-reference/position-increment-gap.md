---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/position-increment-gap.html
---

# position_increment_gap [position-increment-gap]

[Analyzed](/reference/elasticsearch/mapping-reference/mapping-index.md) text fields take term [positions](/reference/elasticsearch/mapping-reference/index-options.md) into account, in order to be able to support [proximity or phrase queries](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md). When indexing text fields with multiple values a "fake" gap is added between the values to prevent most phrase queries from matching across the values. The size of this gap is configured using `position_increment_gap` and defaults to `100`.

For example:

```console
PUT my-index-000001/_doc/1
{
  "names": [ "John Abraham", "Lincoln Smith"]
}

GET my-index-000001/_search
{
  "query": {
    "match_phrase": {
      "names": {
        "query": "Abraham Lincoln" <1>
      }
    }
  }
}

GET my-index-000001/_search
{
  "query": {
    "match_phrase": {
      "names": {
        "query": "Abraham Lincoln",
        "slop": 101 <2>
      }
    }
  }
}
```

1. This phrase query doesn’t match our document which is totally expected.
2. This phrase query matches our document, even though `Abraham` and `Lincoln` are in separate strings, because `slop` > `position_increment_gap`.


The `position_increment_gap` can be specified in the mapping. For instance:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "names": {
        "type": "text",
        "position_increment_gap": 0 <1>
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "names": [ "John Abraham", "Lincoln Smith"]
}

GET my-index-000001/_search
{
  "query": {
    "match_phrase": {
      "names": "Abraham Lincoln" <2>
    }
  }
}
```

1. The first term in the next array element will be 0 terms apart from the last term in the previous array element.
2. The phrase query matches our document which is weird, but its what we asked for in the mapping.


