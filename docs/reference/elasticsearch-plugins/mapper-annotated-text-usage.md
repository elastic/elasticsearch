---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-annotated-text-usage.html
---

# Using the annotated-text field [mapper-annotated-text-usage]

The `annotated-text` tokenizes text content as per the more common [`text`](/reference/elasticsearch/mapping-reference/text.md) field (see "limitations" below) but also injects any marked-up annotation tokens directly into the search index:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_field": {
        "type": "annotated_text"
      }
    }
  }
}
```

Such a mapping would allow marked-up text eg wikipedia articles to be indexed as both text and structured tokens. The annotations use a markdown-like syntax using URL encoding of one or more values separated by the `&` symbol.

We can use the "_analyze" api to test how an example annotation would be stored as tokens in the search index:

```js
GET my-index-000001/_analyze
{
  "field": "my_field",
  "text":"Investors in [Apple](Apple+Inc.) rejoiced."
}
```

Response:

```js
{
  "tokens": [
    {
      "token": "investors",
      "start_offset": 0,
      "end_offset": 9,
      "type": "<ALPHANUM>",
      "position": 0
    },
    {
      "token": "in",
      "start_offset": 10,
      "end_offset": 12,
      "type": "<ALPHANUM>",
      "position": 1
    },
    {
      "token": "Apple Inc.", <1>
      "start_offset": 13,
      "end_offset": 18,
      "type": "annotation",
      "position": 2
    },
    {
      "token": "apple",
      "start_offset": 13,
      "end_offset": 18,
      "type": "<ALPHANUM>",
      "position": 2
    },
    {
      "token": "rejoiced",
      "start_offset": 19,
      "end_offset": 27,
      "type": "<ALPHANUM>",
      "position": 3
    }
  ]
}
```

1. Note the whole annotation token `Apple Inc.` is placed, unchanged as a single token in the token stream and at the same position (position 2) as the text token (`apple`) it annotates.


We can now perform searches for annotations using regular `term` queries that don’t tokenize the provided search values. Annotations are a more precise way of matching as can be seen in this example where a search for `Beck` will not match `Jeff Beck` :

```console
# Example documents
PUT my-index-000001/_doc/1
{
  "my_field": "[Beck](Beck) announced a new tour"<1>
}

PUT my-index-000001/_doc/2
{
  "my_field": "[Jeff Beck](Jeff+Beck&Guitarist) plays a strat"<2>
}

# Example search
GET my-index-000001/_search
{
  "query": {
    "term": {
        "my_field": "Beck" <3>
    }
  }
}
```

1. As well as tokenising the plain text into single words e.g. `beck`, here we inject the single token value `Beck` at the same position as `beck` in the token stream.
2. Note annotations can inject multiple tokens at the same position - here we inject both the very specific value `Jeff Beck` and the broader term `Guitarist`. This enables broader positional queries e.g. finding mentions of a `Guitarist` near to `strat`.
3. A benefit of searching with these carefully defined annotation tokens is that a query for `Beck` will not match document 2 that contains the tokens `jeff`, `beck` and `Jeff Beck`


::::{warning}
Any use of `=` signs in annotation values eg `[Prince](person=Prince)` will cause the document to be rejected with a parse failure. In future we hope to have a use for the equals signs so will actively reject documents that contain this today.
::::


## Synthetic `_source` [annotated-text-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


If using a sub-`keyword` field then the values are sorted in the same way as a `keyword` field’s values are sorted. By default, that means sorted with duplicates removed. So:

$$$synthetic-source-text-example-default$$$

```console
PUT idx
{
  "settings": {
    "index": {
      "mapping": {
        "source": {
          "mode": "synthetic"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "text": {
        "type": "annotated_text",
        "fields": {
          "raw": {
            "type": "keyword"
          }
        }
      }
    }
  }
}
PUT idx/_doc/1
{
  "text": [
    "the quick brown fox",
    "the quick brown fox",
    "jumped over the lazy dog"
  ]
}
```

Will become:

```console-result
{
  "text": [
    "jumped over the lazy dog",
    "the quick brown fox"
  ]
}
```

::::{note}
Reordering text fields can have an effect on [phrase](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) and [span](/reference/query-languages/query-dsl/span-queries.md) queries. See the discussion about [`position_increment_gap`](/reference/elasticsearch/mapping-reference/position-increment-gap.md) for more detail. You can avoid this by making sure the `slop` parameter on the phrase queries is lower than the `position_increment_gap`. This is the default.
::::


If the `annotated_text` field sets `store` to true then order and duplicates are preserved.

$$$synthetic-source-text-example-stored$$$

```console
PUT idx
{
  "settings": {
    "index": {
      "mapping": {
        "source": {
          "mode": "synthetic"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "text": { "type": "annotated_text", "store": true }
    }
  }
}
PUT idx/_doc/1
{
  "text": [
    "the quick brown fox",
    "the quick brown fox",
    "jumped over the lazy dog"
  ]
}
```

Will become:

```console-result
{
  "text": [
    "the quick brown fox",
    "the quick brown fox",
    "jumped over the lazy dog"
  ]
}
```


