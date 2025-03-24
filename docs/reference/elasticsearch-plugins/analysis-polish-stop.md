---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-polish-stop.html
---

# polish_stop token filter [analysis-polish-stop]

The `polish_stop` token filter filters out Polish stopwords (`_polish_`), and any other custom stopwords specified by the user. This filter only supports the predefined `_polish_` stopwords list. If you want to use a different predefined list, then use the [`stop` token filter](/reference/text-analysis/analysis-stop-tokenfilter.md) instead.

```console
PUT /polish_stop_example
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "analyzer_with_stop": {
            "tokenizer": "standard",
            "filter": [
              "lowercase",
              "polish_stop"
            ]
          }
        },
        "filter": {
          "polish_stop": {
            "type": "polish_stop",
            "stopwords": [
              "_polish_",
              "jeść"
            ]
          }
        }
      }
    }
  }
}

GET polish_stop_example/_analyze
{
  "analyzer": "analyzer_with_stop",
  "text": "Gdzie kucharek sześć, tam nie ma co jeść."
}
```

The above request returns:

```console-result
{
  "tokens" : [
    {
      "token" : "kucharek",
      "start_offset" : 6,
      "end_offset" : 14,
      "type" : "<ALPHANUM>",
      "position" : 1
    },
    {
      "token" : "sześć",
      "start_offset" : 15,
      "end_offset" : 20,
      "type" : "<ALPHANUM>",
      "position" : 2
    }
  ]
}
```

