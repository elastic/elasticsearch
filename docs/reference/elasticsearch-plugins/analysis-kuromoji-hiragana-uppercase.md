---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji-hiragana-uppercase.html
---

# hiragana_uppercase token filter [analysis-kuromoji-hiragana-uppercase]

The `hiragana_uppercase` token filter normalizes small letters (捨て仮名) in hiragana into standard letters. This filter is useful if you want to search against old style Japanese text such as patents, legal documents, contract policies, etc.

For example:

```console
PUT kuromoji_sample
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "my_analyzer": {
            "tokenizer": "kuromoji_tokenizer",
            "filter": [
              "hiragana_uppercase"
            ]
          }
        }
      }
    }
  }
}

GET kuromoji_sample/_analyze
{
  "analyzer": "my_analyzer",
  "text": "ちょっとまって"
}
```

Which results in:

```console-result
{
  "tokens": [
    {
      "token": "ちよつと",
      "start_offset": 0,
      "end_offset": 4,
      "type": "word",
      "position": 0
    },
    {
      "token": "まつ",
      "start_offset": 4,
      "end_offset": 6,
      "type": "word",
      "position": 1
    },
    {
      "token": "て",
      "start_offset": 6,
      "end_offset": 7,
      "type": "word",
      "position": 2
    }
  ]
}
```

