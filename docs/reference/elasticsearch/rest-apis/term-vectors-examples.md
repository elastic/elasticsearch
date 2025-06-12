---
applies_to:
  stack: all
navigation_title: Term vectors API examples
---
# Term vectors API examples

This page shows you examples of using the [term vectors API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-termvectors).

## Returning stored term vectors [docs-termvectors-api-stored-termvectors]

First, create an index that stores term vectors, payloads, and so on:

```console
PUT /my-index-000001
{ "mappings": {
    "properties": {
      "text": {
        "type": "text",
        "term_vector": "with_positions_offsets_payloads",
        "store" : true,
        "analyzer" : "fulltext_analyzer"
       },
       "fullname": {
        "type": "text",
        "term_vector": "with_positions_offsets_payloads",
        "analyzer" : "fulltext_analyzer"
      }
    }
  },
  "settings" : {
    "index" : {
      "number_of_shards" : 1,
      "number_of_replicas" : 0
    },
    "analysis": {
      "analyzer": {
        "fulltext_analyzer": {
          "type": "custom",
          "tokenizer": "whitespace",
          "filter": [
            "lowercase",
            "type_as_payload"
          ]
        }
      }
    }
  }
}
```

Add some documents:

```console
PUT /my-index-000001/_doc/1
{
  "fullname" : "John Doe",
  "text" : "test test test "
}

PUT /my-index-000001/_doc/2?refresh=wait_for
{
  "fullname" : "Jane Doe",
  "text" : "Another test ..."
}
```

% TEST[continued]

The following request returns all information and statistics for field
`text` in document `1` (John Doe):

```console
GET /my-index-000001/_termvectors/1
{
  "fields" : ["text"],
  "offsets" : true,
  "payloads" : true,
  "positions" : true,
  "term_statistics" : true,
  "field_statistics" : true
}
```

% TEST[continued]

Response:

```console-result
{
  "_index": "my-index-000001",
  "_id": "1",
  "_version": 1,
  "found": true,
  "took": 6,
  "term_vectors": {
    "text": {
      "field_statistics": {
        "sum_doc_freq": 4,
        "doc_count": 2,
        "sum_ttf": 6
      },
      "terms": {
        "test": {
          "doc_freq": 2,
          "ttf": 4,
          "term_freq": 3,
          "tokens": [
            {
              "position": 0,
              "start_offset": 0,
              "end_offset": 4,
              "payload": "d29yZA=="
            },
            {
              "position": 1,
              "start_offset": 5,
              "end_offset": 9,
              "payload": "d29yZA=="
            },
            {
              "position": 2,
              "start_offset": 10,
              "end_offset": 14,
              "payload": "d29yZA=="
            }
          ]
        }
      }
    }
  }
}
```

% TEST[continued]
% TESTRESPONSE[s/"took": 6/"took": "$body.took"/]

## Generating term vectors on the fly [docs-termvectors-api-generate-termvectors]

Term vectors which are not explicitly stored in the index are automatically
computed on the fly. The following request returns all information and statistics for the
fields in document `1`, even though the terms haven't been explicitly stored in the index.
Note that for the field `text`, the terms are not re-generated.

```console
GET /my-index-000001/_termvectors/1
{
  "fields" : ["text", "some_field_without_term_vectors"],
  "offsets" : true,
  "positions" : true,
  "term_statistics" : true,
  "field_statistics" : true
}
```

% TEST[continued]

## Artificial documents [docs-termvectors-artificial-doc]

Term vectors can also be generated for artificial documents,
that is for documents not present in the index. For example, the following request would
return the same results as in example 1. The mapping used is determined by the `index`.

*If dynamic mapping is turned on (default), the document fields not in the original mapping will be dynamically created.*

```console
GET /my-index-000001/_termvectors
{
  "doc" : {
    "fullname" : "John Doe",
    "text" : "test test test"
  }
}
```

% TEST[continued]

## Per-field analyzer [docs-termvectors-per-field-analyzer]

Additionally, a different analyzer than the one at the field may be provided
by using the `per_field_analyzer` parameter. This is useful in order to
generate term vectors in any fashion, especially when using artificial
documents. When providing an analyzer for a field that already stores term
vectors, the term vectors will be re-generated.

```console
GET /my-index-000001/_termvectors
{
  "doc" : {
    "fullname" : "John Doe",
    "text" : "test test test"
  },
  "fields": ["fullname"],
  "per_field_analyzer" : {
    "fullname": "keyword"
  }
}
```

% TEST[continued]

Response:

```console-result
{
  "_index": "my-index-000001",
  "_version": 0,
  "found": true,
  "took": 6,
  "term_vectors": {
    "fullname": {
       "field_statistics": {
          "sum_doc_freq": 2,
          "doc_count": 4,
          "sum_ttf": 4
       },
       "terms": {
          "John Doe": {
             "term_freq": 1,
             "tokens": [
                {
                   "position": 0,
                   "start_offset": 0,
                   "end_offset": 8
                }
             ]
          }
       }
    }
  }
}
```

% TEST[continued]
% TESTRESPONSE[s/"took": 6/"took": "$body.took"/]
% TESTRESPONSE[s/"sum_doc_freq": 2/"sum_doc_freq": "$body.term_vectors.fullname.field_statistics.sum_doc_freq"/]
% TESTRESPONSE[s/"doc_count": 4/"doc_count": "$body.term_vectors.fullname.field_statistics.doc_count"/]
% TESTRESPONSE[s/"sum_ttf": 4/"sum_ttf": "$body.term_vectors.fullname.field_statistics.sum_ttf"/]

## Terms filtering [docs-termvectors-terms-filtering]

Finally, the terms returned could be filtered based on their tf-idf scores. In
the example below we obtain the three most "interesting" keywords from the
artificial document having the given "plot" field value. Notice
that the keyword "Tony" or any stop words are not part of the response, as
their tf-idf must be too low.

```console
GET /imdb/_termvectors
{
  "doc": {
    "plot": "When wealthy industrialist Tony Stark is forced to build an armored suit after a life-threatening incident, he ultimately decides to use its technology to fight against evil."
  },
  "term_statistics": true,
  "field_statistics": true,
  "positions": false,
  "offsets": false,
  "filter": {
    "max_num_terms": 3,
    "min_term_freq": 1,
    "min_doc_freq": 1
  }
}
```

% TEST[skip:no imdb test index]

Response:

```console-result
{
   "_index": "imdb",
   "_version": 0,
   "found": true,
   "term_vectors": {
      "plot": {
         "field_statistics": {
            "sum_doc_freq": 3384269,
            "doc_count": 176214,
            "sum_ttf": 3753460
         },
         "terms": {
            "armored": {
               "doc_freq": 27,
               "ttf": 27,
               "term_freq": 1,
               "score": 9.74725
            },
            "industrialist": {
               "doc_freq": 88,
               "ttf": 88,
               "term_freq": 1,
               "score": 8.590818
            },
            "stark": {
               "doc_freq": 44,
               "ttf": 47,
               "term_freq": 1,
               "score": 9.272792
            }
         }
      }
   }
}
```
