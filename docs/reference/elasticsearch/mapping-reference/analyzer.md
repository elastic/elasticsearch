---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analyzer.html
---

# analyzer [analyzer]

::::{important}
Only [`text`](/reference/elasticsearch/mapping-reference/text.md) fields support the `analyzer` mapping parameter.

::::


The `analyzer` parameter specifies the [analyzer](docs-content://manage-data/data-store/text-analysis/anatomy-of-an-analyzer.md) used for [text analysis](docs-content://manage-data/data-store/text-analysis.md) when indexing or searching a `text` field.

Unless overridden with the [`search_analyzer`](/reference/elasticsearch/mapping-reference/search-analyzer.md) mapping parameter, this analyzer is used for both [index and search analysis](docs-content://manage-data/data-store/text-analysis/index-search-analysis.md). See [Specify an analyzer](docs-content://manage-data/data-store/text-analysis/specify-an-analyzer.md).

::::{tip}
We recommend testing analyzers before using them in production. See [Test an analyzer](docs-content://manage-data/data-store/text-analysis/test-an-analyzer.md).

::::


::::{tip}
The `analyzer` setting can **not** be updated on existing fields using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
::::


## `search_quote_analyzer` [search-quote-analyzer]

The `search_quote_analyzer` setting allows you to specify an analyzer for phrases, this is particularly useful when dealing with disabling stop words for phrase queries.

To disable stop words for phrases a field utilising three analyzer settings will be required:

1. An `analyzer` setting for indexing all terms including stop words
2. A `search_analyzer` setting for non-phrase queries that will remove stop words
3. A `search_quote_analyzer` setting for phrase queries that will not remove stop words

```console
PUT my-index-000001
{
   "settings":{
      "analysis":{
         "analyzer":{
            "my_analyzer":{ <1>
               "type":"custom",
               "tokenizer":"standard",
               "filter":[
                  "lowercase"
               ]
            },
            "my_stop_analyzer":{ <2>
               "type":"custom",
               "tokenizer":"standard",
               "filter":[
                  "lowercase",
                  "english_stop"
               ]
            }
         },
         "filter":{
            "english_stop":{
               "type":"stop",
               "stopwords":"_english_"
            }
         }
      }
   },
   "mappings":{
       "properties":{
          "title": {
             "type":"text",
             "analyzer":"my_analyzer", <3>
             "search_analyzer":"my_stop_analyzer", <4>
             "search_quote_analyzer":"my_analyzer" <5>
         }
      }
   }
}

PUT my-index-000001/_doc/1
{
   "title":"The Quick Brown Fox"
}

PUT my-index-000001/_doc/2
{
   "title":"A Quick Brown Fox"
}

GET my-index-000001/_search
{
   "query":{
      "query_string":{
         "query":"\"the quick brown fox\"" <6>
      }
   }
}
```

::::{tip}
The `search_quote_analyzer` setting can be updated on existing fields using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
::::


1. `my_analyzer` analyzer which tokens all terms including stop words
2. `my_stop_analyzer` analyzer which removes stop words
3. `analyzer` setting that points to the `my_analyzer` analyzer which will be used at index time
4. `search_analyzer` setting that points to the `my_stop_analyzer` and removes stop words for non-phrase queries
5. `search_quote_analyzer` setting that points to the `my_analyzer` analyzer and ensures that stop words are not removed from phrase queries
6. Since the query is wrapped in quotes it is detected as a phrase query therefore the `search_quote_analyzer` kicks in and ensures the stop words are not removed from the query. The `my_analyzer` analyzer will then return the following tokens [`the`, `quick`, `brown`, `fox`] which will match one of the documents. Meanwhile term queries will be analyzed with the `my_stop_analyzer` analyzer which will filter out stop words. So a search for either `The quick brown fox` or `A quick brown fox` will return both documents since both documents contain the following tokens [`quick`, `brown`, `fox`]. Without the `search_quote_analyzer` it would not be possible to do exact matches for phrase queries as the stop words from phrase queries would be removed resulting in both documents matching.



