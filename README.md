Phonetic Analysis for ElasticSearch
===================================

The Phonetic Analysis plugin integrates phonetic token filter analysis with elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-analysis-phonetic/1.1.0`.

    ---------------------------------------------
    | Phonetic Analysis Plugin | ElasticSearch  |
    ---------------------------------------------
    | master                   | 0.18 -> master |
    ---------------------------------------------
    | 1.0.0                    | 0.18 -> master |
    ---------------------------------------------

A `phonetic` token filter that can be configured with different `encoder` types: `metaphone`, `soundex`, `caverphone`, `refined_soundex`, `double_metaphone` (uses "commons codec":http://jakarta.apache.org/commons/codec/api-release/org/apache/commons/codec/language/package-summary.html).

The `replace` parameter (defaults to `true`) controls if the token processed should be replaced with the encoded one (set it to `true`), or added (set it to `false`).

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "my_analyzer" : {
                        "tokenizer" : "standard",
                        "filter" : ["standard", "lowercase", "my_metaphone"]
                    }
                },
                "filter" : {
                    "my_metaphone" : {
                        "type" : "phonetic",
                        "encoder" : "metaphone",
                        "replace" : false
                    }
                }
            }
        }
    }
