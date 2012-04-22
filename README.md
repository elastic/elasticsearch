Phonetic Analysis for ElasticSearch
===================================

The Phonetic Analysis plugin integrates phonetic token filter analysis with elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-analysis-phonetic/1.1.0`.

    -----------------------------------------------
    | Phonetic Analysis Plugin | ElasticSearch    |
    -----------------------------------------------
    | master                   | 0.19.2 -> master |
    -----------------------------------------------
    | 1.2.0                    | 0.19.2 -> master |
    -----------------------------------------------
    | 1.1.0                    | 0.19             |
    -----------------------------------------------
    | 1.0.0                    | 0.18             |
    -----------------------------------------------

A `phonetic` token filter that can be configured with different `encoder` types: 
`metaphone`, `doublemetaphone`, `soundex`, `refinedsoundex`, 
`caverphone1`, `caverphone2`, `cologne`, `nysiis`,
`koelnerphonetik`, `haasephonetik`

The `replace` parameter (defaults to `true`) controls if the token processed 
should be replaced with the encoded one (set it to `true`), or added (set it to `false`).

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
