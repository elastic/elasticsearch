Phonetic Analysis for ElasticSearch
===================================

The Phonetic Analysis plugin integrates phonetic token filter analysis with elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-analysis-phonetic/1.3.0`.

    -----------------------------------------------
    | Phonetic Analysis Plugin | ElasticSearch    |
    -----------------------------------------------
    | master                   | 0.90   -> master |
    -----------------------------------------------
    | 1.3.0                    | 0.90   -> master |
    -----------------------------------------------
    | 1.2.0                    | 0.19.2 -> 0.20   |
    -----------------------------------------------
    | 1.1.0                    | 0.19             |
    -----------------------------------------------
    | 1.0.0                    | 0.18             |
    -----------------------------------------------

A `phonetic` token filter that can be configured with different `encoder` types: 
`metaphone`, `doublemetaphone`, `soundex`, `refinedsoundex`, 
`caverphone1`, `caverphone2`, `cologne`, `nysiis`,
`koelnerphonetik`, `haasephonetik`, `beidermorse`

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

Questions
---------

If you have questions or comments please use the mailing list instead of Github Issues tracker: https://groups.google.com/group/elasticsearch

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2012 Shay Banon and ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
