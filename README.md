Phonetic Analysis for Elasticsearch
===================================

The Phonetic Analysis plugin integrates phonetic token filter analysis with elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-analysis-phonetic/2.0.0.RC1`.

* For 1.0.x elasticsearch versions, look at [master branch](https://github.com/elasticsearch/elasticsearch-analysis-phonetic/tree/master).
* For 0.90.x elasticsearch versions, look at [1.x branch](https://github.com/elasticsearch/elasticsearch-analysis-phonetic/tree/1.x).

|    Phonetic Analysis Plugin   |    elasticsearch    | Release date |
|-------------------------------|---------------------|:------------:|
| 2.0.0-SNAPSHOT                | 1.0.0.RC1 -> master |  XXXX-XX-XX  |
| 2.0.0.RC1                     | 1.0.0.RC1 -> master |  2014-01-15  |


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

If you have questions or comments please use the [mailing list](https://groups.google.com/group/elasticsearch) instead
of Github Issues tracker.

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
