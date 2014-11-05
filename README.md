ICU Analysis for Elasticsearch
==================================

The ICU Analysis plugin integrates Lucene ICU module into elasticsearch, adding ICU relates analysis components.

In order to install the plugin, simply run: 

```sh
bin/plugin -install elasticsearch/elasticsearch-analysis-icu/2.4.1
```

You need to install a version matching your Elasticsearch version:

| elasticsearch |  ICU Analysis Plugin  |   Docs     |  
|---------------|-----------------------|------------|
| master        |  Build from source    | See below  |
| es-1.x        |  Build from source    | [2.5.0-SNAPSHOT](https://github.com/elasticsearch/elasticsearch-analysis-icu/tree/es-1.x/#version-250-snapshot-for-elasticsearch-1x)  |
|    es-1.4              |     2.4.1         | [2.4.1](https://github.com/elasticsearch/elasticsearch-analysis-icu/tree/v2.4.1/#version-241-for-elasticsearch-14)                  |
| es-1.3        |  2.3.0                | [2.3.0](https://github.com/elasticsearch/elasticsearch-analysis-icu/tree/v2.3.0/#icu-analysis-for-elasticsearch)  |
| es-1.2        |  2.2.0                | [2.2.0](https://github.com/elasticsearch/elasticsearch-analysis-icu/tree/v2.2.0/#icu-analysis-for-elasticsearch)  |
| es-1.1        |  2.1.0                | [2.1.0](https://github.com/elasticsearch/elasticsearch-analysis-icu/tree/v2.1.0/#icu-analysis-for-elasticsearch)  |
| es-1.0        |  2.0.0                | [2.0.0](https://github.com/elasticsearch/elasticsearch-analysis-icu/tree/v2.0.0/#icu-analysis-for-elasticsearch)  |
| es-0.90       |  1.13.0               | [1.13.0](https://github.com/elasticsearch/elasticsearch-analysis-icu/tree/v1.13.0/#icu-analysis-for-elasticsearch)  |

To build a `SNAPSHOT` version, you need to build it with Maven:

```bash
mvn clean install
plugin --install analysis-icu \
       --url file:target/releases/elasticsearch-analysis-icu-X.X.X-SNAPSHOT.zip
```


ICU Normalization
-----------------

Normalizes characters as explained [here](http://userguide.icu-project.org/transforms/normalization). It registers itself by default under `icu_normalizer` or `icuNormalizer` using the default settings. Allows for the name parameter to be provided which can include the following values: `nfc`, `nfkc`, and `nfkc_cf`. Here is a sample settings:

```js
{
    "index" : {
        "analysis" : {
            "analyzer" : {
                "normalized" : {
                    "tokenizer" : "keyword",
                    "filter" : ["icu_normalizer"]
                }
            }
        }
    }
}
```

ICU Folding
-----------

Folding of unicode characters based on `UTR#30`. It registers itself under `icu_folding` and `icuFolding` names. Sample setting:

```js
{
    "index" : {
        "analysis" : {
            "analyzer" : {
                "folded" : {
                    "tokenizer" : "keyword",
                    "filter" : ["icu_folding"]
                }
            }
        }
    }
}
```

ICU Filtering
-------------

The folding can be filtered by a set of unicode characters with the parameter `unicodeSetFilter`. This is useful for a
non-internationalized search engine where retaining a set of national characters which are primary letters in a specific
language is wanted. See syntax for the UnicodeSet [here](http://icu-project.org/apiref/icu4j/com/ibm/icu/text/UnicodeSet.html).

The Following example exempts Swedish characters from the folding. Note that the filtered characters are NOT lowercased which is why we add that filter below.

```js
{
    "index" : {
        "analysis" : {
            "analyzer" : {
                "folding" : {
                    "tokenizer" : "standard",
                    "filter" : ["my_icu_folding", "lowercase"]
                }
            }
            "filter" : {
                "my_icu_folding" : {
                    "type" : "icu_folding"
                    "unicodeSetFilter" : "[^åäöÅÄÖ]"
                }
            }
        }
    }
}
```

ICU Tokenizer
-------------

Breaks text into words according to [UAX #29: Unicode Text Segmentation](http://www.unicode.org/reports/tr29/).

```js
{
    "index" : {
        "analysis" : {
            "analyzer" : {
                "tokenized" : {
                    "tokenizer" : "icu_tokenizer",
                }
            }
        }
    }
}
```


ICU Normalization CharFilter
-----------------

Normalizes characters as explained [here](http://userguide.icu-project.org/transforms/normalization).
It registers itself by default under `icu_normalizer` or `icuNormalizer` using the default settings.
Allows for the name parameter to be provided which can include the following values: `nfc`, `nfkc`, and `nfkc_cf`.
Allows for the mode parameter to be provided which can include the following values: `compose` and `decompose`.
Use `decompose` with `nfc` or `nfkc`, to get `nfd` or `nfkd`, respectively.
Here is a sample settings:

```js
{
    "index" : {
        "analysis" : {
            "analyzer" : {
                "normalized" : {
                    "tokenizer" : "keyword",
                    "char_filter" : ["icu_normalizer"]
                }
            }
        }
    }
}
```

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
