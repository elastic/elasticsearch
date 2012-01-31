ICU Analysis for ElasticSearch
==================================

The ICU Analysis plugin integrates Lucene ICU module into elasticsearch, adding ICU relates analysis components.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-analysis-icu/1.1.0`.

    ----------------------------------------
    | ICU Analysis Plugin | ElasticSearch  |
    ----------------------------------------
    | master              | master (0.19)  |
    ----------------------------------------
    | 1.1.0               | 0.18 -> master |
    ----------------------------------------
    | 1.0.0               | 0.18 -> master |
    ----------------------------------------


ICU Normalization
-----------------

Normalizes characters as explained "here":http://userguide.icu-project.org/transforms/normalization. It registers itself by default under @icu_normalizer@ or @icuNormalizer@ using the default settings. Allows for the name parameter to be provided which can include the following values: @nfc@, @nfkc@, and @nfkc_cf@. Here is a sample settings:

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "keyword",
                        "filter" : ["icu_normalizer"]
                    }
                }
            }
        }
    }

ICU Folding
-----------

Folding of unicode characters based on @UTR#30@. It registers itself under @icu_folding@ and @icuFolding@ names. Sample setting:

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "keyword",
                        "filter" : ["icu_folding"]
                    }
                }
            }
        }
    }

ICU Collation
-------------

Uses collation token filter. Allows to either specify the rules for collation (defined "here":http://www.icu-project.org/userguide/Collate_Customization.html) using the @rules@ parameter (can point to a location or expressed in the settings, location can be relative to config location), or using the @language@ parameter (further specialized by country and variant). By default registers under @icu_collation@ or @icuCollation@ and uses the default locale.

Here is a sample settings:

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "keyword",
                        "filter" : ["icu_collation"]
                    }
                }
            }
        }
    }

And here is a sample of custom collation:

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "keyword",
                        "filter" : ["myCollator"]
                    }
                },
                "filter" : {
                    "myCollator" : {
                        "type" : "icu_collation",
                        "language" : "en"
                    }
                }
            }
        }
    }


ICU Tokenizer
-------------

Breaks text into words according to UAX #29: Unicode Text Segmentation ((http://www.unicode.org/reports/tr29/)).

    {
        "index" : {
            "analysis" : {
                "analyzer" : {
                    "collation" : {
                        "tokenizer" : "icu_tokenizer",
                    }
                }
            }
        }
    }

