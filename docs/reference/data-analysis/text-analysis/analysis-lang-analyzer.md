---
navigation_title: "Language"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-lang-analyzer.html
---

# Language analyzers [analysis-lang-analyzer]


A set of analyzers aimed at analyzing specific language text. The following types are supported: [`arabic`](#arabic-analyzer), [`armenian`](#armenian-analyzer), [`basque`](#basque-analyzer), [`bengali`](#bengali-analyzer), [`brazilian`](#brazilian-analyzer), [`bulgarian`](#bulgarian-analyzer), [`catalan`](#catalan-analyzer), [`cjk`](#cjk-analyzer), [`czech`](#czech-analyzer), [`danish`](#danish-analyzer), [`dutch`](#dutch-analyzer), [`english`](#english-analyzer), [`estonian`](#estonian-analyzer), [`finnish`](#finnish-analyzer), [`french`](#french-analyzer), [`galician`](#galician-analyzer), [`german`](#german-analyzer), [`greek`](#greek-analyzer), [`hindi`](#hindi-analyzer), [`hungarian`](#hungarian-analyzer), [`indonesian`](#indonesian-analyzer), [`irish`](#irish-analyzer), [`italian`](#italian-analyzer), [`latvian`](#latvian-analyzer), [`lithuanian`](#lithuanian-analyzer), [`norwegian`](#norwegian-analyzer), [`persian`](#persian-analyzer), [`portuguese`](#portuguese-analyzer), [`romanian`](#romanian-analyzer), [`russian`](#russian-analyzer), [`serbian`](#serbian-analyzer), [`sorani`](#sorani-analyzer), [`spanish`](#spanish-analyzer), [`swedish`](#swedish-analyzer), [`turkish`](#turkish-analyzer), [`thai`](#thai-analyzer).

## Configuring language analyzers [_configuring_language_analyzers]

### Stopwords [_stopwords]

All analyzers support setting custom `stopwords` either internally in the config, or by using an external stopwords file by setting `stopwords_path`. Check [Stop Analyzer](/reference/data-analysis/text-analysis/analysis-stop-analyzer.md) for more details.


### Excluding words from stemming [_excluding_words_from_stemming]

The `stem_exclusion` parameter allows you to specify an array of lowercase words that should not be stemmed. Internally, this functionality is implemented by adding the [`keyword_marker` token filter](/reference/data-analysis/text-analysis/analysis-keyword-marker-tokenfilter.md) with the `keywords` set to the value of the `stem_exclusion` parameter.

The following analyzers support setting custom `stem_exclusion` list: `arabic`, `armenian`, `basque`, `bengali`, `bulgarian`, `catalan`, `czech`, `dutch`, `english`, `finnish`, `french`, `galician`, `german`, `hindi`, `hungarian`, `indonesian`, `irish`, `italian`, `latvian`, `lithuanian`, `norwegian`, `portuguese`, `romanian`, `russian`, `serbian`, `sorani`, `spanish`, `swedish`, `turkish`.



## Reimplementing language analyzers [_reimplementing_language_analyzers]

The built-in language analyzers can be reimplemented as `custom` analyzers (as described below) in order to customize their behaviour.

::::{note}
If you do not intend to exclude words from being stemmed (the equivalent of the `stem_exclusion` parameter above), then you should remove the `keyword_marker` token filter from the custom analyzer configuration.
::::


### `arabic` analyzer [arabic-analyzer]

The `arabic` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /arabic_example
{
  "settings": {
    "analysis": {
      "filter": {
        "arabic_stop": {
          "type":       "stop",
          "stopwords":  "_arabic_" <1>
        },
        "arabic_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["مثال"] <2>
        },
        "arabic_stemmer": {
          "type":       "stemmer",
          "language":   "arabic"
        }
      },
      "analyzer": {
        "rebuilt_arabic": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "decimal_digit",
            "arabic_stop",
            "arabic_normalization",
            "arabic_keywords",
            "arabic_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `armenian` analyzer [armenian-analyzer]

The `armenian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /armenian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "armenian_stop": {
          "type":       "stop",
          "stopwords":  "_armenian_" <1>
        },
        "armenian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["օրինակ"] <2>
        },
        "armenian_stemmer": {
          "type":       "stemmer",
          "language":   "armenian"
        }
      },
      "analyzer": {
        "rebuilt_armenian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "armenian_stop",
            "armenian_keywords",
            "armenian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `basque` analyzer [basque-analyzer]

The `basque` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /basque_example
{
  "settings": {
    "analysis": {
      "filter": {
        "basque_stop": {
          "type":       "stop",
          "stopwords":  "_basque_" <1>
        },
        "basque_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["Adibidez"] <2>
        },
        "basque_stemmer": {
          "type":       "stemmer",
          "language":   "basque"
        }
      },
      "analyzer": {
        "rebuilt_basque": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "basque_stop",
            "basque_keywords",
            "basque_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `bengali` analyzer [bengali-analyzer]

The `bengali` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /bengali_example
{
  "settings": {
    "analysis": {
      "filter": {
        "bengali_stop": {
          "type":       "stop",
          "stopwords":  "_bengali_" <1>
        },
        "bengali_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["উদাহরণ"] <2>
        },
        "bengali_stemmer": {
          "type":       "stemmer",
          "language":   "bengali"
        }
      },
      "analyzer": {
        "rebuilt_bengali": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "decimal_digit",
            "bengali_keywords",
            "indic_normalization",
            "bengali_normalization",
            "bengali_stop",
            "bengali_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `brazilian` analyzer [brazilian-analyzer]

The `brazilian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /brazilian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "brazilian_stop": {
          "type":       "stop",
          "stopwords":  "_brazilian_" <1>
        },
        "brazilian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["exemplo"] <2>
        },
        "brazilian_stemmer": {
          "type":       "stemmer",
          "language":   "brazilian"
        }
      },
      "analyzer": {
        "rebuilt_brazilian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "brazilian_stop",
            "brazilian_keywords",
            "brazilian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `bulgarian` analyzer [bulgarian-analyzer]

The `bulgarian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /bulgarian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "bulgarian_stop": {
          "type":       "stop",
          "stopwords":  "_bulgarian_" <1>
        },
        "bulgarian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["пример"] <2>
        },
        "bulgarian_stemmer": {
          "type":       "stemmer",
          "language":   "bulgarian"
        }
      },
      "analyzer": {
        "rebuilt_bulgarian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "bulgarian_stop",
            "bulgarian_keywords",
            "bulgarian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `catalan` analyzer [catalan-analyzer]

The `catalan` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /catalan_example
{
  "settings": {
    "analysis": {
      "filter": {
        "catalan_elision": {
          "type":       "elision",
          "articles":   [ "d", "l", "m", "n", "s", "t"],
          "articles_case": true
        },
        "catalan_stop": {
          "type":       "stop",
          "stopwords":  "_catalan_" <1>
        },
        "catalan_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["example"] <2>
        },
        "catalan_stemmer": {
          "type":       "stemmer",
          "language":   "catalan"
        }
      },
      "analyzer": {
        "rebuilt_catalan": {
          "tokenizer":  "standard",
          "filter": [
            "catalan_elision",
            "lowercase",
            "catalan_stop",
            "catalan_keywords",
            "catalan_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `cjk` analyzer [cjk-analyzer]

::::{note}
You may find that `icu_analyzer` in the ICU analysis plugin works better for CJK text than the `cjk` analyzer. Experiment with your text and queries.
::::


The `cjk` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /cjk_example
{
  "settings": {
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  [ <1>
            "a", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on",
            "or", "s", "such", "t", "that", "the", "their", "then",
            "there", "these", "they", "this", "to", "was", "will",
            "with", "www"
          ]
        }
      },
      "analyzer": {
        "rebuilt_cjk": {
          "tokenizer":  "standard",
          "filter": [
            "cjk_width",
            "lowercase",
            "cjk_bigram",
            "english_stop"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters. The default stop words are **almost** the same as the `_english_` set, but not exactly the same.



### `czech` analyzer [czech-analyzer]

The `czech` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /czech_example
{
  "settings": {
    "analysis": {
      "filter": {
        "czech_stop": {
          "type":       "stop",
          "stopwords":  "_czech_" <1>
        },
        "czech_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["příklad"] <2>
        },
        "czech_stemmer": {
          "type":       "stemmer",
          "language":   "czech"
        }
      },
      "analyzer": {
        "rebuilt_czech": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "czech_stop",
            "czech_keywords",
            "czech_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `danish` analyzer [danish-analyzer]

The `danish` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /danish_example
{
  "settings": {
    "analysis": {
      "filter": {
        "danish_stop": {
          "type":       "stop",
          "stopwords":  "_danish_" <1>
        },
        "danish_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["eksempel"] <2>
        },
        "danish_stemmer": {
          "type":       "stemmer",
          "language":   "danish"
        }
      },
      "analyzer": {
        "rebuilt_danish": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "danish_stop",
            "danish_keywords",
            "danish_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `dutch` analyzer [dutch-analyzer]

The `dutch` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /dutch_example
{
  "settings": {
    "analysis": {
      "filter": {
        "dutch_stop": {
          "type":       "stop",
          "stopwords":  "_dutch_" <1>
        },
        "dutch_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["voorbeeld"] <2>
        },
        "dutch_stemmer": {
          "type":       "stemmer",
          "language":   "dutch"
        },
        "dutch_override": {
          "type":       "stemmer_override",
          "rules": [
            "fiets=>fiets",
            "bromfiets=>bromfiets",
            "ei=>eier",
            "kind=>kinder"
          ]
        }
      },
      "analyzer": {
        "rebuilt_dutch": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "dutch_stop",
            "dutch_keywords",
            "dutch_override",
            "dutch_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `english` analyzer [english-analyzer]

The `english` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /english_example
{
  "settings": {
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_" <1>
        },
        "english_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["example"] <2>
        },
        "english_stemmer": {
          "type":       "stemmer",
          "language":   "english"
        },
        "english_possessive_stemmer": {
          "type":       "stemmer",
          "language":   "possessive_english"
        }
      },
      "analyzer": {
        "rebuilt_english": {
          "tokenizer":  "standard",
          "filter": [
            "english_possessive_stemmer",
            "lowercase",
            "english_stop",
            "english_keywords",
            "english_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `estonian` analyzer [estonian-analyzer]

The `estonian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /estonian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "estonian_stop": {
          "type":       "stop",
          "stopwords":  "_estonian_" <1>
        },
        "estonian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["näide"] <2>
        },
        "estonian_stemmer": {
          "type":       "stemmer",
          "language":   "estonian"
        }
      },
      "analyzer": {
        "rebuilt_estonian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "estonian_stop",
            "estonian_keywords",
            "estonian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `finnish` analyzer [finnish-analyzer]

The `finnish` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /finnish_example
{
  "settings": {
    "analysis": {
      "filter": {
        "finnish_stop": {
          "type":       "stop",
          "stopwords":  "_finnish_" <1>
        },
        "finnish_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["esimerkki"] <2>
        },
        "finnish_stemmer": {
          "type":       "stemmer",
          "language":   "finnish"
        }
      },
      "analyzer": {
        "rebuilt_finnish": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "finnish_stop",
            "finnish_keywords",
            "finnish_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `french` analyzer [french-analyzer]

The `french` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /french_example
{
  "settings": {
    "analysis": {
      "filter": {
        "french_elision": {
          "type":         "elision",
          "articles_case": true,
          "articles": [
              "l", "m", "t", "qu", "n", "s",
              "j", "d", "c", "jusqu", "quoiqu",
              "lorsqu", "puisqu"
            ]
        },
        "french_stop": {
          "type":       "stop",
          "stopwords":  "_french_" <1>
        },
        "french_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["Example"] <2>
        },
        "french_stemmer": {
          "type":       "stemmer",
          "language":   "light_french"
        }
      },
      "analyzer": {
        "rebuilt_french": {
          "tokenizer":  "standard",
          "filter": [
            "french_elision",
            "lowercase",
            "french_stop",
            "french_keywords",
            "french_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `galician` analyzer [galician-analyzer]

The `galician` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /galician_example
{
  "settings": {
    "analysis": {
      "filter": {
        "galician_stop": {
          "type":       "stop",
          "stopwords":  "_galician_" <1>
        },
        "galician_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["exemplo"] <2>
        },
        "galician_stemmer": {
          "type":       "stemmer",
          "language":   "galician"
        }
      },
      "analyzer": {
        "rebuilt_galician": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "galician_stop",
            "galician_keywords",
            "galician_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `german` analyzer [german-analyzer]

The `german` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /german_example
{
  "settings": {
    "analysis": {
      "filter": {
        "german_stop": {
          "type":       "stop",
          "stopwords":  "_german_" <1>
        },
        "german_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["Beispiel"] <2>
        },
        "german_stemmer": {
          "type":       "stemmer",
          "language":   "light_german"
        }
      },
      "analyzer": {
        "rebuilt_german": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "german_stop",
            "german_keywords",
            "german_normalization",
            "german_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `greek` analyzer [greek-analyzer]

The `greek` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /greek_example
{
  "settings": {
    "analysis": {
      "filter": {
        "greek_stop": {
          "type":       "stop",
          "stopwords":  "_greek_" <1>
        },
        "greek_lowercase": {
          "type":       "lowercase",
          "language":   "greek"
        },
        "greek_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["παράδειγμα"] <2>
        },
        "greek_stemmer": {
          "type":       "stemmer",
          "language":   "greek"
        }
      },
      "analyzer": {
        "rebuilt_greek": {
          "tokenizer":  "standard",
          "filter": [
            "greek_lowercase",
            "greek_stop",
            "greek_keywords",
            "greek_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `hindi` analyzer [hindi-analyzer]

The `hindi` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /hindi_example
{
  "settings": {
    "analysis": {
      "filter": {
        "hindi_stop": {
          "type":       "stop",
          "stopwords":  "_hindi_" <1>
        },
        "hindi_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["उदाहरण"] <2>
        },
        "hindi_stemmer": {
          "type":       "stemmer",
          "language":   "hindi"
        }
      },
      "analyzer": {
        "rebuilt_hindi": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "decimal_digit",
            "hindi_keywords",
            "indic_normalization",
            "hindi_normalization",
            "hindi_stop",
            "hindi_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `hungarian` analyzer [hungarian-analyzer]

The `hungarian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /hungarian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "hungarian_stop": {
          "type":       "stop",
          "stopwords":  "_hungarian_" <1>
        },
        "hungarian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["példa"] <2>
        },
        "hungarian_stemmer": {
          "type":       "stemmer",
          "language":   "hungarian"
        }
      },
      "analyzer": {
        "rebuilt_hungarian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "hungarian_stop",
            "hungarian_keywords",
            "hungarian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `indonesian` analyzer [indonesian-analyzer]

The `indonesian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /indonesian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "indonesian_stop": {
          "type":       "stop",
          "stopwords":  "_indonesian_" <1>
        },
        "indonesian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["contoh"] <2>
        },
        "indonesian_stemmer": {
          "type":       "stemmer",
          "language":   "indonesian"
        }
      },
      "analyzer": {
        "rebuilt_indonesian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "indonesian_stop",
            "indonesian_keywords",
            "indonesian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `irish` analyzer [irish-analyzer]

The `irish` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /irish_example
{
  "settings": {
    "analysis": {
      "filter": {
        "irish_hyphenation": {
          "type":       "stop",
          "stopwords":  [ "h", "n", "t" ],
          "ignore_case": true
        },
        "irish_elision": {
          "type":       "elision",
          "articles":   [ "d", "m", "b" ],
          "articles_case": true
        },
        "irish_stop": {
          "type":       "stop",
          "stopwords":  "_irish_" <1>
        },
        "irish_lowercase": {
          "type":       "lowercase",
          "language":   "irish"
        },
        "irish_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["sampla"] <2>
        },
        "irish_stemmer": {
          "type":       "stemmer",
          "language":   "irish"
        }
      },
      "analyzer": {
        "rebuilt_irish": {
          "tokenizer":  "standard",
          "filter": [
            "irish_hyphenation",
            "irish_elision",
            "irish_lowercase",
            "irish_stop",
            "irish_keywords",
            "irish_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `italian` analyzer [italian-analyzer]

The `italian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /italian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "italian_elision": {
          "type": "elision",
          "articles": [
                "c", "l", "all", "dall", "dell",
                "nell", "sull", "coll", "pell",
                "gl", "agl", "dagl", "degl", "negl",
                "sugl", "un", "m", "t", "s", "v", "d"
          ],
          "articles_case": true
        },
        "italian_stop": {
          "type":       "stop",
          "stopwords":  "_italian_" <1>
        },
        "italian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["esempio"] <2>
        },
        "italian_stemmer": {
          "type":       "stemmer",
          "language":   "light_italian"
        }
      },
      "analyzer": {
        "rebuilt_italian": {
          "tokenizer":  "standard",
          "filter": [
            "italian_elision",
            "lowercase",
            "italian_stop",
            "italian_keywords",
            "italian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `latvian` analyzer [latvian-analyzer]

The `latvian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /latvian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "latvian_stop": {
          "type":       "stop",
          "stopwords":  "_latvian_" <1>
        },
        "latvian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["piemērs"] <2>
        },
        "latvian_stemmer": {
          "type":       "stemmer",
          "language":   "latvian"
        }
      },
      "analyzer": {
        "rebuilt_latvian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "latvian_stop",
            "latvian_keywords",
            "latvian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `lithuanian` analyzer [lithuanian-analyzer]

The `lithuanian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /lithuanian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "lithuanian_stop": {
          "type":       "stop",
          "stopwords":  "_lithuanian_" <1>
        },
        "lithuanian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["pavyzdys"] <2>
        },
        "lithuanian_stemmer": {
          "type":       "stemmer",
          "language":   "lithuanian"
        }
      },
      "analyzer": {
        "rebuilt_lithuanian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "lithuanian_stop",
            "lithuanian_keywords",
            "lithuanian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `norwegian` analyzer [norwegian-analyzer]

The `norwegian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /norwegian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "norwegian_stop": {
          "type":       "stop",
          "stopwords":  "_norwegian_" <1>
        },
        "norwegian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["eksempel"] <2>
        },
        "norwegian_stemmer": {
          "type":       "stemmer",
          "language":   "norwegian"
        }
      },
      "analyzer": {
        "rebuilt_norwegian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "norwegian_stop",
            "norwegian_keywords",
            "norwegian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `persian` analyzer [persian-analyzer]

The `persian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /persian_example
{
  "settings": {
    "analysis": {
      "char_filter": {
        "zero_width_spaces": {
            "type":       "mapping",
            "mappings": [ "\\u200C=>\\u0020"] <1>
        }
      },
      "filter": {
        "persian_stop": {
          "type":       "stop",
          "stopwords":  "_persian_" <2>
        }
      },
      "analyzer": {
        "rebuilt_persian": {
          "tokenizer":     "standard",
          "char_filter": [ "zero_width_spaces" ],
          "filter": [
            "lowercase",
            "decimal_digit",
            "arabic_normalization",
            "persian_normalization",
            "persian_stop",
            "persian_stem"
          ]
        }
      }
    }
  }
}
```

1. Replaces zero-width non-joiners with an ASCII space.
2. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.



### `portuguese` analyzer [portuguese-analyzer]

The `portuguese` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /portuguese_example
{
  "settings": {
    "analysis": {
      "filter": {
        "portuguese_stop": {
          "type":       "stop",
          "stopwords":  "_portuguese_" <1>
        },
        "portuguese_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["exemplo"] <2>
        },
        "portuguese_stemmer": {
          "type":       "stemmer",
          "language":   "light_portuguese"
        }
      },
      "analyzer": {
        "rebuilt_portuguese": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "portuguese_stop",
            "portuguese_keywords",
            "portuguese_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `romanian` analyzer [romanian-analyzer]

The `romanian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /romanian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "romanian_stop": {
          "type":       "stop",
          "stopwords":  "_romanian_" <1>
        },
        "romanian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["exemplu"] <2>
        },
        "romanian_stemmer": {
          "type":       "stemmer",
          "language":   "romanian"
        }
      },
      "analyzer": {
        "rebuilt_romanian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "romanian_stop",
            "romanian_keywords",
            "romanian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `russian` analyzer [russian-analyzer]

The `russian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /russian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "russian_stop": {
          "type":       "stop",
          "stopwords":  "_russian_" <1>
        },
        "russian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["пример"] <2>
        },
        "russian_stemmer": {
          "type":       "stemmer",
          "language":   "russian"
        }
      },
      "analyzer": {
        "rebuilt_russian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "russian_stop",
            "russian_keywords",
            "russian_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `serbian` analyzer [serbian-analyzer]

The `serbian` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /serbian_example
{
  "settings": {
    "analysis": {
      "filter": {
        "serbian_stop": {
          "type":       "stop",
          "stopwords":  "_serbian_" <1>
        },
        "serbian_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["пример"] <2>
        },
        "serbian_stemmer": {
          "type":       "stemmer",
          "language":   "serbian"
        }
      },
      "analyzer": {
        "rebuilt_serbian": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "serbian_stop",
            "serbian_keywords",
            "serbian_stemmer",
            "serbian_normalization"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `sorani` analyzer [sorani-analyzer]

The `sorani` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /sorani_example
{
  "settings": {
    "analysis": {
      "filter": {
        "sorani_stop": {
          "type":       "stop",
          "stopwords":  "_sorani_" <1>
        },
        "sorani_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["mînak"] <2>
        },
        "sorani_stemmer": {
          "type":       "stemmer",
          "language":   "sorani"
        }
      },
      "analyzer": {
        "rebuilt_sorani": {
          "tokenizer":  "standard",
          "filter": [
            "sorani_normalization",
            "lowercase",
            "decimal_digit",
            "sorani_stop",
            "sorani_keywords",
            "sorani_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `spanish` analyzer [spanish-analyzer]

The `spanish` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /spanish_example
{
  "settings": {
    "analysis": {
      "filter": {
        "spanish_stop": {
          "type":       "stop",
          "stopwords":  "_spanish_" <1>
        },
        "spanish_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["ejemplo"] <2>
        },
        "spanish_stemmer": {
          "type":       "stemmer",
          "language":   "light_spanish"
        }
      },
      "analyzer": {
        "rebuilt_spanish": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "spanish_stop",
            "spanish_keywords",
            "spanish_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `swedish` analyzer [swedish-analyzer]

The `swedish` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /swedish_example
{
  "settings": {
    "analysis": {
      "filter": {
        "swedish_stop": {
          "type":       "stop",
          "stopwords":  "_swedish_" <1>
        },
        "swedish_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["exempel"] <2>
        },
        "swedish_stemmer": {
          "type":       "stemmer",
          "language":   "swedish"
        }
      },
      "analyzer": {
        "rebuilt_swedish": {
          "tokenizer":  "standard",
          "filter": [
            "lowercase",
            "swedish_stop",
            "swedish_keywords",
            "swedish_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `turkish` analyzer [turkish-analyzer]

The `turkish` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /turkish_example
{
  "settings": {
    "analysis": {
      "filter": {
        "turkish_stop": {
          "type":       "stop",
          "stopwords":  "_turkish_" <1>
        },
        "turkish_lowercase": {
          "type":       "lowercase",
          "language":   "turkish"
        },
        "turkish_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["örnek"] <2>
        },
        "turkish_stemmer": {
          "type":       "stemmer",
          "language":   "turkish"
        }
      },
      "analyzer": {
        "rebuilt_turkish": {
          "tokenizer":  "standard",
          "filter": [
            "apostrophe",
            "turkish_lowercase",
            "turkish_stop",
            "turkish_keywords",
            "turkish_stemmer"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.
2. This filter should be removed unless there are words which should be excluded from stemming.



### `thai` analyzer [thai-analyzer]

The `thai` analyzer could be reimplemented as a `custom` analyzer as follows:

```console
PUT /thai_example
{
  "settings": {
    "analysis": {
      "filter": {
        "thai_stop": {
          "type":       "stop",
          "stopwords":  "_thai_" <1>
        }
      },
      "analyzer": {
        "rebuilt_thai": {
          "tokenizer":  "thai",
          "filter": [
            "lowercase",
            "decimal_digit",
            "thai_stop"
          ]
        }
      }
    }
  }
}
```

1. The default stopwords can be overridden with the `stopwords` or `stopwords_path` parameters.




