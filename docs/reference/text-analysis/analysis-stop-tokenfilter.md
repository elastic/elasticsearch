---
navigation_title: "Stop"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-stop-tokenfilter.html
---

# Stop token filter [analysis-stop-tokenfilter]


Removes [stop words](https://en.wikipedia.org/wiki/Stop_words) from a token stream.

When not customized, the filter removes the following English stop words by default:

`a`, `an`, `and`, `are`, `as`, `at`, `be`, `but`, `by`, `for`, `if`, `in`, `into`, `is`, `it`, `no`, `not`, `of`, `on`, `or`, `such`, `that`, `the`, `their`, `then`, `there`, `these`, `they`, `this`, `to`, `was`, `will`, `with`

In addition to English, the `stop` filter supports predefined [stop word lists for several languages](#analysis-stop-tokenfilter-stop-words-by-lang). You can also specify your own stop words as an array or file.

The `stop` filter uses Lucene’s [StopFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/core/StopFilter.md).

## Example [analysis-stop-tokenfilter-analyze-ex]

The following analyze API request uses the `stop` filter to remove the stop words `a` and `the` from `a quick fox jumps over the lazy dog`:

```console
GET /_analyze
{
  "tokenizer": "standard",
  "filter": [ "stop" ],
  "text": "a quick fox jumps over the lazy dog"
}
```

The filter produces the following tokens:

```text
[ quick, fox, jumps, over, lazy, dog ]
```


## Add to an analyzer [analysis-stop-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `stop` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "whitespace",
          "filter": [ "stop" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-stop-tokenfilter-configure-parms]

`stopwords`
:   (Optional, string or array of strings) Language value, such as `_arabic_` or `_thai_`. Defaults to [`_english_`](#english-stop-words).

Each language value corresponds to a predefined list of stop words in Lucene. See [Stop words by language](#analysis-stop-tokenfilter-stop-words-by-lang) for supported language values and their stop words.

Also accepts an array of stop words.

For an empty list of stop words, use `_none_`.


`stopwords_path`
:   (Optional, string) Path to a file that contains a list of stop words to remove.

This path must be absolute or relative to the `config` location, and the file must be UTF-8 encoded. Each stop word in the file must be separated by a line break.


`ignore_case`
:   (Optional, Boolean) If `true`, stop word matching is case insensitive. For example, if `true`, a stop word of `the` matches and removes `The`, `THE`, or `the`. Defaults to `false`.

`remove_trailing`
:   (Optional, Boolean) If `true`, the last token of a stream is removed if it’s a stop word. Defaults to `true`.

This parameter should be `false` when using the filter with a completion suggester. This would ensure a query like `green a` matches and suggests `green apple` while still removing other stop words. For more information about completion suggesters, refer to [](/reference/elasticsearch/rest-apis/search-suggesters.md)

## Customize [analysis-stop-tokenfilter-customize]

To customize the `stop` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom case-insensitive `stop` filter that removes stop words from the [`_english_`](#english-stop-words) stop words list:

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "default": {
          "tokenizer": "whitespace",
          "filter": [ "my_custom_stop_words_filter" ]
        }
      },
      "filter": {
        "my_custom_stop_words_filter": {
          "type": "stop",
          "ignore_case": true
        }
      }
    }
  }
}
```

You can also specify your own list of stop words. For example, the following request creates a custom case-insensitive `stop` filter that removes only the stop words `and`, `is`, and `the`:

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "default": {
          "tokenizer": "whitespace",
          "filter": [ "my_custom_stop_words_filter" ]
        }
      },
      "filter": {
        "my_custom_stop_words_filter": {
          "type": "stop",
          "ignore_case": true,
          "stopwords": [ "and", "is", "the" ]
        }
      }
    }
  }
}
```


## Stop words by language [analysis-stop-tokenfilter-stop-words-by-lang]

The following list contains supported language values for the `stopwords` parameter and a link to their predefined stop words in Lucene.

$$$arabic-stop-words$$$

`_arabic_`
:   [Arabic stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/ar/stopwords.txt)

$$$armenian-stop-words$$$

`_armenian_`
:   [Armenian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/hy/stopwords.txt)

$$$basque-stop-words$$$

`_basque_`
:   [Basque stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/eu/stopwords.txt)

$$$bengali-stop-words$$$

`_bengali_`
:   [Bengali stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/bn/stopwords.txt)

$$$brazilian-stop-words$$$

`_brazilian_` (Brazilian Portuguese)
:   [Brazilian Portuguese stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/br/stopwords.txt)

$$$bulgarian-stop-words$$$

`_bulgarian_`
:   [Bulgarian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/bg/stopwords.txt)

$$$catalan-stop-words$$$

`_catalan_`
:   [Catalan stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/ca/stopwords.txt)

$$$cjk-stop-words$$$

`_cjk_` (Chinese, Japanese, and Korean)
:   [CJK stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/cjk/stopwords.txt)

$$$czech-stop-words$$$

`_czech_`
:   [Czech stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/cz/stopwords.txt)

$$$danish-stop-words$$$

`_danish_`
:   [Danish stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/danish_stop.txt)

$$$dutch-stop-words$$$

`_dutch_`
:   [Dutch stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/dutch_stop.txt)

$$$english-stop-words$$$

`_english_`
:   [English stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/java/org/apache/lucene/analysis/en/EnglishAnalyzer.java#L48)

$$$estonian-stop-words$$$

`_estonian_`
:   [Estonian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/et/stopwords.txt)

$$$finnish-stop-words$$$

`_finnish_`
:   [Finnish stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/finnish_stop.txt)

$$$french-stop-words$$$

`_french_`
:   [French stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/french_stop.txt)

$$$galician-stop-words$$$

`_galician_`
:   [Galician stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/gl/stopwords.txt)

$$$german-stop-words$$$

`_german_`
:   [German stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/german_stop.txt)

$$$greek-stop-words$$$

`_greek_`
:   [Greek stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/el/stopwords.txt)

$$$hindi-stop-words$$$

`_hindi_`
:   [Hindi stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/hi/stopwords.txt)

$$$hungarian-stop-words$$$

`_hungarian_`
:   [Hungarian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/hungarian_stop.txt)

$$$indonesian-stop-words$$$

`_indonesian_`
:   [Indonesian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/id/stopwords.txt)

$$$irish-stop-words$$$

`_irish_`
:   [Irish stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/ga/stopwords.txt)

$$$italian-stop-words$$$

`_italian_`
:   [Italian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/italian_stop.txt)

$$$latvian-stop-words$$$

`_latvian_`
:   [Latvian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/lv/stopwords.txt)

$$$lithuanian-stop-words$$$

`_lithuanian_`
:   [Lithuanian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/lt/stopwords.txt)

$$$norwegian-stop-words$$$

`_norwegian_`
:   [Norwegian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/norwegian_stop.txt)

$$$persian-stop-words$$$

`_persian_`
:   [Persian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/fa/stopwords.txt)

$$$portuguese-stop-words$$$

`_portuguese_`
:   [Portuguese stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/portuguese_stop.txt)

$$$romanian-stop-words$$$

`_romanian_`
:   [Romanian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/ro/stopwords.txt)

$$$russian-stop-words$$$

`_russian_`
:   [Russian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/russian_stop.txt)

$$$serbian-stop-words$$$

`_serbian_`
:   [Serbian stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/sr/stopwords.txt)

$$$sorani-stop-words$$$

`_sorani_`
:   [Sorani stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/ckb/stopwords.txt)

$$$spanish-stop-words$$$

`_spanish_`
:   [Spanish stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/spanish_stop.txt)

$$$swedish-stop-words$$$

`_swedish_`
:   [Swedish stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/snowball/swedish_stop.txt)

$$$thai-stop-words$$$

`_thai_`
:   [Thai stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/th/stopwords.txt)

$$$turkish-stop-words$$$

`_turkish_`
:   [Turkish stop words](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis/tr/stopwords.txt)


