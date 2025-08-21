---
navigation_title: "Hunspell"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-hunspell-tokenfilter.html
---

# Hunspell token filter [analysis-hunspell-tokenfilter]


Provides [dictionary stemming](docs-content://manage-data/data-store/text-analysis/stemming.md#dictionary-stemmers) based on a provided [Hunspell dictionary](https://en.wikipedia.org/wiki/Hunspell). The `hunspell` filter requires [configuration](#analysis-hunspell-tokenfilter-dictionary-config) of one or more language-specific Hunspell dictionaries.

This filter uses Lucene’s [HunspellStemFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/hunspell/HunspellStemFilter.md).

::::{tip}
If available, we recommend trying an algorithmic stemmer for your language before using the `hunspell` token filter. In practice, algorithmic stemmers typically outperform dictionary stemmers. See [Dictionary stemmers](docs-content://manage-data/data-store/text-analysis/stemming.md#dictionary-stemmers).

::::


## Configure Hunspell dictionaries [analysis-hunspell-tokenfilter-dictionary-config]

Hunspell dictionaries are stored and detected on a dedicated `hunspell` directory on the filesystem: `<$ES_PATH_CONF>/hunspell`. Each dictionary is expected to have its own directory, named after its associated language and locale (e.g., `pt_BR`, `en_GB`). This dictionary directory is expected to hold a single `.aff` and one or more `.dic` files, all of which will automatically be picked up. For example, the following directory layout will define the `en_US` dictionary:

```txt
- config
    |-- hunspell
    |    |-- en_US
    |    |    |-- en_US.dic
    |    |    |-- en_US.aff
```

Each dictionary can be configured with one setting:

$$$analysis-hunspell-ignore-case-settings$$$

`ignore_case`
:   (Static, Boolean) If true, dictionary matching will be case insensitive. Defaults to `false`.

    This setting can be configured globally in `elasticsearch.yml` using `indices.analysis.hunspell.dictionary.ignore_case`.

    To configure the setting for a specific locale, use the `indices.analysis.hunspell.dictionary.<locale>.ignore_case` setting (e.g., for the `en_US` (American English) locale, the setting is `indices.analysis.hunspell.dictionary.en_US.ignore_case`).

    You can also add a `settings.yml` file under the dictionary directory which holds these settings. This overrides any other `ignore_case` settings defined in `elasticsearch.yml`.



## Example [analysis-hunspell-tokenfilter-analyze-ex]

The following analyze API request uses the `hunspell` filter to stem `the foxes jumping quickly` to `the fox jump quick`.

The request specifies the `en_US` locale, meaning that the `.aff` and `.dic` files in the `<$ES_PATH_CONF>/hunspell/en_US` directory are used for the Hunspell dictionary.

```console
GET /_analyze
{
  "tokenizer": "standard",
  "filter": [
    {
      "type": "hunspell",
      "locale": "en_US"
    }
  ],
  "text": "the foxes jumping quickly"
}
```

The filter produces the following tokens:

```text
[ the, fox, jump, quick ]
```


## Configurable parameters [analysis-hunspell-tokenfilter-configure-parms]

$$$analysis-hunspell-tokenfilter-dictionary-param$$$

`dictionary`
:   (Optional, string or array of strings) One or more `.dic` files (e.g, `en_US.dic, my_custom.dic`) to use for the Hunspell dictionary.

    By default, the `hunspell` filter uses all `.dic` files in the `<$ES_PATH_CONF>/hunspell/<locale>` directory specified using the `lang`, `language`, or `locale` parameter.


`dedup`
:   (Optional, Boolean) If `true`, duplicate tokens are removed from the filter’s output. Defaults to `true`.

`lang`
:   (Required*, string) An alias for the [`locale` parameter](#analysis-hunspell-tokenfilter-locale-param).

    If this parameter is not specified, the `language` or `locale` parameter is required.


`language`
:   (Required*, string) An alias for the [`locale` parameter](#analysis-hunspell-tokenfilter-locale-param).

    If this parameter is not specified, the `lang` or `locale` parameter is required.


$$$analysis-hunspell-tokenfilter-locale-param$$$

`locale`
:   (Required*, string) Locale directory used to specify the `.aff` and `.dic` files for a Hunspell dictionary. See [Configure Hunspell dictionaries](#analysis-hunspell-tokenfilter-dictionary-config).

    If this parameter is not specified, the `lang` or `language` parameter is required.


`longest_only`
:   (Optional, Boolean) If `true`, only the longest stemmed version of each token is included in the output. If `false`, all stemmed versions of the token are included. Defaults to `false`.


## Customize and add to an analyzer [analysis-hunspell-tokenfilter-analyzer-ex]

To customize the `hunspell` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses a custom `hunspell` filter, `my_en_US_dict_stemmer`, to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

The `my_en_US_dict_stemmer` filter uses a `locale` of `en_US`, meaning that the `.aff` and `.dic` files in the `<$ES_PATH_CONF>/hunspell/en_US` directory are used. The filter also includes a `dedup` argument of `false`, meaning that duplicate tokens added from the dictionary are not removed from the filter’s output.

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "en": {
          "tokenizer": "standard",
          "filter": [ "my_en_US_dict_stemmer" ]
        }
      },
      "filter": {
        "my_en_US_dict_stemmer": {
          "type": "hunspell",
          "locale": "en_US",
          "dedup": false
        }
      }
    }
  }
}
```


## Settings [analysis-hunspell-tokenfilter-settings]

In addition to the [`ignore_case` settings](#analysis-hunspell-ignore-case-settings), you can configure the following global settings for the `hunspell` filter using `elasticsearch.yml`:

`indices.analysis.hunspell.dictionary.lazy`
:   (Static, Boolean) If `true`, the loading of Hunspell dictionaries is deferred until a dictionary is used. If `false`, the dictionary directory is checked for dictionaries when the node starts, and any dictionaries are automatically loaded. Defaults to `false`.


