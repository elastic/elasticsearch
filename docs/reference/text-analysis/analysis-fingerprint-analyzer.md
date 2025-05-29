---
navigation_title: "Fingerprint"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-fingerprint-analyzer.html
---

# Fingerprint analyzer [analysis-fingerprint-analyzer]


The `fingerprint` analyzer implements a [fingerprinting algorithm](https://github.com/OpenRefine/OpenRefine/wiki/Clustering-In-Depth#fingerprint) which is used by the OpenRefine project to assist in clustering.

Input text is lowercased, normalized to remove extended characters, sorted, deduplicated and concatenated into a single token. If a stopword list is configured, stop words will also be removed.


## Example output [_example_output]

```console
POST _analyze
{
  "analyzer": "fingerprint",
  "text": "Yes yes, Gödel said this sentence is consistent and."
}
```

The above sentence would produce the following single term:

```text
[ and consistent godel is said sentence this yes ]
```


## Configuration [_configuration_2]

The `fingerprint` analyzer accepts the following parameters:

`separator`
:   The character to use to concatenate the terms. Defaults to a space.

`max_output_size`
:   The maximum token size to emit. Defaults to `255`. Tokens larger than this size will be discarded.

`stopwords`
:   A pre-defined stop words list like `_english_` or an array containing a list of stop words. Defaults to `_none_`.

`stopwords_path`
:   The path to a file containing stop words.

See the [Stop Token Filter](/reference/text-analysis/analysis-stop-tokenfilter.md) for more information about stop word configuration.


## Example configuration [_example_configuration_2]

In this example, we configure the `fingerprint` analyzer to use the pre-defined list of English stop words:

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_fingerprint_analyzer": {
          "type": "fingerprint",
          "stopwords": "_english_"
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_fingerprint_analyzer",
  "text": "Yes yes, Gödel said this sentence is consistent and."
}
```

The above example produces the following term:

```text
[ consistent godel said sentence yes ]
```


## Definition [_definition]

The `fingerprint` tokenizer consists of:

Tokenizer
:   * [Standard Tokenizer](/reference/text-analysis/analysis-standard-tokenizer.md)


Token Filters (in order)
:   * [Lower Case Token Filter](/reference/text-analysis/analysis-lowercase-tokenfilter.md)
* [ASCII folding](/reference/text-analysis/analysis-asciifolding-tokenfilter.md)
* [Stop Token Filter](/reference/text-analysis/analysis-stop-tokenfilter.md) (disabled by default)
* [Fingerprint](/reference/text-analysis/analysis-fingerprint-tokenfilter.md)


If you need to customize the `fingerprint` analyzer beyond the configuration parameters then you need to recreate it as a `custom` analyzer and modify it, usually by adding token filters. This would recreate the built-in `fingerprint` analyzer and you can use it as a starting point for further customization:

```console
PUT /fingerprint_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "rebuilt_fingerprint": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "asciifolding",
            "fingerprint"
          ]
        }
      }
    }
  }
}
```
% TEST[s/\n$/\nstartyaml\n  - compare_analyzers: {index: fingerprint_example, first: fingerprint, second: rebuilt_fingerprint}\nendyaml\n/]

