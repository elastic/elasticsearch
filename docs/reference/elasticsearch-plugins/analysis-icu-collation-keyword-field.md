---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu-collation-keyword-field.html
---

# ICU collation keyword field [analysis-icu-collation-keyword-field]

Collations are used for sorting documents in a language-specific word order. The `icu_collation_keyword` field type is available to all indices and will encode the terms directly as bytes in a doc values field and a single indexed token just like a standard [Keyword Field](/reference/elasticsearch/mapping-reference/keyword.md).

Defaults to using [DUCET collation](https://www.elastic.co/guide/en/elasticsearch/guide/2.x/sorting-collations.html#uca), which is a best-effort attempt at language-neutral sorting.

Below is an example of how to set up a field for sorting German names in phonebook order:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "name": {   <1>
        "type": "text",
        "fields": {
          "sort": {  <2>
            "type": "icu_collation_keyword",
            "index": false,
            "language": "de",
            "country": "DE",
            "variant": "@collation=phonebook"
          }
        }
      }
    }
  }
}

GET /my-index-000001/_search <3>
{
  "query": {
    "match": {
      "name": "Fritz"
    }
  },
  "sort": "name.sort"
}
```

1. The `name` field uses the `standard` analyzer, and so supports full text queries.
2. The `name.sort` field is an `icu_collation_keyword` field that will preserve the name as a single token doc values, and applies the German phonebook order.
3. An example query which searches the `name` field and sorts on the `name.sort` field.


## Parameters for ICU collation keyword fields [_parameters_for_icu_collation_keyword_fields]

The following parameters are accepted by `icu_collation_keyword` fields:

`doc_values`
:   Should the field be stored on disk in a column-stride fashion, so that it can later be used for sorting, aggregations, or scripting? Accepts `true` (default) or `false`.

`index`
:   Should the field be searchable? Accepts `true` (default) or `false`.

`null_value`
:   Accepts a string value which is substituted for any explicit `null` values. Defaults to `null`, which means the field is treated as missing.

[`ignore_above`](/reference/elasticsearch/mapping-reference/ignore-above.md)
:   Strings longer than the `ignore_above` setting will be ignored. Checking is performed on the original string before the collation. The `ignore_above` setting can be updated on existing fields using the [PUT mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping). By default, there is no limit and all values will be indexed.

`store`
:   Whether the field value should be stored and retrievable separately from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepts `true` or `false` (default).

`fields`
:   Multi-fields allow the same string value to be indexed in multiple ways for different purposes, such as one field for search and a multi-field for sorting and aggregations.


## Collation options [_collation_options]

`strength`
:   The strength property determines the minimum level of difference considered significant during comparison. Possible values are : `primary`, `secondary`, `tertiary`, `quaternary` or `identical`. See the [ICU Collation documentation](https://icu-project.org/apiref/icu4j/com/ibm/icu/text/Collator.md) for a more detailed explanation for each value. Defaults to `tertiary` unless otherwise specified in the collation.

`decomposition`
:   Possible values: `no` (default, but collation-dependent) or `canonical`. Setting this decomposition property to `canonical` allows the Collator to handle unnormalized text properly, producing the same results as if the text were normalized. If `no` is set, it is the user’s responsibility to ensure that all text is already in the appropriate form before a comparison or before getting a CollationKey. Adjusting decomposition mode allows the user to select between faster and more complete collation behavior. Since a great many of the world’s languages do not require text normalization, most locales set `no` as the default decomposition mode.

The following options are expert only:

`alternate`
:   Possible values: `shifted` or `non-ignorable`. Sets the alternate handling for strength `quaternary` to be either shifted or non-ignorable. Which boils down to ignoring punctuation and whitespace.

`case_level`
:   Possible values: `true` or `false` (default). Whether case level sorting is required. When strength is set to `primary` this will ignore accent differences.

`case_first`
:   Possible values: `lower` or `upper`. Useful to control which case is sorted first when the case is not ignored for strength `tertiary`. The default depends on the collation.

`numeric`
:   Possible values: `true` or `false` (default) . Whether digits are sorted according to their numeric representation. For example the value `egg-9` is sorted before the value `egg-21`.

`variable_top`
:   Single character or contraction. Controls what is variable for `alternate`.

`hiragana_quaternary_mode`
:   Possible values: `true` or `false`. Distinguishing between Katakana and Hiragana characters in `quaternary` strength.


