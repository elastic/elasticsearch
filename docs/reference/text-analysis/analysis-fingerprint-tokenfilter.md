---
navigation_title: "Fingerprint"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-fingerprint-tokenfilter.html
---

# Fingerprint token filter [analysis-fingerprint-tokenfilter]


Sorts and removes duplicate tokens from a token stream, then concatenates the stream into a single output token.

For example, this filter changes the `[ the, fox, was, very, very, quick ]` token stream as follows:

1. Sorts the tokens alphabetically to `[ fox, quick, the, very, very, was ]`
2. Removes a duplicate instance of the `very` token.
3. Concatenates the token stream to a output single token: `[fox quick the very was ]`

Output tokens produced by this filter are useful for fingerprinting and clustering a body of text as described in the [OpenRefine project](https://github.com/OpenRefine/OpenRefine/wiki/Clustering-In-Depth#fingerprint).

This filter uses Lucene’s [FingerprintFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/miscellaneous/FingerprintFilter.md).

## Example [analysis-fingerprint-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `fingerprint` filter to create a single output token for the text `zebra jumps over resting resting dog`:

```console
GET _analyze
{
  "tokenizer" : "whitespace",
  "filter" : ["fingerprint"],
  "text" : "zebra jumps over resting resting dog"
}
```

The filter produces the following token:

```text
[ dog jumps over resting zebra ]
```


## Add to an analyzer [analysis-fingerprint-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `fingerprint` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT fingerprint_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "whitespace_fingerprint": {
          "tokenizer": "whitespace",
          "filter": [ "fingerprint" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-fingerprint-tokenfilter-configure-parms]

$$$analysis-fingerprint-tokenfilter-max-size$$$

`max_output_size`
:   (Optional, integer) Maximum character length, including whitespace, of the output token. Defaults to `255`. Concatenated tokens longer than this will result in no token output.

`separator`
:   (Optional, string) Character to use to concatenate the token stream input. Defaults to a space.


## Customize [analysis-fingerprint-tokenfilter-customize]

To customize the `fingerprint` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom `fingerprint` filter with that use `+` to concatenate token streams. The filter also limits output tokens to `100` characters or fewer.

```console
PUT custom_fingerprint_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "whitespace_": {
          "tokenizer": "whitespace",
          "filter": [ "fingerprint_plus_concat" ]
        }
      },
      "filter": {
        "fingerprint_plus_concat": {
          "type": "fingerprint",
          "max_output_size": 100,
          "separator": "+"
        }
      }
    }
  }
}
```


