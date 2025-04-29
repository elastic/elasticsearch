---
navigation_title: "Version"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/version.html
---

# Version field type [version]


The `version` field type is a specialization of the `keyword` field for handling software version values and to support specialized precedence rules for them. Precedence is defined following the rules outlined by [Semantic Versioning](https://semver.org/), which for example means that major, minor and patch version parts are sorted numerically (i.e. "2.1.0" < "2.4.1" < "2.11.2") and pre-release versions are sorted before release versions (i.e. "1.0.0-alpha" < "1.0.0").

You index a `version` field as follows

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_version": {
        "type": "version"
      }
    }
  }
}
```

The field offers the same search capabilities as a regular keyword field. It can e.g. be searched for exact matches using `match` or `term` queries and supports prefix and wildcard searches. The main benefit is that `range` queries will honor Semver ordering, so a `range` query between "1.0.0" and "1.5.0" will include versions of "1.2.3" but not "1.11.2" for example. Note that this would be different when using a regular `keyword` field for indexing where ordering is alphabetical.

Software versions are expected to follow the [Semantic Versioning rules](https://semver.org/) schema and precedence rules with the notable exception that more or less than three main version identifiers are allowed (i.e. "1.2" or "1.2.3.4" qualify as valid versions while they wouldn’t under strict Semver rules). Version strings that are not valid under the Semver definition (e.g. "1.2.alpha.4") can still be indexed and retrieved as exact matches, however they will all appear *after* any valid version with regular alphabetical ordering. The empty String "" is considered invalid and sorted after all valid versions, but before other invalid ones.


### Parameters for version fields [version-params]

The following parameters are accepted by `version` fields:

[`meta`](/reference/elasticsearch/mapping-reference/mapping-field-meta.md)
:   Metadata about the field.


### Limitations [_limitations_3]

This field type isn’t optimized for heavy wildcard, regex, or fuzzy searches. While those types of queries work in this field, you should consider using a regular `keyword` field if you strongly rely on these kinds of queries.

## Synthetic `_source` [version-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices, synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


`version` fields support [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) in their default configuration..

Synthetic source may sort `version` field values and remove duplicates. For example:

$$$synthetic-source-version-example$$$

```console
PUT idx
{
  "settings": {
    "index": {
      "mapping": {
        "source": {
          "mode": "synthetic"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "versions": { "type": "version" }
    }
  }
}
PUT idx/_doc/1
{
  "versions": ["8.0.0-beta1", "8.5.0", "0.90.12", "2.6.1", "1.3.4", "1.3.4"]
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "versions": ["0.90.12", "1.3.4", "2.6.1", "8.0.0-beta1", "8.5.0"]
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]


