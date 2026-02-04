---
navigation_title: "Intervals"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-intervals-query.html
---

# Intervals query [query-dsl-intervals-query]


Returns documents based on the order and proximity of matching terms.

The `intervals` query uses **matching rules**, constructed from a small set of definitions. These rules are then applied to terms from a specified `field`.

The definitions produce sequences of minimal intervals that span terms in a body of text. These intervals can be further combined and filtered by parent sources.

## Example request [intervals-query-ex-request]

The following `intervals` search returns documents containing `my favorite food` without any gap, followed by `hot water` or `cold porridge` in the `my_text` field.

This search would match a `my_text` value of `my favorite food is cold porridge` but not `when it's cold my favorite food is porridge`.

```console
POST _search
{
  "query": {
    "intervals" : {
      "my_text" : {
        "all_of" : {
          "ordered" : true,
          "intervals" : [
            {
              "match" : {
                "query" : "my favorite food",
                "max_gaps" : 0,
                "ordered" : true
              }
            },
            {
              "any_of" : {
                "intervals" : [
                  { "match" : { "query" : "hot water" } },
                  { "match" : { "query" : "cold porridge" } }
                ]
              }
            }
          ]
        }
      }
    }
  }
}
```


## Top-level parameters for `intervals` [intervals-top-level-params]

$$$intervals-rules$$$

`<field>`
:   (Required, rule object) Field you wish to search.

The value of this parameter is a rule object used to match documents based on matching terms, order, and proximity.

Valid rules include:

* [`match`](#intervals-match)
* [`prefix`](#intervals-prefix)
* [`wildcard`](#intervals-wildcard)
* [`regexp`](#intervals-regexp)
* [`fuzzy`](#intervals-fuzzy)
* [`range`](#intervals-range)
* [`all_of`](#intervals-all_of)
* [`any_of`](#intervals-any_of)



## `match` rule parameters [intervals-match]

The `match` rule matches analyzed text.

`query`
:   (Required, string) Text you wish to find in the provided `<field>`.

`max_gaps`
:   (Optional, integer) Maximum number of positions between the matching terms. Terms further apart than this are not considered matches. Defaults to `-1`.

If unspecified or set to `-1`, there is no width restriction on the match. If set to `0`, the terms must appear next to each other.


`ordered`
:   (Optional, Boolean) If `true`, matching terms must appear in their specified order. Defaults to `false`.

`analyzer`
:   (Optional, string) [analyzer](docs-content://manage-data/data-store/text-analysis.md) used to analyze terms in the `query`. Defaults to the top-level `<field>`'s analyzer.

`filter`
:   (Optional, [interval filter](#interval_filter) rule object) An optional interval filter.

`use_field`
:   (Optional, string) If specified, then match intervals from this field rather than the top-level `<field>`. Terms are analyzed using the search analyzer from this field. This allows you to search across multiple fields as if they were all the same field; for example, you could index the same text into stemmed and unstemmed fields, and search for stemmed tokens near unstemmed ones.


## `prefix` rule parameters [intervals-prefix]

The `prefix` rule matches terms that start with a specified set of characters. This prefix can expand to match at most `indices.query.bool.max_clause_count` [search setting](/reference/elasticsearch/configuration-reference/search-settings.md) terms. If the prefix matches more terms, {{es}} returns an error. You can use the [`index-prefixes`](/reference/elasticsearch/mapping-reference/index-prefixes.md) option in the field mapping to avoid this limit.

`prefix`
:   (Required, string) Beginning characters of terms you wish to find in the top-level `<field>`.

`analyzer`
:   (Optional, string) [analyzer](docs-content://manage-data/data-store/text-analysis.md) used to normalize the `prefix`. Defaults to the top-level `<field>`'s analyzer.

`use_field`
:   (Optional, string) If specified, then match intervals from this field rather than the top-level `<field>`.

The `prefix` is normalized using the search analyzer from this field, unless a separate `analyzer` is specified.



## `wildcard` rule parameters [intervals-wildcard]

The `wildcard` rule matches terms using a wildcard pattern. This pattern can expand to match at most  `indices.query.bool.max_clause_count` [search setting](/reference/elasticsearch/configuration-reference/search-settings.md) terms. If the pattern matches more terms, {{es}} returns an error.

`pattern`
:   (Required, string) Wildcard pattern used to find matching terms.

    This parameter supports two wildcard operators:

    * `?`, which matches any single character
    * `*`, which can match zero or more characters, including an empty one

    ::::{warning}
    Avoid beginning patterns with `*` or `?`. This can increase the iterations needed to find matching terms and slow search performance.
    ::::


`analyzer`
:   (Optional, string) [analyzer](docs-content://manage-data/data-store/text-analysis.md) used to normalize the `pattern`. Defaults to the top-level `<field>`'s analyzer.

`use_field`
:   (Optional, string) If specified, match intervals from this field rather than the top-level `<field>`.

The `pattern` is normalized using the search analyzer from this field, unless `analyzer` is specified separately.



## `regexp` rule parameters [intervals-regexp]

The `regexp` rule matches terms using a regular expression pattern. This pattern can expand to match at most  `indices.query.bool.max_clause_count` [search setting](/reference/elasticsearch/configuration-reference/search-settings.md) terms. If the pattern matches more terms,{{es}} returns an error.

`pattern`
:   (Required, string) Regexp pattern used to find matching terms. For a list of operators supported by the `regexp` pattern, see [Regular expression syntax](/reference/query-languages/query-dsl/regexp-syntax.md).

::::{warning}
Avoid using wildcard patterns, such as `.*` or `.*?+``. This can increase the iterations needed to find matching terms and slow search performance.
::::


`analyzer`
:   (Optional, string) [analyzer](docs-content://manage-data/data-store/text-analysis.md) used to normalize the `pattern`. Defaults to the top-level `<field>`'s analyzer.

`use_field`
:   (Optional, string) If specified, match intervals from this field rather than the top-level `<field>`.

The `pattern` is normalized using the search analyzer from this field, unless `analyzer` is specified separately.



## `fuzzy` rule parameters [intervals-fuzzy]

The `fuzzy` rule matches terms that are similar to the provided term, within an edit distance defined by [Fuzziness](/reference/elasticsearch/rest-apis/common-options.md#fuzziness). If the fuzzy expansion matches more than `indices.query.bool.max_clause_count` [search setting](/reference/elasticsearch/configuration-reference/search-settings.md) terms, {{es}} returns an error.

`term`
:   (Required, string) The term to match

`prefix_length`
:   (Optional, integer) Number of beginning characters left unchanged when creating expansions. Defaults to `0`.

`transpositions`
:   (Optional, Boolean) Indicates whether edits include transpositions of two adjacent characters (ab → ba). Defaults to `true`.

`fuzziness`
:   (Optional, string) Maximum edit distance allowed for matching. See [Fuzziness](/reference/elasticsearch/rest-apis/common-options.md#fuzziness) for valid values and more information. Defaults to `auto`.

`analyzer`
:   (Optional, string) [analyzer](docs-content://manage-data/data-store/text-analysis.md) used to normalize the `term`. Defaults to the top-level `<field>` 's analyzer.

`use_field`
:   (Optional, string) If specified, match intervals from this field rather than the top-level `<field>`.

The `term` is normalized using the search analyzer from this field, unless `analyzer` is specified separately.



## `range` rule parameters [intervals-range]

The `range` rule matches terms contained within a provided range. This range can expand to match at most  `indices.query.bool.max_clause_count` [search setting](/reference/elasticsearch/configuration-reference/search-settings.md) terms. If the range matches more terms,{{es}} returns an error.

`gt`
:   (Optional, string) Greater than: match terms greater than the provided term.

`gte`
:   (Optional, string) Greater than or equal to: match terms greater than or equal to the provided term.

`lt`
:   (Optional, string) Less than: match terms less than the provided term.

`lte`
:   (Optional, string) Less than or equal to: match terms less than or equal to the provided term.

::::{note}
It is required to provide one of `gt` or `gte` params. It is required to provide one of `lt` or `lte` params.
::::


`analyzer`
:   (Optional, string) [analyzer](docs-content://manage-data/data-store/text-analysis.md) used to normalize the `pattern`. Defaults to the top-level `<field>`'s analyzer.

`use_field`
:   (Optional, string) If specified, match intervals from this field rather than the top-level `<field>`.


## `all_of` rule parameters [intervals-all_of]

The `all_of` rule returns matches that span a combination of other rules.

`intervals`
:   (Required, array of rule objects) An array of rules to combine. All rules must produce a match in a document for the overall source to match.

`max_gaps`
:   (Optional, integer) Maximum number of positions between the matching terms. Intervals produced by the rules further apart than this are not considered matches. Defaults to `-1`.

If unspecified or set to `-1`, there is no width restriction on the match. If set to `0`, the terms must appear next to each other.

Internal intervals can have their own `max_gaps` values. In this case we first find internal intervals with their `max_gaps` values, and then combine them to see if a gap between internal intervals match the value of `max_gaps` of the `all_of` rule.

For examples, how `max_gaps` works, see [max_gaps in `all_of` ordered and unordered rule](#interval-max_gaps-all-rule).


`ordered`
:   (Optional, Boolean) If `true`, intervals produced by the rules should appear in the order in which they are specified. Defaults to `false`.

If `ordered` is `false`, intervals can appear in any order, including overlapping with each other.

`filter`
:   (Optional, [interval filter](#interval_filter) rule object) Rule used to filter returned intervals.


## `any_of` rule parameters [intervals-any_of]

The `any_of` rule returns intervals produced by any of its sub-rules.

`intervals`
:   (Required, array of rule objects) An array of rules to match.

`filter`
:   (Optional, [interval filter](#interval_filter) rule object) Rule used to filter returned intervals.


## `filter` rule parameters [interval_filter]

The `filter` rule returns intervals based on a query. See [Filter example](#interval-filter-rule-ex) for an example.

`after`
:   (Optional, query object) Query used to return intervals that follow an interval from the `filter` rule.

`before`
:   (Optional, query object) Query used to return intervals that occur before an interval from the `filter` rule.

`contained_by`
:   (Optional, query object) Query used to return intervals contained by an interval from the `filter` rule.

`containing`
:   (Optional, query object) Query used to return intervals that contain an interval from the `filter` rule.

`not_contained_by`
:   (Optional, query object) Query used to return intervals that are **not** contained by an interval from the `filter` rule.

`not_containing`
:   (Optional, query object) Query used to return intervals that do **not** contain an interval from the `filter` rule.

`not_overlapping`
:   (Optional, query object) Query used to return intervals that do **not** overlap with an interval from the `filter` rule.

`overlapping`
:   (Optional, query object) Query used to return intervals that overlap with an interval from the `filter` rule.

`script`
:   (Optional, [script object](docs-content://explore-analyze/scripting/modules-scripting-using.md)) Script used to return matching documents. This script must return a boolean value, `true` or `false`. See [Script filters](#interval-script-filter) for an example.


## Notes [intervals-query-note]

### Filter example [interval-filter-rule-ex]

The following search includes a `filter` rule. It returns documents that have the words `hot` and `porridge` within 10 positions of each other, without the word `salty` in between:

```console
POST _search
{
  "query": {
    "intervals" : {
      "my_text" : {
        "match" : {
          "query" : "hot porridge",
          "max_gaps" : 10,
          "filter" : {
            "not_containing" : {
              "match" : {
                "query" : "salty"
              }
            }
          }
        }
      }
    }
  }
}
```


### Script filters [interval-script-filter]

You can use a script to filter intervals based on their start position, end position, and internal gap count. The following `filter` script uses the `interval` variable with the `start`, `end`, and `gaps` methods:

```console
POST _search
{
  "query": {
    "intervals" : {
      "my_text" : {
        "match" : {
          "query" : "hot porridge",
          "filter" : {
            "script" : {
              "source" : "interval.start > 10 && interval.end < 20 && interval.gaps == 0"
            }
          }
        }
      }
    }
  }
}
```


### Minimization [interval-minimization]

The intervals query always minimizes intervals, to ensure that queries can run in linear time. This can sometimes cause surprising results, particularly when using `max_gaps` restrictions or filters. For example, take the following query, searching for `salty` contained within the phrase `hot porridge`:

```console
POST _search
{
  "query": {
    "intervals" : {
      "my_text" : {
        "match" : {
          "query" : "salty",
          "filter" : {
            "contained_by" : {
              "match" : {
                "query" : "hot porridge"
              }
            }
          }
        }
      }
    }
  }
}
```

This query does **not** match a document containing the phrase `hot porridge is salty porridge`, because the intervals returned by the match query for `hot porridge` only cover the initial two terms in this document, and these do not overlap the intervals covering `salty`.


### max_gaps in `all_of` ordered and unordered rule [interval-max_gaps-all-rule]

The following `intervals` search returns documents containing `my favorite food` without any gap, followed by `cold porridge` that can have at most 4 tokens between "cold" and "porridge". These two inner intervals when combined in the outer `all_of` interval, must have at most 1 gap between each other.

Because the `all_of` rule has `ordered` set to `true`, the inner intervals are expected to be in the provided order. Thus, this search would match a `my_text` value of `my favorite food is cold porridge` but not `when it's cold my favorite food is porridge`.

```console
POST _search
{
  "query": {
    "intervals" : {
      "my_text" : {
        "all_of" : {
          "ordered" : true,     <1>
          "max_gaps": 1,
          "intervals" : [
            {
              "match" : {
                "query" : "my favorite food",
                "max_gaps" : 0,
                "ordered" : true
              }
            },
            {
              "match" : {
                "query" : "cold porridge",
                "max_gaps" : 4,
                "ordered" : true
              }
            }
          ]
        }
      }
    }
  }
}
```

1. The `ordered` parameter is set to `true`, so intervals must appear in the order specified.


Below is the same query, but with `ordered` set to `false`. This means that intervals can appear in any order, even overlap with each other. Thus, this search would match a `my_text` value of `my favorite food is cold porridge`, as well as `when it's cold my favorite food is porridge`. In `when it's cold my favorite food is porridge`, `cold .... porridge` interval overlaps with `my favorite food` interval.

```console
POST _search
{
  "query": {
    "intervals" : {
      "my_text" : {
        "all_of" : {
          "ordered" : false, <1>
          "max_gaps": 1,
          "intervals" : [
            {
              "match" : {
                "query" : "my favorite food",
                "max_gaps" : 0,
                "ordered" : true
              }
            },
            {
              "match" : {
                "query" : "cold porridge",
                "max_gaps" : 4,
                "ordered" : true
              }
            }
          ]
        }
      }
    }
  }
}
```

1. The `ordered` parameter is set to `true`, so intervals can appear in any order, even overlap with each other.




