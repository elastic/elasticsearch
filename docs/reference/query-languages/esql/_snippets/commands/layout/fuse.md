```yaml {applies_to}
serverless: ga
stack: preview 9.2-9.4, ga 9.5+
```

The `FUSE` [processing command](/reference/query-languages/esql/commands/processing-commands.md) merges rows from multiple result sets and assigns
new relevance scores.

`FUSE` enables [hybrid search](/reference/query-languages/esql/esql-search-tutorial.md#perform-hybrid-search) to combine and score results from multiple queries, together with the [`FORK`](/reference/query-languages/esql/commands/fork.md) command.

`FUSE` works by:

1. Merging rows with matching `<key_columns>` values
2. Assigning new relevance scores using the specified `<fuse_method>` algorithm
and the values from the `<group_column>` and `<score_column>`

:::::{tip}
`FUSE` is for search use cases: it merges ranked result sets and computes relevance.
Learn more about [how search works in ES|QL](docs-content://solutions/search/esql-for-search.md#how-search-works-in-esql).
:::::

`FUSE` can also normalize scores, making it simple to apply a minimum score cutoff and filter out low-quality results.

A `LIMIT` is required before `FUSE`, because `FUSE` can only work with a finite set of rows.

::::{applies-switch}

:::{applies-item} { serverless: ga , stack: ga 9.4+ }
`FORK` branches do not have an implicit `LIMIT 1000`.
When using `FUSE` after `FORK`, a `LIMIT` must be added to each `FORK` branch.
:::

:::{applies-item} stack: preview 9.1-9.3
An implicit `LIMIT 1000` is added to each `FORK` branch.
When using `FUSE` after `FORK`, `FUSE` does not require an explicit `LIMIT` in each `FORK` branch.
However, as a best practice and to avoid issues when upgrading to newer versions, we recommend still adding an explicit `LIMIT` before `FUSE`.
:::

::::

:::{note}
Previously, `FUSE` collected all values for each passthrough column across fork branches. `FUSE` now picks the first non-null value, or null in the absence of a value.
Since all `FORK` branches read the same underlying document, any non-null value for a passthrough column is that document's actual field value. Note that natively multi-valued fields (for example, `tags: ["a", "b"]`) are preserved intact, while the runtime-generated cross-branch union is dropped.
Columns of type `aggregate_metric_double`, `histogram`, and `date_range` are not yet supported and must be dropped using [`DROP`](/reference/query-languages/esql/commands/drop.md) before `FUSE`.
:::

## Syntax

Use default parameters:

```esql
FUSE
```

Specify custom parameters:

```esql
FUSE <fuse_method> SCORE BY <score_column> GROUP BY <group_column> KEY BY <key_columns> WITH <options>
```

## Parameters

`fuse_method`
:   Defaults to `RRF`. Can be one of `RRF` (for [Reciprocal Rank Fusion](https://cormack.uwaterloo.ca/cormacksigir09-rrf.pdf)) or `LINEAR` (for linear combination of scores).
    Designates which method to use to assign new relevance scores.

`options`
:   Options for the `fuse_method`.

::::{tab-set}
:::{tab-item} RRF
When `fuse_method` is `RRF`, `options` supports the following parameters:

`rank_constant`
:   Defaults to `60`. Represents the `rank_constant` used in the RRF formula.

`weights`
:   Defaults to `{}`. Allows you to set different weights for RRF scores based on `group_column` values. Refer to the [Set custom weights](#set-custom-weights) example.
:::

:::{tab-item} LINEAR
When `fuse_method` is `LINEAR`, `options` supports the following parameters:

`normalizer`
:   Defaults to `none`. Can be one of `none` or `minmax`. Specifies which score normalization method to apply.

`weights`
:   Defaults to `{}`. Allows you to set different weights for scores based on `group_column` values. Refer to the [Set custom weights](#set-custom-weights) example.
:::
::::

`score_column`
:   Defaults to `_score`. Designates which column to use to retrieve the relevance scores of the input row
    and where to output the new relevance scores of the merged rows.

`group_column`
:   Defaults to `_fork`. Designates which column represents the result set.

`key_columns`
:   Defaults to `_id, _index`. Rows with matching `key_columns` values are merged. Each remaining column value
    of the merged row is the first non-null value across the fork branches, or null in the absence of a value.

## Examples

The following examples use `FORK` to run parallel queries and `FUSE` to merge the results.

### Use RRF

Run a lexical and a semantic query in parallel with `FORK`, then merge with `FUSE` (applies `RRF` by default):

```esql
FROM books METADATA _id, _index, _score  # Include document ID, index name, and relevance score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC | LIMIT 100)  # Fork 1: Lexical search on title field, sorted by relevance score
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC | LIMIT 100)  # Fork 2: Semantic search on semantic_title field, sorted by relevance score
| FUSE  # Merge results using RRF algorithm by default
| SORT _score DESC # Sort results by the new scores, since `FUSE` does not do any sorting.
```

### Use linear combination

`FUSE` can also use linear score combination:

```esql
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC | LIMIT 100)  # Fork 1: Lexical search on title
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC | LIMIT 100)  # Fork 2: Semantic search on semantic_title
| FUSE LINEAR  # Merge results using linear combination of scores (equal weights by default)
| SORT _score DESC # Sort results by the new scores, since `FUSE` does not do any sorting.
```

### Normalize scores

When combining results from semantic and lexical queries through linear combination, we recommend first normalizing the scores from each result set.

The following example uses `minmax` score normalization.
Scores are normalized to values between 0 and 1 before the rows are combined:

```esql
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC | LIMIT 100)  # Fork 1: Lexical search
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC | LIMIT 100)  # Fork 2: Semantic search
| FUSE LINEAR WITH { "normalizer": "minmax" }  # Linear combination with min-max normalization (scales scores to 0-1 range)
| SORT _score DESC # Sort results by the new scores, since `FUSE` does not do any sorting.
```

### Set custom weights

`FUSE` allows you to specify different weights to scores, based on the `_fork` column values, enabling you to control the relative importance of each query branch in the final results.

```esql
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC | LIMIT 100)  # Fork 1: Lexical search
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC | LIMIT 100)  # Fork 2: Semantic search
| FUSE LINEAR WITH { "weights": { "fork1": 0.7, "fork2": 0.3 }, "normalizer": "minmax" }  # Weighted linear combination: 70% lexical, 30% semantic, with min-max normalization
| SORT _score DESC # Sort results by the new scores, since `FUSE` does not do any sorting.
```

### Minimum score cutoff

While `FUSE` is generally used for merging result sets, it can also be used to apply minimum score cutoffs, to filter out low-quality or irrelevant results.
Using `FUSE` to first normalize the scores gives a consistent threshold for applying a minimum score cutoff.

The following example uses `FUSE` to normalize the scores and apply a minimum score cutoff.

```esql
FROM books METADATA _id, _index, _score
| WHERE title:"Shakespeare"
| SORT _score DESC
| LIMIT 100
| EVAL label = "semantic_results" # Column used by FUSE to indicate that we are using a single result set
| FUSE LINEAR GROUP BY label WITH { "normalizer": "minmax" } # Applies score normalization, modifying only the _score column
| WHERE _score > 0.1 # Applies a score cutoff, filtering out irrelevant results
| SORT _score DESC # `FUSE` can change the order of the results, so we need to sort by `_score`
```

In the following example, we use `FUSE` to normalize the scores in each `FORK` branch and remove irrelevant results, before they are merged together by another `FUSE` command using `RRF`:

```
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare"
        | SORT _score DESC
        | LIMIT 100
        | EVAL label = "lexical"
        | FUSE LINEAR GROUP BY label WITH { "normalizer": "minmax" } # Lexical scores are normalized, they will take values between 0 and 1.
        | WHERE _score > 0.1 # Irrelevant lexical results are filtered out.
        | SORT _score DESC # We reorder the results, since FUSE does not guarantee the order
        | LIMIT 100)
       (WHERE semantic_title:"Shakespeare"
       | SORT _score DESC
       | LIMIT 100
       | FUSE LINEAR GROUP BY label WITH { "normalizer": "minmax" } # Semantic search scores are normalized, they will take values between 0 and 1.
       | WHERE _score > 0.1 # Irrelevant semantic search results are filtered out
       | SORT _score DESC
       | LIMIT 100)
| FUSE RRF # Lexical and semantic search results are merged together using Reciprocal Rank Fusion.
| SORT _score DESC # FUSE does not re-order the results based on their score, so we need to sort again.
```

## Limitations

These limitations can be present either when:

-  `FUSE` is not combined with [`FORK`](/reference/query-languages/esql/commands/fork.md)
- `FUSE` doesn't use the default [metadata](/reference/query-languages/esql/esql-metadata-fields.md) columns `_id`, `_index`, `_score` and `_fork`

  1. `FUSE` assumes that `key_columns` are single valued. When `key_columns` are multivalued, `FUSE` can produce unreliable relevance scores.
  1. `FUSE` automatically assigns a score value of `NULL` if the `<score_column>` or `<group_column>` are multivalued.
  1. `FUSE` assumes that the combination of `key_columns` and `group_column` is unique. If not, `FUSE` can produce unreliable relevance scores.
