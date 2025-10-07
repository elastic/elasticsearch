```yaml {applies_to}
serverless: preview
stack: preview 9.2.0
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
:   Defaults to `{}`. Allows you to different weights for scores based on `group_column` values. Refer to the [Set custom weights](#set-custom-weights) example.
:::
::::

`score_column`
:   Defaults to `_score`. Designates which column to use to retrieve the relevance scores of the input row
    and where to output the new relevance scores of the merged rows.

`group_column`
:   Defaults to `_fork`. Designates which column represents the result set.

`key_columns`
:   Defaults to `_id, _index`. Rows with matching `key_columns` values are merged.

## Examples

### Use RRF

In the following example, we use the [`FORK`](/reference/query-languages/esql/commands/fork.md) command to run two different queries: a lexical and a semantic query.
We then use `FUSE` to merge the results (applies `RRF` by default):

```esql
FROM books METADATA _id, _index, _score  # Include document ID, index name, and relevance score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC)  # Fork 1: Lexical search on title field, sorted by relevance score
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC)  # Fork 2: Semantic search on semantic_title field, sorted by relevance score
| FUSE  # Merge results using RRF algorithm by default
```

### Use linear combination

`FUSE` can also use linear score combination:

```esql
FROM books METADATA _id, _index, _score 
| FORK (WHERE title:"Shakespeare" | SORT _score DESC)  # Fork 1: Lexical search on title
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC)  # Fork 2: Semantic search on semantic_title
| FUSE LINEAR  # Merge results using linear combination of scores (equal weights by default)
```

### Normalize scores

When combining results from semantic and lexical queries through linear combination, we recommend first normalizing the scores from each result set.

The following example uses `minmax` score normalization.
This means the scores normalize and assign values between 0 and 1, before combining the rows:

```esql
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC)  # Fork 1: Lexical search
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC)  # Fork 2: Semantic search
| FUSE LINEAR WITH { "normalizer": "minmax" }  # Linear combination with min-max normalization (scales scores to 0-1 range)
```

### Set custom weights

`FUSE` allows you to specify different weights to scores, based on the `_fork` column values, enabling you to control the relative importance of each query branch in the final results.

```esql
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC)  # Fork 1: Lexical search
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC)  # Fork 2: Semantic search
| FUSE LINEAR WITH { "weights": { "fork1": 0.7, "fork2": 0.3 }, "normalizer": "minmax" }  # Weighted linear combination: 70% lexical, 30% semantic, with min-max normalization
```

## Limitations

These limitations can be present either when:

-  `FUSE` is not combined with [`FORK`](/reference/query-languages/esql/commands/fork.md)
- `FUSE` doesn't use the default  [metadata](/reference/query-languages/esql/esql-metadata-fields.md) columns `_id`, `_index`, `_score` and `_fork`

  1. `FUSE` assumes that `key_columns` are single valued. When `key_columns` are multivalued, `FUSE` can produce unreliable relevance scores.
  1. `FUSE` automatically assigns a score value of `NULL` if the `<score_column>` or `<group_column>` are multivalued.
  1. `FUSE` assumes that the combination of `key_columns` and `group_column` is unique. If not, `FUSE` can produce unreliable relevance scores.

