```yaml {applies_to}
serverless: preview
stack: preview 9.2.0
```

The `FUSE` processing command merges rows from multiple result sets and assigns
new relevance scores.

**Syntax**

```esql
FUSE
FUSE <fuse_method> SCORE BY <score_column> GROUP BY <group_column> KEY BY <key_columns> WITH <options>
```

**Parameters**

`fuse_method`
: Defaults to `RRF`. Can be one of `RRF` (for Reciprocal Rank Fusion) or `LINEAR` (for linear combination of scores).
Designates which method to use to assign new relevance scores.

`score_column`
: Defaults to `_score`. Designates which column to use to retriever the relevance scores of the input row
and where to output the new relevance scores of the merged rows.

`group_column`
: Defaults to `_fork`. Designates which column represents the result set.

`key_columns`
: Defaults to `_id, _index`. Rows with the same values for `key_columns` will be
merged together.

`options`
: Options for the <fuse_method>.

When `<fuse_method>` is `RRF`, `options` supports the following parameters:

`rank_constant`
: Defaults to `60`. Represents the `rank_constant` used in the RRF formula.

`weights`
: Defaults to `{}`. Allows setting different weights on the RRF scores, depending
on the `group_column` values. See examples.

When `<fuse_method>` is `LINEAR`, `options` supports the following parameters:

`normalizer`
: Defaults to `none`. Can be one of `none` or `minmax`. Specifies which score normalization method to apply.

`weights`
: Defaults to `{}`. Allows setting different weights for the scores, depending
on the `group_column` values. See examples.

**Description**

`FUSE` will merge rows that have the same values for `<key_columns>` and will assign new relevance scores using the algorithm specified
by `<fuse_method>` and the values from the `<group_column>` and `<score_column>`.

`FUSE` enables hybrid search as a way to combine and assign new relevance scores to results sets coming from multiple queries.

**Examples**

In the following example, we use FORK to execute two different queries: a lexical and a semantic query.
We then use `FUSE` to merge the results, in this case `FUSE` applies `RRF` by default:

```esql
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC)
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC)
| FUSE
```

`FUSE` can also do a linear combination of scores:

```esql
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC)
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC)
| FUSE LINEAR
```

When combining results from semantic and lexical queries through linear combination, it is recommended to
first normalize the scores from each result set. The following example uses `minmax` score normalization.
This means the scores will be normalized and assigned values between 0 and 1, before the rows are combined:

```
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC)
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC)
| FUSE LINEAR WITH { "normalizer": "minmax" }
```

`FUSE` allows to specify different weights to scores, depending on the values from `_fork` column.
In the following example

```esql
FROM books METADATA _id, _index, _score
| FORK (WHERE title:"Shakespeare" | SORT _score DESC)
       (WHERE semantic_title:"Shakespeare" | SORT _score DESC)
| FUSE LINEAR WITH { "weights": { "fork1": 0.7, "fork2": 0.3 }, "normalizer": "minmax" }
```

**Limitations**

These limitations can be present when `FUSE` is not used in combination with `FORK` or when `FUSE` is using columns different than the `_id`, `_index`, `_score` and `_fork` default columns:
- `FUSE` assumes that `key_columns` are single valued. When `key_columns` are multivalued, `FUSE` can produce unreliable relevance scores.
- `FUSE` automatically assigns a score value of `NULL` if the `<score_column>` or `<group_column>` are multivalued.
- `FUSE` assumes that the combination of `key_columns` and `group_column` is unique. If this is not the case, `FUSE` can produce unreliable relevance scores.

