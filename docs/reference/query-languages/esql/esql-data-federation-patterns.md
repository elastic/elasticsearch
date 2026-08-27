---
navigation_title: "Resource patterns"
description: "Reference for the glob pattern language used by ES|QL Data Federation dataset resources and file exclusions: wildcards, character classes, alternation, and numeric ranges."
applies_to:
  stack: experimental 9.6+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Resource patterns for {{esql}} Data Federation

A dataset's `resource` selects the objects it reads with a glob pattern, and entries in its
[`file_exclusions` setting](esql-data-federation-datasets.md#excluding-non-data-objects) are patterns in the
same language. This page is the reference for that language: what each construct matches, how a resource is
split into a listing prefix and a pattern, and which patterns are rejected as invalid.

:::{include} _snippets/data-federation/experimental-warning.md
:::

The language is deliberately compatible with the glob syntax of the ClickHouse `s3` table function, so a
pattern written for it means the same thing here. The differences are listed in
[ClickHouse compatibility](#clickhouse-compatibility).

## How a resource is matched

A `resource` is a storage URI whose path may contain pattern metacharacters: `*`, `?`, `[`, and `{`. It is
split into two parts:

- The **listing prefix**: everything up to and including the last `/` before the first metacharacter. This is
  the location that gets listed.
- The **pattern**: the remainder. Every listed object's path, taken relative to the listing prefix, is matched
  against it. The pattern must match the whole relative path, not a part of it.

| Resource | Listing prefix | The pattern, matched against paths under the prefix |
|---|---|---|
| `s3://logs/access/2024/*.parquet` | `s3://logs/access/2024/` | `*.parquet` |
| `s3://logs/access/year=*/month=*/*.parquet` | `s3://logs/access/` | `year=*/month=*/*.parquet` |
| `s3://logs/data-*/part-1.parquet` | `s3://logs/` | `data-*/part-1.parquet` |
| `s3://logs/access/**/*.csv` | `s3://logs/access/` | `**/*.csv` |

For example, with the resource `s3://logs/access/year=*/month=*/*.parquet`, the object
`s3://logs/access/year=2024/month=06/part-0.parquet` is matched as the relative path
`year=2024/month=06/part-0.parquet`.

Some rules that follow from this model:

- **Matching is case-sensitive.** `*.csv` does not match `data.CSV`.
- **Metacharacters are only special in the path.** A `*` or `{` in the scheme or bucket name is literal text,
  not a pattern.
- **A resource with no metacharacters names exactly one object**, which is read directly without any listing.
- **A comma separates resources.** `s3://b/a.csv, s3://b/b.csv` is a list of two resources, each resolved
  independently. A comma inside a brace group belongs to the pattern, so `s3://b/x.{csv,tsv}` is a single
  resource. Whitespace around each listed resource is trimmed, and empty entries are ignored.

:::{tip}
A pattern built only from literal text and brace groups, such as `s3://b/data/{a,b}.csv` or
`s3://b/file-{01..12}.parquet`, names a finite set of objects. Such patterns are resolved by checking each
named object directly instead of listing the prefix, which is much cheaper on prefixes holding many objects.
The [`esql.external.max_glob_expansion` cluster setting](esql-data-federation-cluster-settings.md#glob-and-file-discovery-limits)
caps how many objects are checked this way before the pattern falls back to listing.
:::

## Pattern constructs

| Construct | Matches |
|---|---|
| `*` | Any run of characters, including none, within one path segment. Never crosses `/`. |
| `?` | Exactly one character. Never matches `/`. |
| `**` | Zero or more whole path segments. Only special when it is a complete segment. |
| `[abc]`, `[a-z]` | One character from a set or range. |
| `[!abc]`, `[^abc]` | One character not in the set. Never matches `/`. |
| `{a,b}` | Either alternative. Each alternative can itself contain wildcards. |
| `{N..M}` | One integer from a numeric range, optionally zero-padded. |
| Anything else | Itself. There is no escape character; `\` is an ordinary character. |

### `*` and `?`

`*` matches any run of characters within a single path segment, including the empty run. `?` matches exactly
one character. Neither ever matches the `/` separator, so both are contained within one directory or file
name.

| Pattern | Matches | Does not match |
|---|---|---|
| `*.parquet` | `file.parquet` | `dir/file.parquet` (crosses a `/`), `file.csv` |
| `data-*-out.csv` | `data-2024-out.csv`, `data--out.csv` | `data-a/b-out.csv` |
| `file?.parquet` | `file1.parquet`, `fileA.parquet` | `file.parquet` (`?` requires a character), `file12.parquet` |

### `**`

`**` matches zero or more whole path segments, and is the way to search a directory tree recursively. It is
only special when it stands alone as a complete segment: written next to other characters, a run of stars is
the same as a single `*`, so `a**` means `a*` and stays within one segment.

| Pattern | Matches | Does not match |
|---|---|---|
| `**/*.parquet` | `x.parquet`, `a/b/x.parquet` | `x.csv` |
| `logs/**/events.csv` | `logs/events.csv` (zero segments), `logs/2024/06/events.csv` | `logs/old_events.csv` |
| `**` | every object under the prefix | |
| `a**` | `abc` (same as `a*`) | `a/b` |

Note what `logs/**/events.csv` does not match: `**` matches whole segments only, so the pattern requires a
file named exactly `events.csv`. It cannot stop partway through a name and treat `old_events.csv` as a
match.

Because `**` can match zero segments, a trailing `/**` also matches its anchor itself: `logs/**` matches an
object named `logs` as well as everything under `logs/`.

### Character classes

`[...]` matches exactly one character from a set. The set can list characters, ranges, or both:
`[abc]`, `[0-9]`, `[a-cx-z0-9]`. A class beginning with `!` or `^` is negated and matches one character not
in the set.

| Pattern | Matches | Does not match |
|---|---|---|
| `part-[0-9].csv` | `part-7.csv` | `part-x.csv`, `part-10.csv` (a class is one character) |
| `file[abc].txt` | `filea.txt` | `filed.txt` |
| `file[!0-9].txt` | `filea.txt` | `file1.txt` |

Like `*` and `?`, a class never matches the `/` separator, negated or not, and a class containing `/` is
rejected as invalid.

Within a class, a few positions make a metacharacter literal:

- `]` as the first member is a literal `]`: the class `[]]` matches the character `]`.
- `-` first, last, or anywhere it does not sit between two characters is a literal `-`: `[-x]` and `[x-]`
  both match `-` or `x`.

### Matching a literal metacharacter

There is no escape character in this language. `\` matches a literal backslash and does not change the
meaning of the character after it: the pattern `a\*b` matches `a\` followed by anything and then `b`, because
the `*` is still a wildcard.

To match a `*`, `?`, `[`, or `{` that appears literally in an object name, put it in a one-character class:

| Pattern | Matches | Does not match |
|---|---|---|
| `a[*]b` | `a*b` | `axb` |
| `report[?].pdf` | `report?.pdf` | `reportx.pdf` |
| `x[[]y` | `x[y` | |
| `v[{]1}` | `v{1}` | |

`]` and `}` need no such treatment: outside a class or brace group they are ordinary characters.

### Alternation: `{a,b}`

A brace group with commas matches any one of its alternatives. Each alternative is itself a small pattern, so
it can contain `*`, `?`, and classes. An empty alternative matches the empty string. Brace groups cannot be
nested.

| Pattern | Matches | Does not match |
|---|---|---|
| `*.{parquet,csv}` | `data.parquet`, `data.csv` | `data.json` |
| `{a*,b}.csv` | `a.csv`, `axyz.csv`, `b.csv` | `.csv` |
| `report{,-final}.pdf` | `report.pdf`, `report-final.pdf` | `report-draft.pdf` |

### Numeric ranges: `{N..M}`

A brace group of the form `{N..M}`, where both endpoints are non-negative integers and the group contains no
comma, matches each integer in the range, endpoints included. Descending ranges such as `{3..1}` work too.

If either endpoint is written with a leading zero and more than one digit, every value is left-padded with
zeros to the width of the wider endpoint. Otherwise there is no padding: a bare `0` does not turn it on, so
`{0..10}` matches `0` through `10` unpadded, while `{00..10}` matches `00` through `10`.

| Pattern | Matches | Does not match |
|---|---|---|
| `file-{1..12}.csv` | `file-1.csv`, `file-7.csv`, `file-12.csv` | `file-07.csv` (no padding without a leading zero) |
| `file-{01..12}.csv` | `file-01.csv`, `file-07.csv`, `file-12.csv` | `file-7.csv` |
| `shard-{3..1}.csv` | `shard-1.csv`, `shard-2.csv`, `shard-3.csv` | |

A brace body with `..` that is not two bare integers is not a range. It falls back to plain alternation, and
`..` within an alternative is literal text: `{a..c}` matches only the literal `a..c`, not `b`, and
`{-1..3}` matches only `-1..3`. Likewise, when a comma is present the body is alternation: `{1..3,5}`
matches `1..3` or `5`, not `2`.

A numeric range can produce at most 1024 values. A wider one, such as `{1..100000}`, is rejected as invalid
rather than silently truncated. The cap is on ranges because a range turns a dozen characters into any number
of values; a comma list is limited by how much of it you type, and is not capped.

## Invalid patterns

Malformed patterns are rejected with an error naming the problem, rather than being silently reinterpreted. A
malformed `resource` fails the query that reads the dataset; a malformed `file_exclusions` entry is rejected
when you register the dataset. The error always starts with `Invalid glob pattern [<pattern>]:` followed by
one of:

| Pattern shape | Example | Error |
|---|---|---|
| Unclosed character class | `file[abc` | `unterminated character class, missing ']' — note that a character class cannot contain or span a path separator` |
| Character class containing `/` | `a[/]b` | same as above: an unclosed class and a class holding a `/` are the same error, because a `/` ends the class it appears in |
| POSIX class syntax | `[[:digit:]]` | `POSIX character classes such as [[:digit:]] are not supported` |
| Reversed range in a class | `[z-a]` | `reversed range [z-a]` |
| Unclosed brace group | `file{a,b` | `unterminated brace group, missing '}'` |
| Nested brace groups | `{a,{b,c}}` | `nested brace groups are not supported` |
| Brace group that cannot be expanded | `{1..100000}`, `{99999999999999999999..2}` | `brace group [...] cannot be expanded; a numeric range needs parseable endpoints and at most 1024 alternatives` |

By contrast, a stray `]` or `}` with no matching opener is an ordinary character, not an error: `a]b` and
`a}b` match themselves.

## ClickHouse compatibility

The language is compatible with the glob syntax of the ClickHouse `s3` table function: `*`, `?`, `**`,
`{a,b}` alternation, and `{N..M}` numeric ranges mean the same thing, and `\` is a literal there too. A
pattern written for ClickHouse selects the same objects here, with two deliberate exceptions:

- **Character classes are supported here.** ClickHouse treats `[` and `]` as literal characters. A pattern
  relying on that, such as one matching a file literally named `part[0].csv` with bare brackets, must use a
  one-character class here: `part[[]0].csv`.
- **Malformed patterns are rejected here.** ClickHouse treats shapes such as an unclosed `[` or `{` as
  literal text; here they are [errors](#invalid-patterns).

Four smaller differences:

- Here a `*` inside a brace alternative is a wildcard of that alternative, so `{a*,b}.csv` matches `a.csv`,
  `axyz.csv` and `b.csv`. In ClickHouse the braces and comma become literal characters while the `*` stays a
  live wildcard, so the same pattern matches names such as `{ax,b}.csv`.
- Here a run of stars glued to other text, such as `a**`, stays within one segment like `a*`. In ClickHouse it
  can cross directory levels. This follows ClickHouse's stated rule, that `**` is special only as a complete
  path component, rather than its behaviour.
- Zero-padding of a numeric range differs at the edges. Here a range pads when either endpoint is written
  padded, so `{1..05}` pads; ClickHouse pads from one endpoint only.
- A numeric range here expands to at most 1024 values. ClickHouse has no such cap.

## Brace groups and partition placeholders

The `partition_path` dataset setting, which declares partition columns for paths that do not follow the
Hive `name=value` convention, uses single braces as column placeholders: in `partition_path`, `{year}`
means "this path segment is the `year` column". A `resource` gives the same spelling a different meaning:
`{year}` is a brace group with one alternative, which matches only a directory literally named `year`.

```console
PUT /_query/dataset/access_logs
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/*/*.parquet",
  "settings": {
    "partition_path": "{year}"
  }
}
```

Here the placeholder belongs in `partition_path`, and the corresponding `resource` segment is a plain `*`.
Writing `"resource": "s3://logs-bucket/access/{year}/*.parquet"` instead would read only a directory named
`year`, which almost certainly does not exist, and the query would report that the pattern matched no files.

## Patterns in `file_exclusions`

An entry in the [`file_exclusions` dataset setting](esql-data-federation-datasets.md#excluding-non-data-objects)
is an ordinary pattern in this language. There is no second dialect: an exclusion entry is matched against
the same prefix-relative path as the `resource` pattern that discovered the object, with the same whole-path
rule. An object whose relative path matches any entry is dropped from the listing.

The default is:

```
["**/_*", "**/.*", "**/_temporary/**", "**/_delta_log/**"]
```

The four entries are two different shapes, on purpose:

- **The file-name rules** `**/_*` and `**/.*` implement the Spark and Hive convention that a name beginning
  with `_` or `.` is not data, covering markers and sidecars such as `_SUCCESS`, `_metadata`, and
  `.part-0.crc` at any depth. Because `*` cannot cross a `/`, these entries match only the final segment of
  the path: they exclude files by name and cannot touch a directory. That is what makes them safe for
  partitioned data, where values live in directory names: `_dept=alpha/part-0.parquet` and
  `_foo/part-0.parquet` are read, whatever the partition detection mode.
- **The named-directory rules** `**/_temporary/**` and `**/_delta_log/**` cover the two well-known
  directories whose contents look like data but are not: a failed Spark job's leftover part-files, and a
  Delta Lake transaction log. A wildcard directory rule such as `**/_*/**` would also swallow partition
  directories starting with `_`, so directories are only ever excluded by their exact name. A directory
  named literally `_temporary` or `_delta_log` that holds real data would be excluded by the default; replace
  the list to keep it.

Exclusions apply to wildcard discovery only. An object the `resource` names explicitly, whether as a
pattern-free resource, a pattern-free member of a comma-separated list, or one of the objects a finite brace
pattern names, is always read: naming an object is a request to read it.

Because exclusion entries are patterns, a malformed entry is rejected when the dataset is registered, with
the message `[file_exclusions] must contain only valid patterns` followed by the
[specific pattern error](#invalid-patterns).
