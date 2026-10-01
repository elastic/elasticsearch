---
navigation_title: "Resource patterns"
description: "Reference for ES|QL Data Federation resource pattern syntax, including wildcards, character classes, alternation, numeric ranges, and file exclusions."
applies_to:
  stack: experimental 9.6+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Resource pattern syntax for {{esql}} Data Federation

A dataset's `resource` uses a glob pattern to select objects in external storage. This syntax reference explains how resource patterns are split and matched, which pattern constructs are available, and which patterns are rejected. The [`file_exclusions` setting](#exclude-non-data-objects) uses the same syntax.

:::{include} _snippets/data-federation/experimental-warning.md
:::

## How resource patterns are resolved

A `resource` is a storage URI whose path can contain pattern metacharacters: `*`, `?`, `[`, and `{`. It is
resolved by separating the storage location to list from the pattern to match.

### Separate the listing prefix from the pattern

A resource is split into two parts:

- The **listing prefix**: everything up to and including the last `/` before the first metacharacter. This is
  the location that gets listed.
- The **pattern**: the remainder. Every listed object's path, taken relative to the listing prefix, is matched
  against it. The pattern must match the whole relative path, not a part of it.

The following examples show how resources are split:

| Resource | Listing prefix | The pattern, matched against paths under the prefix |
|---|---|---|
| `s3://logs/access/2024/*.parquet` | `s3://logs/access/2024/` | `*.parquet` |
| `s3://logs/access/year=*/month=*/*.parquet` | `s3://logs/access/` | `year=*/month=*/*.parquet` |
| `s3://logs/data-*/part-1.parquet` | `s3://logs/` | `data-*/part-1.parquet` |
| `s3://logs/access/**/*.csv` | `s3://logs/access/` | `**/*.csv` |

For example, with the resource `s3://logs/access/year=*/month=*/*.parquet`, the object
`s3://logs/access/year=2024/month=06/part-0.parquet` is matched as the relative path
`year=2024/month=06/part-0.parquet`.

### Apply resource-level matching rules

Resource patterns follow these matching rules:

- **Matching is case-sensitive.** `*.csv` does not match `data.CSV`.
- **Metacharacters are only special in the path.** A `*` or `{` in the scheme or bucket name is literal text,
  not a pattern.
- **A resource with no metacharacters names exactly one object**, which is read directly without any listing.
- **A comma separates resources.** `s3://b/a.csv, s3://b/b.csv` is a list of two resources, each resolved
  independently. A comma inside a brace group belongs to the pattern, so `s3://b/x.{csv,tsv}` is a single
  resource. Whitespace around each listed resource is trimmed, and empty entries are ignored.

### Resolve finite patterns without listing

Use a pattern containing only literal text and finite brace groups when you know which objects to check. For
example:

- `s3://b/data/{a,b}.csv` checks `data/a.csv` and `data/b.csv`.
- `s3://b/file-{01..12}.parquet` checks 12 numbered files.

These objects are checked directly instead of listing the prefix, which is faster for prefixes containing
many objects.

The [`esql.external.max_glob_expansion` cluster setting](esql-data-federation-cluster-settings.md#glob-and-file-discovery-limits)
limits the number of direct checks. If a pattern exceeds the limit, the prefix is listed instead.

## Pattern constructs

The following table summarizes the supported pattern constructs:

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

### Match characters within a path segment

`*` matches any run of characters within a single path segment, including the empty run. `?` matches exactly
one character. Neither ever matches the `/` separator, so both are contained within one directory or file
name.

| Pattern | Matches | Does not match |
|---|---|---|
| `*.parquet` | `file.parquet` | `dir/file.parquet` (crosses a `/`), `file.csv` |
| `data-*-out.csv` | `data-2024-out.csv`, `data--out.csv` | `data-a/b-out.csv` |
| `file?.parquet` | `file1.parquet`, `fileA.parquet` | `file.parquet` (`?` requires a character), `file12.parquet` |

### Match path segments recursively

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

### Match one character from a set

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

### Match literal metacharacters

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

### Match one of several alternatives

A brace group with commas matches any one of its alternatives. Each alternative is itself a small pattern, so
it can contain `*`, `?`, and classes. An empty alternative matches the empty string. Brace groups cannot be
nested.

| Pattern | Matches | Does not match |
|---|---|---|
| `*.{parquet,csv}` | `data.parquet`, `data.csv` | `data.json` |
| `{a*,b}.csv` | `a.csv`, `axyz.csv`, `b.csv` | `.csv` |
| `report{,-final}.pdf` | `report.pdf`, `report-final.pdf` | `report-draft.pdf` |

`*.{parquet,csv}` is a matcher: it selects objects whose names end in either extension. A dataset still needs a single format. Use this pattern only with an explicit [`format`](esql-data-federation-dataset-settings.md#format) setting, or split the files into two datasets.

### Match a numeric range

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
of values. A comma list is limited by how much of it you type and is not capped.

## Invalid patterns

Malformed patterns are rejected with an error naming the problem, rather than being silently reinterpreted. A
malformed `resource` fails the query that reads the dataset. A malformed `file_exclusions` entry is rejected
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

## Storage-specific resource restrictions

In addition to the pattern syntax, a storage provider can restrict which resource identifiers a dataset can use.

$$$s3-resource-requirements$$$
### Amazon S3 resource restrictions
```{applies_to}
stack: experimental 9.6+
```

{{es}} rejects an Amazon S3 resource when the AWS SDK routes its bucket identifier somewhere other than the regional object endpoint. Setting `endpoint` on the data source does not override this behavior for S3 on Outposts aliases.

The following S3 resource identifiers are not supported:

- An S3 Express directory bucket whose name ends in `--x-s3` or `--xa-s3`.
- An S3 on Outposts access point alias that the AWS SDK recognizes from a sufficiently long bucket name ending in `--op-s3`. Shorter names ending in `--op-s3` remain ordinary bucket names and are supported.
- A multi-region access point specified as an alias ending in `.mrap` or as its full hostname.
- An Amazon Resource Name (ARN). Use the bucket name, or an access point alias if the bucket is behind an access point.

No node setting permits these identifiers. The `esql.external.allowed_endpoint_hosts` setting governs the data source endpoint, not the bucket. Use a bucket that is reachable through the regional endpoint instead.

## Patterns in dataset settings

[Dataset settings](esql-data-federation-dataset-settings.md) use the resource pattern language for partition
placeholders and file exclusions.

### Define partition paths

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

### Exclude non-data objects

```{applies_to}
stack: experimental 9.6+
```

#### Exclusion behavior

Object-store prefixes can contain markers, sidecars, temporary files, and transaction logs alongside data.
The `file_exclusions` setting drops these objects after the `resource` pattern selects them. Each exclusion
uses the same pattern language as `resource` and matches the object's path relative to the listing prefix.

Exclusions apply only to objects found by wildcard discovery. An object named explicitly in `resource` is
always read, including a pattern-free member of a comma-separated resource or an object named by a finite
brace pattern such as `data/{a,b}.csv`.

Objects whose keys end in `/` are directory placeholders rather than files. These objects are skipped
before patterns are applied, so they stay skipped even when `file_exclusions` is set to `[]`.

#### Default exclusions

By default, datasets use the following exclusions:

| Pattern | Objects excluded |
|---|---|
| `**/_*` | Files whose names begin with `_`, such as `_SUCCESS` and `_metadata`, at any depth. |
| `**/.*` | Files whose names begin with `.`, such as `.part-0.crc`, at any depth. |
| `**/_temporary/**` | Contents of directories named `_temporary`. |
| `**/_delta_log/**` | Contents of directories named `_delta_log`. |

The file-name patterns match only the final path segment because `*` does not cross `/`. They do not exclude
partition directories such as `_dept=alpha/` or `_foo/`. Avoid a broad directory pattern such as `**/_*/**`,
which would exclude those partitions.

#### Custom exclusions

Setting `file_exclusions` replaces the default list. To preserve the defaults while excluding a retired
`backup_2024` directory, include all default patterns and add the directory:

```console
PUT /_query/dataset/access_logs
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "settings": {
    "file_exclusions": ["**/_*", "**/.*", "**/_temporary/**", "**/_delta_log/**", "backup_2024/**"]
  }
}
```

Here, `backup_2024/**` is relative to the listing prefix `s3://logs-bucket/access/`. Use
`**/backup_2024/**` instead to exclude a directory with that name at any depth. To turn off exclusions, set
`"file_exclusions": []`.

#### Exclusion diagnostics

When an exclusion drops an object, the node log records the count, an example object, and its matching
pattern at `DEBUG` level. If every object matched by a wildcard is excluded, the query's "matched no
files" error identifies exclusions as the reason. For a comma-separated resource whose other members match,
the response reports the same condition as a warning.

A malformed exclusion is rejected when the dataset is registered. The error begins with
`[file_exclusions] must contain only valid patterns` and includes the [specific pattern error](#invalid-patterns).

## ClickHouse compatibility

The language is compatible with the glob syntax of the ClickHouse `s3` table function: `*`, `?`, `**`,
`{a,b}` alternation, and `{N..M}` numeric ranges mean the same thing, and `\` is a literal there too. A
pattern written for ClickHouse selects the same objects here, with two deliberate exceptions:

- **Character classes are supported here.** ClickHouse treats `[` and `]` as literal characters. A pattern
  relying on that, such as one matching a file literally named `part[0].csv` with bare brackets, must use a
  one-character class here: `part[[]0].csv`.
- **Malformed patterns are rejected here.** ClickHouse treats shapes such as an unclosed `[` or `{` as
  literal text. Here they are [errors](#invalid-patterns).

Four smaller differences:

- Here a `*` inside a brace alternative is a wildcard of that alternative, so `{a*,b}.csv` matches `a.csv`,
  `axyz.csv` and `b.csv`. In ClickHouse the braces and comma become literal characters while the `*` stays a
  live wildcard, so the same pattern matches names such as `{ax,b}.csv`.
- Here a run of stars glued to other text, such as `a**`, stays within one segment like `a*`. In ClickHouse it
  can cross directory levels. This follows ClickHouse's stated rule, that `**` is special only as a complete
  path component, rather than its behavior.
- Zero-padding of a numeric range differs at the edges. Here a range pads when either endpoint is written
  padded, so `{1..05}` pads. ClickHouse pads from one endpoint only.
- A numeric range here expands to at most 1024 values. ClickHouse has no such cap.
