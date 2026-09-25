# External-datasource test declarations

Shared declarations that external-datasource test suites read instead of hard-coding what to run and
what to expect. Two of them live here, and they answer different questions:

| file | question it answers |
|---|---|
| `dataset-registration-cases.properties` | given these settings, what must `PUT /_query/dataset` do, and what must it say? |
| `fixture-dimensions.properties` | which combinations of data shape and settings are worth generating at all? |

This document is about the first. The second is read by the read-path suites and is described at the
bottom.

## What the registration declaration is for

The round trip is: register a dataset with some settings, then query it. There are three things that
can happen, and a suite has to be able to tell them apart:

1. **The registration is refused.** The setting is wrong in a way the endpoint can see.
2. **The registration is accepted and the query fails.** Registration performs no I/O, so a setting
   that disagrees with the bytes cannot be detected until something opens the object.
3. **The registration is accepted and the query succeeds.** The setting took effect — or was
   silently ignored, which looks identical unless the test checks the shape of the result.

A suite that only checks the HTTP status cannot distinguish any of these from a refusal of something
it never varied. That is not a theoretical concern: the first run of the suite in this repo had every
case returning 400 while asserting nothing, because the resource was written as a filesystem path and
every registration failed on the URI scheme rather than on the setting under test. Only an assertion
on the message showed it.

So **the message is the assertion.** Every case names the substring the failure must contain, and the
symbol that emits it.

## A case

```properties
case.dataset.setting.skip_rows.negative.settings.skip_rows = -1
case.dataset.setting.skip_rows.negative.message = [skip_rows] must be between 0 and 1000, got [-1]
case.dataset.setting.skip_rows.negative.emitter = DataSourceValidationUtils.validateInt
```

That is a complete case. The suite registers a dataset carrying `skip_rows: -1`, expects the PUT to
fail, and asserts the response contains that message.

### Names are paths

`dataset.setting.skip_rows.negative` reads as a path so the corpus can be scanned by subject rather
than as one flat list. The convention in use:

```
dataset.setting.<key>.<what is wrong>        one setting, one bound or vocabulary
dataset.combination.<what is wrong>          two or more settings that are each valid alone
dataset.format.<what is wrong>               the declared format against the actual bytes
dataset.resource.<what is wrong>             the resource itself
```

Anything after `case.` and before the attribute is the name, so the hierarchy can grow without the
parser changing. Datasource cases will be `datasource.*` when they arrive.

## The three outcomes, with the cases that show them

### Refused, one setting

The common case: a bound or a vocabulary the endpoint checks on its own.

```properties
case.dataset.setting.partition_detection.unknown_value.settings.partition_detection = sideways
case.dataset.setting.partition_detection.unknown_value.message = [partition_detection] must be one of
case.dataset.setting.partition_detection.unknown_value.emitter = DataSourceValidationUtils.validateEnum
```

The message deliberately stops at `must be one of` rather than listing the values. The assertion is
that the refusal *names the vocabulary back*; pinning the exact list would go red every time a value
is added, which is churn rather than coverage.

### Refused, a combination

The refusals worth having most. Each setting is accepted on its own and the pair is refused:

```properties
case.dataset.combination.probe_budget_exceeded.settings.split_probe_window = 64mb
case.dataset.combination.probe_budget_exceeded.settings.max_split_probes = 1000
case.dataset.combination.probe_budget_exceeded.message = Invalid combination of [split_probe_window]
case.dataset.combination.probe_budget_exceeded.emitter = FileSplitProvider.validateProbeBudget
```

Neither value is out of range. Their product is. No per-setting bound check can reach this, which is
why the message names both settings — the user has to choose which one to lower.

The other combinations declared today follow the same shape: an error budget with no `error_mode`,
`partition_detection=template` with no `partition_path`, and file ordering under a schema resolution
where read order cannot change the answer.

**Where to find more of them:** a combination can only be refused where some validator reads more
than one setting. Grep for validators taking the whole settings map rather than generating
combinations and waiting for one to fail.

### Accepted, then failing at query

```properties
case.dataset.format.parquet_declared_over_text_bytes.settings.format = parquet
case.dataset.format.parquet_declared_over_text_bytes.resource = noext
case.dataset.format.parquet_declared_over_text_bytes.outcome = query_fails
case.dataset.format.parquet_declared_over_text_bytes.message = as a Parquet file: 
case.dataset.format.parquet_declared_over_text_bytes.emitter = ParquetFormatReader.ParquetReadFailures.wrap
```

The file holds CSV bytes under a name that implies no format, so nothing at registration can
contradict the setting.

**Both halves are assertions.** The PUT succeeding is not setup: if it ever starts refusing this,
registration has acquired I/O and the no-I/O guarantee is gone — and it would go unnoticed, because
the query would never run. The message after it is the contract for what the reader says when it
finally looks.

### Accepted, and taking effect

The outcome no status can detect. A setting that is accepted, plumbed to the reader and then ignored
returns rows exactly as one that worked does.

```properties
case.dataset.setting.delimiter.takes_effect.settings.delimiter = |
case.dataset.setting.delimiter.takes_effect.outcome = query_succeeds
case.dataset.setting.delimiter.takes_effect.columns.0 = a,b
```

`simple.csv` is `a,b` over two comma-separated rows. Read with the declared delimiter it is **one**
column named `a,b`; read with the default comma — which is what an ignored setting gives — it is two
columns. So the column list is the whole assertion, and it fails in the direction that matters.

Choosing the fixture is the work here. A case of this kind is only worth having if the bytes parse
*differently* under the declared value than under the default; on data that reads the same either way
it passes without testing anything, which is the failure it exists to catch.

Columns are declared one per key, `columns.0`, `columns.1`, and compared in index order. They are not
a comma-separated list because a column name can contain anything the bytes contain — this very case
expects one called `a,b`.

### Blocked on a filed defect

A case may assert behaviour the product does not have yet:

```properties
case.dataset.resource.path_not_uri_reports_settings_unknown.settings.schema_sample_size = 500
case.dataset.resource.path_not_uri_reports_settings_unknown.resource_form = path
case.dataset.resource.path_not_uri_reports_settings_unknown.message = [resource] must use one of the supported URI schemes
case.dataset.resource.path_not_uri_reports_settings_unknown.absent = unknown setting [schema_sample_size]
case.dataset.resource.path_not_uri_reports_settings_unknown.emitter = FileDataSourceValidator.validateResource
case.dataset.resource.path_not_uri_reports_settings_unknown.blocked_by = elastic/esql-planning#1999
```

Two things worth copying from this one.

`absent` is the assertion. The registration is refused either way, so a case checking the status
passes today, and so does one asserting only the schemes message. What has to change is that a second
misleading error stops arriving — and only a negative assertion can say that.

`blocked_by` makes the suite skip the case and names the issue. **The citation is required**: a
blocked case with no `elastic/<repo>#<n>` fails the parse. An exclusion whose reason lives in a mute
list somewhere else outlives the defect and quietly loses the coverage it was added for.

Verify a blocked case fails for the reason you think before committing it. Remove the `blocked_by`
line, run it, read the failure, put the line back. A blocked case nobody has watched fail is a line
that will sit green forever once someone deletes the block.

## Attribute reference

| attribute | required | default | meaning |
|---|---|---|---|
| `settings.<key>` | yes, one or more | — | a dataset setting to register; repeat the attribute for a combination |
| `message` | for a failing case | — | a substring the failure must contain |
| `emitter` | for a failing case | — | the symbol the message was read from |
| `columns.<n>` | for `query_succeeds` | — | expected column names, compared in index order |
| `absent` | no | — | a substring the failure must **not** contain |
| `outcome` | no | `refused` | `refused`, `query_fails`, or `query_succeeds` |
| `query` | no | `FROM %s \| LIMIT 1` | the query for a `query_fails` case; `%s` is the dataset |
| `format` | no | `csv` | the format the dataset is registered as |
| `resource` | no | `simple.<format>` | the fixture file, relative to the suite's fixture directory |
| `resource_form` | no | `uri` | `uri`, or `path` to register a bare filesystem path |
| `blocked_by` | no | — | `elastic/<repo>#<n>`; the suite skips the case until it is fixed |

### What the parser refuses, and why

Each of these is a way a case can look present and assert nothing:

- **An unknown attribute.** A typo silently drops the assertion the line was meant to make.
- **A key outside `case.<name>.<attribute>`.** Same reason.
- **No settings.** The case registers nothing.
- **No `message` or `emitter` on a failing case.** Such a case asserts only the status.
- **No `columns` on a `query_succeeds` case.** It would assert that rows came back, which is exactly
  what a silently ignored setting also produces.
- **`columns` on a case that expects no result.** They would never be compared.
- **A blank value** anywhere required. Blank reads as deliberate and is the same missing declaration.
- **An empty declaration.** A contract with no cases reports green having checked nothing.
- **An unknown `outcome`.** A typo would otherwise fall back to `refused` and never run the query.
- **A `query` on a `refused` case.** The query would never run, so the case was half-converted from
  the other kind and reads as covering a path it never touches.
- **A `blocked_by` with no issue reference.** Nothing would close the exclusion when the defect is
  fixed.

## Adding a case

1. **Read the message out of the component that emits it.** Not from what it ought to say — a
   contract asserting a message nobody implements goes red on correct code.
2. **Choose a substring that names the offending setting and the constraint it broke.** A reworded
   sentence should survive; a changed rule should not. `"invalid"` passes on almost any refusal.
3. **Name the case on the path convention above.**
4. **Record the emitter**, so the next reader can re-check the message against the code rather than
   searching for the string — and a message that has drifted is exactly the one the search will not
   find.
5. **Run it and watch it pass for the right reason.** If you are adding a `blocked_by` case, watch it
   *fail* for the right reason first.

## Running it

```
./gradlew :x-pack:plugin:esql:qa:server:single-node:javaRestTest \
  --tests "*DatasetRegistrationContractIT*"
```

One case:

```
./gradlew :x-pack:plugin:esql:qa:server:single-node:javaRestTest \
  --tests "*DatasetRegistrationContractIT*probe_budget*"
```

The suite is snapshot-only: the `local` data-source type it registers against is not present in a
release distribution, and the suite skips itself there rather than reporting green having run nothing.

The declaration's own parser has unit tests that need no cluster:

```
./gradlew :x-pack:plugin:esql:qa:fixture-common:test
```

## Wiring it into a module

A module that reads these declarations depends on this one:

```groovy
javaRestTestImplementation project(xpackModule('esql:qa:fixture-common'))
```

The declarations are classpath resources, so a module that forgets the dependency gets a load-time
failure naming what to add rather than an empty corpus.

This module is **dependency-free outside the JDK**, and must stay that way: the ORC and Parquet
fixture generators isolate their Hadoop classpaths, and anything dragged in here lands on them.

## The dimension declaration

`fixture-dimensions.properties` answers the other question — which combinations are worth generating
at all. Twenty-one dimensions, each declaring its values, its default, and how the value becomes real:

```properties
dimension.delimiter.values = comma, tab, semicolon, pipe
dimension.delimiter.default = comma
dimension.delimiter.binds = fixture
```

`binds` says what kind of axis it is, and is the closest thing to a grouping the file has today:
`fixture` changes the bytes on disk, `directive` is a dataset setting, `backend` selects where the
data lives, `cluster` needs a differently-configured node.

Combinations are not enumerated by hand. The declaration records, for every **pair** of dimensions,
whether varying them together reaches code that varying each alone does not, and the test set is
derived from those verdicts. An `independent` verdict removes cells and nothing ever reveals they are
missing, so it carries the mechanism that makes it safe; an untraced pair is recorded `unverified`
and treated as interacting, so uncertainty costs test executions rather than coverage.

**No suite in this repository reads it yet.** It is carried here so the registration suite and the
read-path suites that will consume it depend on one declaration rather than each growing their own.
Its own tests run as part of this module.
