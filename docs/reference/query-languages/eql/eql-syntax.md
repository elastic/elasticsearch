---
navigation_title: "Syntax reference"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-syntax.html
---

# EQL syntax reference [eql-syntax]



## Basic syntax [eql-basic-syntax]

EQL queries require an event category and a matching condition. The `where` keyword connects them.

```eql
event_category where condition
```

An event category is an indexed value of the [event category field](/reference/query-languages/eql.md#eql-required-fields). By default, the [EQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-search) uses the `event.category` field from the [Elastic Common Schema (ECS)][Elastic Common Schema (ECS)](ecs://reference/index.md)). You can specify another event category field using the API’s [`event_category_field`](/reference/query-languages/eql.md#specify-a-timestamp-or-event-category-field) parameter.

For example, the following EQL query matches events with an event category of `process` and a `process.name` of `svchost.exe`:

```eql
process where process.name == "svchost.exe"
```


### Match any event category [eql-syntax-match-any-event-category]

To match events of any category, use the `any` keyword. You can also use the `any` keyword to search for documents without a event category field.

For example, the following EQL query matches any documents with a `network.protocol` field value of `http`:

```eql
any where network.protocol == "http"
```


### Escape an event category [eql-syntax-escape-an-event-category]

Use enclosing double quotes (`"`) or three enclosing double quotes (`"""`) to escape event categories that:

* Contain a special character, such as a hyphen (`-`) or dot (`.`)
* Contain a space
* Start with a numeral

```eql
".my.event.category"
"my-event-category"
"my event category"
"6eventcategory"

""".my.event.category"""
"""my-event-category"""
"""my event category"""
"""6eventcategory"""
```


### Escape a field name [eql-syntax-escape-a-field-name]

Use enclosing backticks (`) to escape field names that:

* Contain a hyphen (`-`)
* Contain a space
* Start with a numeral

```eql
`my-field`
`my field`
`6myfield`
```

Use double backticks (``) to escape any backticks (`) in the field name.

```eql
my`field -> `my``field`
```


## Conditions [eql-syntax-conditions]

A condition consists of one or more criteria an event must match. You can specify and combine these criteria using the following operators. Most EQL operators are case-sensitive by default.


### Comparison operators [eql-syntax-comparison-operators]

```eql
<   <=   ==   :   !=   >=   >
```

`<` (less than)
:   Returns `true` if the value to the left of the operator is less than the value to the right. Otherwise returns `false`.

`<=` (less than or equal)
:   Returns `true` if the value to the left of the operator is less than or equal to the value to the right. Otherwise returns `false`.

`==` (equal, case-sensitive)
:   Returns `true` if the values to the left and right of the operator are equal. Otherwise returns `false`. Wildcards are not supported.

`:` (equal, case-insensitive)
:   Returns `true` if strings to the left and right of the operator are equal. Otherwise returns `false`. Can only be used to compare strings. Supports [wildcards](#eql-syntax-wildcards) and [list lookups](#eql-syntax-lookup-operators).

`!=` (not equal, case-sensitive)
:   Returns `true` if the values to the left and right of the operator are not equal. Otherwise returns `false`. Wildcards are not supported.

`>=` (greater than or equal)
:   Returns `true` if the value to the left of the operator is greater than or equal to the value to the right. Otherwise returns `false`. When comparing strings, the operator uses a case-sensitive lexicographic order.

`>` (greater than)
:   Returns `true` if the value to the left of the operator is greater than the value to the right. Otherwise returns `false`. When comparing strings, the operator uses a case-sensitive lexicographic order.

::::{note}
`=` is not supported as an equal operator. Use `==` or `:` instead.
::::



### Pattern comparison keywords [eql-syntax-pattern-comparison-keywords]

```eql
my_field like  "VALUE*"         // case-sensitive wildcard matching
my_field like~ "value*"         // case-insensitive wildcard matching

my_field regex  "VALUE[^Z].?"   // case-sensitive regex matching
my_field regex~ "value[^z].?"   // case-insensitive regex matching
```

`like` (case-sensitive)
:   Returns `true` if the string to the left of the keyword matches a [wildcard pattern](#eql-syntax-wildcards) to the right. Supports [list lookups](#eql-syntax-lookup-operators). Can only be used to compare strings. For case-insensitive matching, use `like~`.

`regex` (case-sensitive)
:   Returns `true` if the string to the left of the keyword matches a regular expression to the right. For supported regular expression syntax, see [*Regular expression syntax*](/reference/query-languages/query-dsl/regexp-syntax.md). Supports [list lookups](#eql-syntax-lookup-operators). Can only be used to compare strings. For case-insensitive matching, use `regex~`.


#### Limitations for comparisons [limitations-for-comparisons]

You cannot chain comparisons. Instead, use a [logical operator](#eql-syntax-logical-operators) between comparisons. For example, `foo < bar <= baz` is not supported. However, you can rewrite the expression as `foo < bar and bar <= baz`, which is supported.

You also cannot compare a field to another field, even if the fields are changed using a [function](#eql-functions).

**Example**<br> The following EQL query compares the `process.parent_name` field value to a static value, `foo`. This comparison is supported.

However, the query also compares the `process.parent.name` field value to the `process.name` field. This comparison is not supported and will return an error for the entire query.

```eql
process where process.parent.name == "foo" and process.parent.name == process.name
```

Instead, you can rewrite the query to compare both the `process.parent.name` and `process.name` fields to static values.

```eql
process where process.parent.name == "foo" and process.name == "foo"
```


### Logical operators [eql-syntax-logical-operators]

```eql
and  or  not
```

`and`
:   Returns `true` only if the condition to the left and right *both* return `true`. Otherwise returns `false`.

`or`
:   Returns `true` if one of the conditions to the left or right `true`. Otherwise returns `false`.

`not`
:   Returns `true` if the condition to the right is `false`.


### Lookup operators [eql-syntax-lookup-operators]

```eql
my_field in ("Value-1", "VALUE2", "VAL3")                 // case-sensitive
my_field in~ ("value-1", "value2", "val3")                // case-insensitive

my_field not in ("Value-1", "VALUE2", "VAL3")             // case-sensitive
my_field not in~ ("value-1", "value2", "val3")            // case-insensitive

my_field : ("value-1", "value2", "val3")                  // case-insensitive

my_field like  ("Value-*", "VALUE2", "VAL?")              // case-sensitive
my_field like~ ("value-*", "value2", "val?")              // case-insensitive

my_field regex  ("[vV]alue-[0-9]", "VALUE[^2].?", "VAL3") // case-sensitive
my_field regex~  ("value-[0-9]", "value[^2].?", "val3")   // case-insensitive
```

`in` (case-sensitive)
:   Returns `true` if the value is contained in the provided list. For case-insensitive matching, use `in~`.

`not in` (case-sensitive)
:   Returns `true` if the value is not contained in the provided list. For case-insensitive matching, use `not in~`.

`:` (case-insensitive)
:   Returns `true` if the string is contained in the provided list. Can only be used to compare strings.

`like` (case-sensitive)
:   Returns `true` if the string matches a [wildcard pattern](#eql-syntax-wildcards) in the provided list. Can only be used to compare strings. For case-insensitive matching, use `like~`.

`regex` (case-sensitive)
:   Returns `true` if the string matches a regular expression pattern in the provided list. For supported regular expression syntax, see [*Regular expression syntax*](/reference/query-languages/query-dsl/regexp-syntax.md). Can only be used to compare strings. For case-insensitive matching, use `regex~`.


### Math operators [eql-syntax-math-operators]

```eql
+  -  *  /  %
```

`+` (add)
:   Adds the values to the left and right of the operator.

`-` (subtract)
:   Subtracts the value to the right of the operator from the value to the left.

`*` (multiply)
:   Multiplies the values to the left and right of the operator.

`/` (divide)
:   Divides the value to the left of the operator by the value to the right.

    ::::{warning}
    :name: eql-divide-operator-float-rounding

    If both the dividend and divisor are integers, the divide (`\`) operation *rounds down* any returned floating point numbers to the nearest integer. To avoid rounding, convert either the dividend or divisor to a float.

    **Example**<br> The `process.args_count` field is a [`long`](/reference/elasticsearch/mapping-reference/number.md) integer field containing a count of process arguments.

    A user might expect the following EQL query to only match events with a `process.args_count` value of `4`.

    ```eql
    process where ( 4 / process.args_count ) == 1
    ```

    However, the EQL query matches events with a `process.args_count` value of `3` or `4`.

    For events with a `process.args_count` value of `3`, the divide operation returns a float of `1.333...`, which is rounded down to `1`.

    To match only events with a `process.args_count` value of `4`, convert either the dividend or divisor to a float.

    The following EQL query changes the integer `4` to the equivalent float `4.0`.

    ```eql
    process where ( 4.0 / process.args_count ) == 1
    ```

    ::::


`%` (modulo)
:   Divides the value to the left of the operator by the value to the right. Returns only the remainder.


### Match any condition [eql-syntax-match-any-condition]

To match events solely on event category, use the `where true` condition.

For example, the following EQL query matches any `file` events:

```eql
file where true
```

To match any event, you can combine the `any` keyword with the `where true` condition:

```eql
any where true
```


## Optional fields [eql-syntax-optional-fields]

By default, an EQL query can only contain fields that exist in the dataset you’re searching. A field exists in a dataset if it has an [explicit](docs-content://manage-data/data-store/mapping/explicit-mapping.md), [dynamic](docs-content://manage-data/data-store/mapping/dynamic-mapping.md), or [runtime](/reference/query-languages/eql.md#eql-use-runtime-fields) mapping. If an EQL query contains a field that doesn’t exist, it returns an error.

If you aren’t sure if a field exists in a dataset, use the `?` operator to mark the field as optional. If an optional field doesn’t exist, the query replaces it with `null` instead of returning an error.

**Example**<br> In the following query, the `user.id` field is optional.

```eql
network where ?user.id != null
```

If the `user.id` field exists in the dataset you’re searching, the query matches any `network` event that contains a `user.id` value. If the `user.id` field doesn’t exist in the dataset, EQL interprets the query as:

```eql
network where null != null
```

In this case, the query matches no events.


### Check if a field exists [eql-syntax-check-field-exists]

To match events containing any value for a field, compare the field to `null` using the `!=` operator:

```eql
?my_field != null
```

To match events that do not contain a field value, compare the field to `null` using the `==` operator:

```eql
?my_field == null
```


## Strings [eql-syntax-strings]

Strings are enclosed in double quotes (`"`).

```eql
"hello world"
```

Strings enclosed in single quotes (`'`) are not supported.


### Escape characters in a string [eql-syntax-escape-characters]

When used within a string, special characters, such as a carriage return or double quote (`"`), must be escaped with a preceding backslash (`\`).

```eql
"example \r of \" escaped \n characters"
```

| Escape sequence | Literal character |
| --- | --- |
| `\n` | Newline (linefeed) |
| `\r` | Carriage return |
| `\t` | Tab |
| `\\` | Backslash (`\`) |
| `\"` | Double quote (`"`) |

You can escape Unicode characters using a hexadecimal `\u{{XXXXXXXX}}` escape sequence. The hexadecimal value can be 2-8 characters and is case-insensitive. Values shorter than 8 characters are zero-padded. You can use these escape sequences to include non-printable or right-to-left (RTL) characters in your strings. For example, you can escape a [right-to-left mark (RLM)](https://en.wikipedia.org/wiki/Right-to-left_mark) as `\u{{200f}}`, `\u{{200F}}`, or `\u{{0000200f}}`.

::::{important}
The single quote (`'`) character is reserved for future use. You cannot use an escaped single quote (`\'`) for literal strings. Use an escaped double quote (`\"`) instead.
::::



### Raw strings [eql-syntax-raw-strings]

Raw strings treat special characters, such as backslashes (`\`), as literal characters. Raw strings are enclosed in three double quotes (`"""`).

```eql
"""Raw string with a literal double quote " and blackslash \ included"""
```

A raw string cannot contain three consecutive double quotes (`"""`). Instead, use a regular string with the `\"` escape sequence.

```eql
"String containing \"\"\" three double quotes"
```


### Wildcards [eql-syntax-wildcards]

For string comparisons using the `:` operator or `like` keyword, you can use the `*` and `?` wildcards to match specific patterns. The `*` wildcard matches zero or more characters:

```eql
my_field : "doc*"     // Matches "doc", "docs", or "document" but not "DOS"
my_field : "*doc"     // Matches "adoc" or "asciidoc"
my_field : "d*c"      // Matches "doc" or "disc"

my_field like "DOC*"  // Matches "DOC", "DOCS", "DOCs", or "DOCUMENT" but not "DOS"
my_field like "D*C"   // Matches "DOC", "DISC", or "DisC"
```

The `?` wildcard matches exactly one character:

```eql
my_field : "doc?"     // Matches "docs" but not "doc", "document", or "DOS"
my_field : "?doc"     // Matches "adoc" but not "asciidoc"
my_field : "d?c"      // Matches "doc" but not "disc"

my_field like "DOC?"  // Matches "DOCS" or "DOCs" but not "DOC", "DOCUMENT", or "DOS"
my_field like "D?c"   // Matches "DOC" but not "DISC"
```

The `:` operator and `like` keyword also support wildcards in [list lookups](#eql-syntax-lookup-operators):

```eql
my_field : ("doc*", "f*o", "ba?", "qux")
my_field like ("Doc*", "F*O", "BA?", "QUX")
```


## Sequences [eql-sequences]

You can use EQL sequences to describe and match an ordered series of events. Each item in a sequence is an event category and event condition, surrounded by square brackets (`[ ]`). Events are listed in ascending chronological order, with the most recent event listed last.

```eql
sequence
  [ event_category_1 where condition_1 ]
  [ event_category_2 where condition_2 ]
  ...
```

**Example**<br> The following EQL sequence query matches this series of ordered events:

1. Start with an event with:

    * An event category of `file`
    * A `file.extension` of `exe`

2. Followed by an event with an event category of `process`

```eql
sequence
  [ file where file.extension == "exe" ]
  [ process where true ]
```


### `with maxspan` statement [eql-with-maxspan-keywords]

You can use `with maxspan` to constrain a sequence to a specified timespan. All events in a matching sequence must occur within this duration, starting at the first event’s timestamp.

`maxspan` accepts [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units) arguments.

```eql
sequence with maxspan=30s
  [ event_category_1 where condition_1 ] by field_baz
  [ event_category_2 where condition_2 ] by field_bar
  ...
```

**Example**<br> The following sequence query uses a `maxspan` value of `15m` (15 minutes). Events in a matching sequence must occur within 15 minutes of the first event’s timestamp.

```eql
sequence with maxspan=15m
  [ file where file.extension == "exe" ]
  [ process where true ]
```


### Missing events [eql-missing-events]

Use `!` to match missing events: events in a timespan-constrained sequence that do not meet a given condition.

```eql
sequence with maxspan=1h
  [ event_category_1 where condition_1 ]
  ![ event_category_2 where condition_2 ]
  [ event_category_3 where condition_3 ]
  ...
```

Missing event clauses can be used at the beginning, at the end, and/or in the middle of a sequence, in any combination with positive event clauses. A sequence can have multiple missing event clauses, but needs to have at least one positive clause. [`with maxspan`](#eql-with-maxspan-keywords) is mandatory when missing event clauses are present.

**Example**<br> The following sequence query finds logon events that are not followed within 5 seconds by a logoff event.

```eql
sequence by host.name, user.name with maxspan=5s
  [ authentication where event.code : "4624" ]
  ![ authentication where event.code : "4647" ]
```


### `by` keyword [eql-by-keyword]

Use the `by` keyword in a sequence query to only match events that share the same values, even if those values are in different fields. These shared values are called join keys. If a join key should be in the same field across all events, use `sequence by`.

```eql
sequence by field_foo
  [ event_category_1 where condition_1 ] by field_baz
  [ event_category_2 where condition_2 ] by field_bar
  ...
```

**Example**<br> The following sequence query uses the `by` keyword to constrain matching events to:

* Events with the same `user.name` value
* `file` events with a `file.path` value equal to the following `process` event’s `process.executable` value.

```eql
sequence
  [ file where file.extension == "exe" ] by user.name, file.path
  [ process where true ] by user.name, process.executable
```

Because the `user.name` field is shared across all events in the sequence, it can be included using `sequence by`. The following sequence is equivalent to the prior one.

```eql
sequence by user.name
  [ file where file.extension == "exe" ] by file.path
  [ process where true ] by process.executable
```

You can combine `sequence by` and `with maxspan` to constrain a sequence by both field values and a timespan.

```eql
sequence by field_foo with maxspan=30s
  [ event_category_1 where condition_1 ]
  [ event_category_2 where condition_2 ]
  ...
```

**Example**<br> The following sequence query uses `sequence by` and `with maxspan` to only match a sequence of events that:

* Share the same `user.name` field values
* Occur within `15m` (15 minutes) of the first matching event

```eql
sequence by user.name with maxspan=15m
  [ file where file.extension == "exe" ]
  [ process where true ]
```


### Optional `by` fields [eql-syntax-optional-by-fields]

By default, a join key must be a non-`null` field value. To allow `null` join keys, use the `?` operator to mark the `by` field as [optional](#eql-syntax-optional-fields). This is also helpful if you aren’t sure the dataset you’re searching contains the `by` field.

**Example**<br> The following sequence query uses `sequence by` to constrain matching events to:

* Events with the same `process.pid` value, excluding `null` values. If the `process.pid` field doesn’t exist in the dataset you’re searching, the query returns an error.
* Events with the same `process.entity_id` value, including `null` values. If an event doesn’t contain the `process.entity_id` field, its `process.entity_id` value is considered `null`. This applies even if the `process.pid` field doesn’t exist in the dataset you’re searching.

```eql
sequence by process.pid, ?process.entity_id
  [process where process.name == "regsvr32.exe"]
  [network where true]
```


### `until` keyword [eql-until-keyword]

You can use the `until` keyword to specify an expiration event for a sequence. If this expiration event occurs *between* matching events in a sequence, the sequence expires and is not considered a match. If the expiration event occurs *after* matching events in a sequence, the sequence is still considered a match. The expiration event is not included in the results.

```eql
sequence
  [ event_category_1 where condition_1 ]
  [ event_category_2 where condition_2 ]
  ...
until [ event_category_3 where condition_3 ]
```

**Example**<br> A dataset contains the following event sequences, grouped by shared IDs:

```txt
A, B
A, B, C
A, C, B
```

The following EQL query searches the dataset for sequences containing event `A` followed by event `B`. Event `C` is used as an expiration event.

```eql
sequence by ID
  A
  B
until C
```

The query matches sequences `A, B` and `A, B, C` but not `A, C, B`.

::::{tip}
The `until` keyword can be useful when searching for process sequences in Windows event logs.

In Windows, a process ID (PID) is unique only while a process is running. After a process terminates, its PID can be reused.

You can search for a sequence of events with the same PID value using the `by` and `sequence by` keywords.

**Example**<br> The following EQL query uses the `sequence by` keyword to match a sequence of events that share the same `process.pid` value.

```eql
sequence by process.pid
  [ process where event.type == "start" and process.name == "cmd.exe" ]
  [ process where file.extension == "exe" ]
```

However, due to PID reuse, this can result in a matching sequence that contains events across unrelated processes. To prevent false positives, you can use the `until` keyword to end matching sequences before a process termination event.

The following EQL query uses the `until` keyword to end sequences before `process` events with an `event.type` of `stop`. These events indicate a process has been terminated.

```eql
sequence by process.pid
  [ process where event.type == "start" and process.name == "cmd.exe" ]
  [ process where file.extension == "exe" ]
until [ process where event.type == "stop" ]
```

::::



### `with runs` statement [eql-with-runs-statement]

Use a `with runs` statement to run the same event criteria successively within a sequence query. For example:

```eql
sequence
  [ process where event.type == "creation" ]
  [ library where process.name == "regsvr32.exe" ] with runs=3
  [ registry where true ]
```

is equivalent to:

```eql
sequence
  [ process where event.type == "creation" ]
  [ library where process.name == "regsvr32.exe" ]
  [ library where process.name == "regsvr32.exe" ]
  [ library where process.name == "regsvr32.exe" ]
  [ registry where true ]
```

The `runs` value must be between `1` and `100` (inclusive).

You can use a `with runs` statement with the [`by` keyword](#eql-by-keyword). For example:

```eql
sequence
  [ process where event.type == "creation" ] by process.executable
  [ library where process.name == "regsvr32.exe" ] by dll.path with runs=3
```


## Samples [eql-samples]

You can use EQL samples to describe and match a chronologically unordered series of events. All events in a sample share the same value for one or more fields that are specified using the [`by` keyword](#eql-by-keyword) (join keys). Each item in a sample is an event category and event condition, surrounded by square brackets (`[ ]`). Events are listed in the order of the filters they match.

```eql
sample by join_key
  [ event_category_1 where condition_1 ]
  [ event_category_2 where condition_2 ]
  ...
```

**Example**<br> The following EQL sample query returns up to 10 samples with unique values for `host`. Each sample consists of two events:

1. Start with an event with:

    * An event category of `file`
    * A `file.extension` of `exe`

2. Followed by an event with an event category of `process`

```eql
sample by host
  [ file where file.extension == "exe" ]
  [ process where true ]
```

Sample queries do not take into account the chronological ordering of events. The `with maxspan` and `with runs` statements as well as the `until` keyword are not supported.


## Functions [eql-functions]

You can use EQL functions to convert data types, perform math, manipulate strings, and more. For a list of supported functions, see [Function reference](/reference/query-languages/eql/eql-function-ref.md).


### Case-insensitive functions [eql-case-insensitive-functions]

Most EQL functions are case-sensitive by default. To make a function case-insensitive, use the `~` operator after the function name:

```eql
stringContains(process.name,".exe")  // Matches ".exe" but not ".EXE" or ".Exe"
stringContains~(process.name,".exe") // Matches ".exe", ".EXE", or ".Exe"
```


### How functions impact search performance [eql-how-functions-impact-search-performance]

Using functions in EQL queries can result in slower search speeds. If you often use functions to transform indexed data, you can speed up search by making these changes during indexing instead. However, that often means slower index speeds.

**Example**<br> An index contains the `file.path` field. `file.path` contains the full path to a file, including the file extension.

When running EQL searches, users often use the `endsWith` function with the `file.path` field to match file extensions:

```eql
file where endsWith(file.path,".exe") or endsWith(file.path,".dll")
```

While this works, it can be repetitive to write and can slow search speeds. To speed up search, you can do the following instead:

1. [Add a new field](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping), `file.extension`, to the index. The `file.extension` field will contain only the file extension from the `file.path` field.
2. Use an [ingest pipeline](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md) containing the [`grok`](/reference/enrich-processor/grok-processor.md) processor or another preprocessor tool to extract the file extension from the `file.path` field before indexing.
3. Index the extracted file extension to the `file.extension` field.

These changes may slow indexing but allow for faster searches. Users can use the `file.extension` field instead of multiple `endsWith` function calls:

```eql
file where file.extension in ("exe", "dll")
```

We recommend testing and benchmarking any indexing changes before deploying them in production. See [*Tune for indexing speed*](docs-content://deploy-manage/production-guidance/optimize-performance/indexing-speed.md) and [*Tune for search speed*](docs-content://deploy-manage/production-guidance/optimize-performance/search-speed.md).


## Pipes [eql-pipes]

EQL pipes filter, aggregate, and post-process events returned by an EQL query. You can use pipes to narrow down EQL query results or make them more specific.

Pipes are delimited using the pipe (`|`) character.

```eql
event_category where condition | pipe
```

**Example**<br> The following EQL query uses the `tail` pipe to return only the 10 most recent events matching the query.

```eql
authentication where agent.id == 4624
| tail 10
```

You can pass the output of a pipe to another pipe. This lets you use multiple pipes with a single query.

For a list of supported pipes, see [Pipe reference](/reference/query-languages/eql/eql-pipe-ref.md).


## Limitations [eql-syntax-limitations]

EQL has the following limitations.


### EQL uses the `fields` parameter [eql-uses-fields-parameter]

EQL retrieves field values using the search API’s [`fields` parameter](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#search-fields-param). Any limitations on the `fields` parameter also apply to EQL queries. For example, if `_source` is disabled for any returned fields or at index level, the values cannot be retrieved.


### Comparing fields [eql-compare-fields]

You cannot use EQL comparison operators to compare a field to another field. This applies even if the fields are changed using a [function](#eql-functions).


### Text fields are not supported [eql-text-fields]

EQL searches do not support [`text`](/reference/elasticsearch/mapping-reference/text.md) fields. To a search a `text` field, use the EQL search API’s [Query DSL `filter`](/reference/query-languages/eql.md#eql-search-filter-query-dsl) parameter.


### EQL search on nested fields [eql-nested-fields]

You cannot use EQL to search the values of a [`nested`](/reference/elasticsearch/mapping-reference/nested.md) field or the sub-fields of a `nested` field. However, data streams and indices containing `nested` field mappings are otherwise supported.


### Differences from Endgame EQL syntax [eql-unsupported-syntax]

{{es}} EQL differs from the [Elastic Endgame EQL syntax](https://eql.readthedocs.io/en/latest/query-guide/index.html) as follows:

* In {{es}} EQL, most operators are case-sensitive. For example, `process_name == "cmd.exe"` is not equivalent to `process_name == "Cmd.exe"`.
* In {{es}} EQL, functions are case-sensitive. To make a function case-insensitive, use `~`, such as `endsWith~(process_name, ".exe")`.
* For case-insensitive equality comparisons, use the `:` operator. Both `*` and `?` are recognized wildcard characters.
* The `==` and `!=` operators do not expand wildcard characters. For example, `process_name == "cmd*.exe"` interprets `*` as a literal asterisk, not a wildcard.
* For wildcard matching, use the `like` keyword when case-sensitive and `like~` when case-insensitive. The `:` operator is equivalent to `like~`.
* For regular expression matching, use `regex` or `regex~`.
* `=` cannot be substituted for the `==` operator.
* Strings enclosed in single quotes (`'`) are not supported. Enclose strings in double quotes (`"`) instead.
* `?"` and `?'` do not indicate raw strings. Enclose raw strings in three double quotes (`"""`) instead.
* {{es}} EQL does not support:

    * Array functions:

        * [`arrayContains`](https://eql.readthedocs.io/en/latest/query-guide/functions.html#arrayContains)
        * [`arrayCount`](https://eql.readthedocs.io/en/latest/query-guide/functions.html#arrayCount)
        * [`arraySearch`](https://eql.readthedocs.io/en/latest/query-guide/functions.html#arraySearch)

    * The [`match`](https://eql.readthedocs.io/en/latest/query-guide//functions.html#match) function
    * [Joins](https://eql.readthedocs.io/en/latest/query-guide/joins.html)
    * [Lineage-related keywords](https://eql.readthedocs.io/en/latest/query-guide/basic-syntax.html#event-relationships):

        * `child of`
        * `descendant of`
        * `event of`

    * The following [pipes](https://eql.readthedocs.io/en/latest/query-guide/pipes.html):

        * [`count`](https://eql.readthedocs.io/en/latest/query-guide/pipes.html#count)
        * [`filter`](https://eql.readthedocs.io/en/latest/query-guide/pipes.html#filter)
        * [`sort`](https://eql.readthedocs.io/en/latest/query-guide/pipes.html#sort)
        * [`unique`](https://eql.readthedocs.io/en/latest/query-guide/pipes.html#unique)
        * [`unique_count`](https://eql.readthedocs.io/en/latest/query-guide/pipes.html#unique-count)



### How sequence queries handle matches [eql-how-sequence-queries-handle-matches]

[Sequence queries](#eql-sequences) don’t find all potential matches for a sequence. This approach would be too slow and costly for large event data sets. Instead, a sequence query handles pending sequence matches as a [state machine](https://en.wikipedia.org/wiki/Finite-state_machine):

* Each event item in the sequence query is a state in the machine.
* Only one pending sequence can be in each state at a time.
* If two pending sequences are in the same state at the same time, the most recent sequence overwrites the older one.
* If the query includes [`by` fields](#eql-by-keyword), the query uses a separate state machine for each unique `by` field value.

:::::{dropdown} Example
A data set contains the following `process` events in ascending chronological order:

```js
{ "index" : { "_id": "1" } }
{ "user": { "name": "root" }, "process": { "name": "attrib" }, ...}
{ "index" : { "_id": "2" } }
{ "user": { "name": "root" }, "process": { "name": "attrib" }, ...}
{ "index" : { "_id": "3" } }
{ "user": { "name": "elkbee" }, "process": { "name": "bash" }, ...}
{ "index" : { "_id": "4" } }
{ "user": { "name": "root" }, "process": { "name": "bash" }, ...}
{ "index" : { "_id": "5" } }
{ "user": { "name": "root" }, "process": { "name": "bash" }, ...}
{ "index" : { "_id": "6" } }
{ "user": { "name": "elkbee" }, "process": { "name": "attrib" }, ...}
{ "index" : { "_id": "7" } }
{ "user": { "name": "root" }, "process": { "name": "attrib" }, ...}
{ "index" : { "_id": "8" } }
{ "user": { "name": "elkbee" }, "process": { "name": "bash" }, ...}
{ "index" : { "_id": "9" } }
{ "user": { "name": "root" }, "process": { "name": "cat" }, ...}
{ "index" : { "_id": "10" } }
{ "user": { "name": "elkbee" }, "process": { "name": "cat" }, ...}
{ "index" : { "_id": "11" } }
{ "user": { "name": "root" }, "process": { "name": "cat" }, ...}
```
% NOTCONSOLE

An EQL sequence query searches the data set:

```eql
sequence by user.name
  [process where process.name == "attrib"]
  [process where process.name == "bash"]
  [process where process.name == "cat"]
```

The query’s event items correspond to the following states:

* State A:  `[process where process.name == "attrib"]`
* State B:  `[process where process.name == "bash"]`
* Complete: `[process where process.name == "cat"]`

:::{image} ../images/sequence-state-machine.svg
:alt: sequence state machine
:::

To find matching sequences, the query uses separate state machines for each unique `user.name` value. Based on the data set, you can expect two state machines: one for the `root` user and one for `elkbee`.

:::{image} ../images/separate-state-machines.svg
:alt: separate state machines
:::

Pending sequence matches move through each machine’s states as follows:

```txt
{ "index" : { "_id": "1" } }
{ "user": { "name": "root" }, "process": { "name": "attrib" }, ...}
// Creates sequence [1] in state A for the "root" user.
//
// +------------------------"root"------------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+     +------------+  |
// |  |    [1]    |     |           |     |            |  |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "2" } }
{ "user": { "name": "root" }, "process": { "name": "attrib" }, ...}
// Creates sequence [2] in state A for "root", overwriting sequence [1].
//
// +------------------------"root"------------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+     +------------+  |
// |  |    [2]    |     |           |     |            |  |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "3" } }
{ "user": { "name": "elkbee" }, "process": { "name": "bash" }, ...}
// Nothing happens. The "elkbee" user has no pending sequence to move
// from state A to state B.
//
// +-----------------------"elkbee"-----------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+     +------------+  |
// |  |           |     |           |     |            |  |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "4" } }
{ "user": { "name": "root" }, "process": { "name": "bash" }, ...}
// Sequence [2] moves out of state A for "root".
// State B for "root" now contains [2, 4].
// State A for "root" is empty.
//
// +------------------------"root"------------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+ --> +-----------+     +------------+  |
// |  |           |     |   [2, 4]  |     |            |  |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "5" } }
{ "user": { "name": "root" }, "process": { "name": "bash" }, ...}
// Nothing happens. State A is empty for "root".
//
// +------------------------"root"------------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+     +------------+  |
// |  |           |     |   [2, 4]  |     |            |  |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "6" } }
{ "user": { "name": "elkbee" }, "process": { "name": "attrib" }, ...}
// Creates sequence [6] in state A for "elkbee".
//
// +-----------------------"elkbee"-----------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+     +------------+  |
// |  |    [6]    |     |           |     |            |  |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "7" } }
{ "user": { "name": "root" }, "process": { "name": "attrib" }, ...}
// Creates sequence [7] in state A for "root".
// Sequence [2, 4] remains in state B for "root".
//
// +------------------------"root"------------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+     +------------+  |
// |  |    [7]    |     |   [2, 4]  |     |            |  |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "8" } }
{ "user": { "name": "elkbee" }, "process": { "name": "bash" }, ...}
// Sequence [6, 8] moves to state B for "elkbee".
// State A for "elkbee" is now empty.
//
// +-----------------------"elkbee"-----------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+ --> +-----------+     +------------+  |
// |  |           |     |   [6, 8]  |     |            |  |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "9" } }
{ "user": { "name": "root" }, "process": { "name": "cat" }, ...}
// Sequence [2, 4, 9] is complete for "root".
// State B for "root" is now empty.
// Sequence [7] remains in state A.
//
// +------------------------"root"------------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+ --> +------------+  |
// |  |    [7]    |     |           |     |  [2, 4, 9] |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "10" } }
{ "user": { "name": "elkbee" }, "process": { "name": "cat" }, ...}
// Sequence [6, 8, 10] is complete for "elkbee".
// State A and B for "elkbee" are now empty.
//
// +-----------------------"elkbee"-----------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+ --> +------------+  |
// |  |           |     |           |     | [6, 8, 10] |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+

{ "index" : { "_id": "11" } }
{ "user": { "name": "root" }, "process": { "name": "cat" }, ...}
// Nothing happens.
// The machines for "root" and "elkbee" remain the same.
//
// +------------------------"root"------------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+     +------------+  |
// |  |    [7]    |     |           |     |  [2, 4, 9] |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+
//
// +-----------------------"elkbee"-----------------------+
// |  +-----------+     +-----------+     +------------+  |
// |  |  State A  |     |  State B  |     |  Complete  |  |
// |  +-----------+     +-----------+     +------------+  |
// |  |           |     |           |     | [6, 8, 10] |
// |  +-----------+     +-----------+     +------------+  |
// +------------------------------------------------------+
```

:::::


