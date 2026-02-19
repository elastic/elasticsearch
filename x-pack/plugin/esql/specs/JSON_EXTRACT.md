# ES|QL `JSON_EXTRACT` Function Specification

## Function Signature

```
JSON_EXTRACT(string, path) → keyword
```

## Status

**Preview / Snapshot-only** — Gated behind `FN_JSON_EXTRACT` capability with `Build.current().isSnapshot()`. Marked `preview = true` in `@FunctionInfo` and registered in `snapshotFunctions()`.

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `string` | `keyword`, `text`, `_source` | A string containing valid JSON, or the `_source` metadata field. Returns `null` if `null`. Emits a warning if the value is not valid JSON. |
| `path` | `keyword`, `text` | A dot-notation path expression identifying the value to extract. Returns `null` if `null`. |

## Return Type

`keyword` — always returns a string representation of the extracted value:

| JSON Value Type | Returned As |
|-----------------|-------------|
| String | The string value without surrounding quotes |
| Number | String representation (e.g., `"42"`, `"3.14"`) |
| Boolean | `"true"` or `"false"` |
| JSON `null` | ES|QL `null` (no warning) |
| Object | JSON string (e.g., `{"nested":"value"}`) |
| Array | JSON string (e.g., `[1,2,3]`) |

## Path Syntax

Path matching is **case-sensitive** (per the JSON specification). An optional JSONPath `$` prefix is accepted (e.g., `$.name` is equivalent to `name`, `$['key']` is equivalent to `['key']`).

| Syntax | Meaning | Example |
|--------|---------|---------|
| `key` | Top-level field | `name` extracts `$.name` |
| `key1.key2` | Nested field | `address.city` extracts `$.address.city` |
| `key[N]` | Array index (0-based) | `items[0]` extracts first element of `$.items` |
| `key1.key2[N].key3` | Mixed nesting | `orders[0].id` extracts `$.orders[0].id` |
| `['key']` or `["key"]` | Quoted key (for special chars) | `['user.name']` extracts a key literally named `user.name` |
| `$.key` or `$['key']` | JSONPath prefix (optional) | `$.name` is equivalent to `name` |

## Error and Edge Case Behavior

| Scenario | Behavior |
|----------|----------|
| `string` is `null` | Returns `null` |
| `path` is `null` | Returns `null` |
| `string` is not valid JSON | Returns `null`, emits warning |
| `path` is malformed (empty, leading/trailing dot, consecutive dots, empty brackets) | Returns `null`, emits warning |
| `path` does not exist in the JSON | Returns `null`, emits warning |
| Extracted value is JSON `null` | Returns `null` (no warning) |
| `null` inside a JSON array (e.g., `[1, null, 3]`) | Returns `null` (no warning) |
| Array index out of bounds | Returns `null`, emits warning |
| Path traverses through a non-object/non-array | Returns `null`, emits warning |
| Duplicate JSON keys (e.g., `{"foo":1, "foo":2}`) | Returns the **first** matching value (streaming parser semantics). Note: `jq` and `psql` return the last value. |

## Examples

**Simple extraction:**
```esql
ROW json = '{"name":"Alice","age":30}'
| EVAL name = JSON_EXTRACT(json, "name")
```
→ `Alice`

**Nested field:**
```esql
ROW json = '{"user":{"address":{"city":"London"}}}'
| EVAL city = JSON_EXTRACT(json, "user.address.city")
```
→ `London`

**Array indexing:**
```esql
ROW json = '{"orders":[{"id":1,"item":"book"},{"id":2,"item":"pen"}]}'
| EVAL second_item = JSON_EXTRACT(json, "orders[1].item")
```
→ `pen`

**Extract nested object as JSON string:**
```esql
ROW json = '{"user":{"name":"Alice","age":30}}'
| EVAL user = JSON_EXTRACT(json, "user")
```
→ `{"name":"Alice","age":30}`

**Extract from _source:**
```esql
FROM logs METADATA _source
| EVAL status = JSON_EXTRACT(_source, "response.status")
| WHERE status == "200"
```

## Implementation Notes

- **Streaming JSON parser**: Uses `XContentParser` to avoid creating intermediate objects for paths not being extracted
- **Constant path optimization**: When the `path` argument is a foldable constant (the common case), it is parsed once into a `ParsedPath` record and reused across all rows via a specialized `processConstant` evaluator (following the `Hash.process`/`Hash.processConstant` pattern). Path splitting uses manual character iteration instead of `String.split(regex)` to avoid regex compilation overhead.
- **`_source` support**: First parameter accepts `keyword`, `text`, or `_source` metadata field
- **Duplicate keys**: Returns the first matching value (first-match streaming parser semantics). `jq` and `psql` return the last value — switching to last-match can be considered as a follow-up.

## Test Coverage

- Unit tests (`JsonExtractTests.java`) — covers both generic and constant-path evaluators
- Serialization tests (`JsonExtractSerializationTests.java`)
- Error tests (`JsonExtractErrorTests.java`)
- CSV spec integration tests (`json_extract.csv-spec`) — includes duplicate keys, null-in-array
- YAML REST test configuration (`60_usage.yml`)
- Changelog entry (`docs/changelog/142375.yaml`)
