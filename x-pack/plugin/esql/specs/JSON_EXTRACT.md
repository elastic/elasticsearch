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
| `path` | `keyword`, `text` | A path expression identifying the value to extract, using a subset of JSONPath syntax. The `$` prefix is optional (`$.name` and `name` are equivalent). Returns `null` if `null`. |

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

## Path Syntax (JSONPath Subset)

This function implements a subset of [JSONPath (RFC 9535)](https://datatracker.ietf.org/doc/rfc9535/). Paths can be written with or without the `$` prefix — `$.name` and `name` are equivalent.

### Supported features

| Syntax | Meaning | Example |
|--------|---------|---------|
| `key` | Top-level field | `name` → value of `$.name` |
| `key1.key2` | Nested field (dot notation) | `user.address.city` |
| `key[N]` | Array index (0-based) | `items[0]` → first element |
| `key1[N].key2` | Mixed nesting | `orders[1].item` |
| `['key']` or `["key"]` | Quoted key (for special characters) | `['user.name']` → literal key `user.name` |
| `$` | Root selector (entire document) | `$` → the whole JSON input |
| `$.key` | Prefixed dot notation | `$.name` equivalent to `name` |
| `$['key']` | Prefixed bracket notation | `$['user.name']` equivalent to `['user.name']` |
| `$[N]` | Prefixed array index | `$[0]` → first element of root array |
| _(empty string)_ | Root selector (same as `$`) | `""` → the whole JSON input |

Path matching is **case-sensitive** per the JSON specification. Escaped quotes are supported within quoted brackets (e.g., `['it\'s']`).

### Not supported (JSONPath features out of scope)

These JSONPath (RFC 9535) features are not supported and will not be added to this function. They require multi-value return semantics, which would need a separate function (e.g., `JSON_QUERY`).

| Feature | Syntax | Reason |
|---------|--------|--------|
| Wildcard | `$.*`, `$[*]` | Returns multiple values |
| Recursive descent | `$..name` | Returns multiple values |
| Array slicing | `$[0:3]`, `$[::2]` | Returns multiple values |
| Union / multiple indices | `$[0,1]`, `$['a','b']` | Returns multiple values |
| Filter expressions | `$[?(@.price < 10)]` | Returns multiple values |
| Negative array indices | `$[-1]` | Not supported (consistent with ClickHouse) |
| Built-in functions | `length()`, `count()` | Different return type semantics |

## Error and Edge Case Behavior

| Scenario | Behavior |
|----------|----------|
| `string` is `null` | Returns `null` |
| `path` is `null` | Returns `null` |
| `string` is not valid JSON | Returns `null`, emits warning |
| `path` is malformed (leading/trailing dot, consecutive dots, empty brackets, unterminated quote) | Returns `null`, emits warning |
| `path` does not exist in the JSON | Returns `null`, emits warning |
| Extracted value is JSON `null` | Returns `null` (no warning) |
| `null` inside a JSON array (e.g., `[1, null, 3]`) | Returns `null` (no warning) |
| Array index out of bounds | Returns `null`, emits warning |
| Path traverses through a non-object/non-array | Returns `null`, emits warning |
| Duplicate JSON keys (e.g., `{"foo":1, "foo":2}`) | Returns the **first** matching value (streaming parser semantics) |

## Examples

**Simple extraction:**
```esql
ROW json = '{"name":"Alice","age":30}'
| EVAL name = JSON_EXTRACT(json, "name")
```
→ `Alice`

**Nested field (dot notation):**
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

**Quoted bracket notation for keys with special characters:**
```esql
ROW json = '{"user.name":"Alice","age":30}'
| EVAL name = JSON_EXTRACT(json, "['user.name']")
```
→ `Alice`

**With JSONPath `$` prefix (equivalent to bare path):**
```esql
ROW json = '{"name":"Alice"}'
| EVAL name = JSON_EXTRACT(json, "$.name")
```
→ `Alice`

**Root selector (`$` returns entire document):**
```esql
ROW json = '{"name":"Alice","age":30}'
| EVAL doc = JSON_EXTRACT(json, "$")
```
→ `{"name":"Alice","age":30}`

**Extract from `_source`:**
```esql
FROM logs METADATA _source
| EVAL status = JSON_EXTRACT(_source, "response.status")
| WHERE status == "200"
```

## Implementation Notes

- **JSONPath subset**: Implements dot notation, bracket notation (numeric and quoted), and the `$` root selector from RFC 9535. Multi-value features (wildcards, recursive descent, filters, slicing) are out of scope.
- **Streaming JSON parser**: Uses `XContentParser` to avoid creating intermediate objects for paths not being extracted.
- **Constant path optimization**: When the `path` argument is a foldable constant (the common case), it is parsed once into a `ParsedPath` record and reused across all rows via a specialized `processConstant` evaluator. Path splitting uses manual character iteration instead of `String.split(regex)` to avoid regex compilation overhead.
- **`_source` support**: First parameter accepts `keyword`, `text`, or `_source` metadata field.
- **Duplicate keys**: Returns the first matching value (first-match streaming parser semantics). Behavior is undefined per RFC 8259; ClickHouse also uses first-match.

## Test Coverage

- Unit tests (`JsonExtractTests.java`) — covers both generic and constant-path evaluators
- Static edge case tests (`JsonExtractStaticTests.java`) — path parsing, Unicode, bracket notation, `$` prefix, large inputs
- Serialization tests (`JsonExtractSerializationTests.java`)
- Error tests (`JsonExtractErrorTests.java`)
- CSV spec integration tests (`json_extract.csv-spec`) — includes duplicate keys, null-in-array
- YAML REST test configuration (`60_usage.yml`)
- Changelog entry (`docs/changelog/142375.yaml`)
