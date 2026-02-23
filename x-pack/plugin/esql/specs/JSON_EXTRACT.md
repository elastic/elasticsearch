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
| `['key']` or `["key"]` | Quoted key (for special characters or empty keys) | `['user.name']` → literal key `user.name` |
| `$` | Root selector (entire document) | `$` → the whole JSON input |
| `$.key` | Prefixed dot notation | `$.name` equivalent to `name` |
| `$['key']` | Prefixed bracket notation | `$['user.name']` equivalent to `['user.name']` |
| `$[N]` | Prefixed array index | `$[0]` → first element of root array |
| _(empty string)_ | Root selector (same as `$`) | `""` → the whole JSON input |

Path matching is **case-sensitive** per the JSON specification.

Per RFC 9535, **dot notation and quoted bracket notation are interchangeable** for simple keys — `a.b`, `a['b']`, and `a["b"]` all produce the same result. The quoted form is only required for keys that contain special characters (dots, spaces, brackets, etc.) that dot notation cannot express. Empty string keys are supported via quoted bracket notation (`['']`), since an empty string is a valid JSON member name per RFC 9535.

Escape sequences are supported within quoted brackets: `\'` for a literal single quote (e.g., `['it\'s']`), `\"` for a literal double quote, and `\\` for a literal backslash (e.g., `['a\\b']` accesses key `a\b`).

Per RFC 9535, **optional blank space** (spaces, tabs, newlines, carriage returns) is allowed inside brackets — `[ 0 ]` and `[ 'key' ]` are equivalent to `[0]` and `['key']`.

When `$` is used, it must be followed by `.` or `[` — bare `$name` is not valid JSONPath and will produce an error.

### Not supported (JSONPath features out of scope)

These JSONPath (RFC 9535) features are not supported and will not be added to this function. They require multi-value return semantics, which would need a separate function (e.g., `JSON_QUERY`).

| Feature | Syntax |
|---------|--------|
| Wildcard | `$.*`, `$[*]` |
| Recursive descent | `$..name` |
| Array slicing | `$[0:3]`, `$[::2]` |
| Union / multiple indices | `$[0,1]`, `$['a','b']` |
| Filter expressions | `$[?(@.price < 10)]` |
| Negative array indices | `$[-1]` |
| Built-in functions | `length()`, `count()` |

## Error and Edge Case Behavior

| Scenario | Behavior |
|----------|----------|
| `string` is `null` | Returns `null` |
| `path` is `null` | Returns `null` |
| `string` is not valid JSON | Returns `null`, emits warning |
| `path` is malformed (leading/trailing dot, consecutive dots, empty brackets, unterminated quote, `$name` without `.` or `[`) | Returns `null`, emits warning |
| `path` does not exist in the JSON | Returns `null`, emits warning |
| Leading zeros in array index (e.g., `[01]`, `[007]`) | Returns `null`, emits warning (RFC 9535 disallows octal-like indices) |
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

- **JSONPath subset**: Implements dot notation, bracket notation (numeric and quoted), the `$` root selector, optional blank space inside brackets, and escape sequences inside quoted keys from RFC 9535. Rejects leading zeros in array indices per the RFC. Multi-value features (wildcards, recursive descent, filters, slicing) are out of scope.
- **Streaming JSON parser**: Uses `XContentParser` to avoid creating intermediate objects for paths not being extracted.
- **Multi-encoding `_source` support**: The `_source` metadata field may be stored in any of Elasticsearch's four XContent encodings — JSON, SMILE, CBOR, or YAML. `JsonExtract` detects the encoding via `XContentFactory.xContentType()` and creates the appropriate parser, so it works regardless of the underlying storage format.
- **Structure serialization**: When extracting objects or arrays, uses `XContentBuilder.copyCurrentStructure(parser)` to serialize the sub-structure back to a JSON string. This delegates to the established Elasticsearch pattern for token-by-token structure copying, handling all encodings and correct RFC 8259 escaping. A future optimization to use zero-copy byte slicing (once `XContentParser` exposes byte offsets) is tracked in [#142873](https://github.com/elastic/elasticsearch/issues/142873).
- **Constant path optimization**: When the `path` argument is a foldable constant (the common case), it is parsed once into a `JsonPath` object and reused across all rows via a specialized `processConstant` evaluator. Path splitting uses manual character iteration instead of `String.split(regex)` to avoid regex compilation overhead.
- **Error reporting**: Parse errors include the full path, a human-readable reason, and a character position. For constant paths, positions can be offset to refer to the query text rather than the path string via the `errorPositionOffset` parameter.
- **Duplicate keys**: Returns the first matching value (first-match streaming parser semantics). Behavior is undefined per RFC 8259; ClickHouse also uses first-match.

## Test Coverage

- **Unit tests** (`JsonExtractTests.java`) — parameterized type combination/warning suppliers, plus ~70 inline tests covering: root accessors, `$` prefix, bracket notation, Unicode, escaped characters, deep nesting, large JSON, constant path evaluator type checks, numeric edge cases, duplicate keys, null-in-array, empty structures, unsupported JSONPath syntax, XContent encoding round-trips (JSON, SMILE, CBOR, YAML), empty input, type mismatch navigation (key into array, index into object, key into scalar), and array index out of bounds. Includes 4 randomized tests across all four XContent encodings.
- **Path parsing tests** (`JsonPathTests.java`) — dot notation, bracket notation, quoted keys, `$` prefix, whitespace inside brackets, escape sequences, empty string keys, error cases with full message assertions, error position offsets. Includes randomized tests for round-trip parsing, dot/bracket equivalence, quoted keys with special characters, escape sequences, whitespace, and `$` prefix variations.
- **Serialization tests** (`JsonExtractSerializationTests.java`)
- **Error tests** (`JsonExtractErrorTests.java`)
- **CSV spec integration tests** (`json_extract.csv-spec`) — includes duplicate keys, null-in-array
- **YAML REST test configuration** (`60_usage.yml`)
- **Changelog entry** (`docs/changelog/142375.yaml`)
