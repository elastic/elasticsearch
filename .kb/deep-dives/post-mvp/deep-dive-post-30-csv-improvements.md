# Deep Dive: CSV Improvements -- Configurable Delimiters, TSV Support, Better Type Inference, Quoting Modes

**Item:** Post-MVP CSV improvements
**Date:** 2026-03-03
**Status:** Post-MVP (quality-of-life)

---

## 1. CsvFormatReader Implementation Analysis

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/src/main/java/org/elasticsearch/xpack/esql/datasource/csv/CsvFormatReader.java`

### Library and Structure

The CSV reader uses **Jackson CSV** (`jackson-dataformat-csv` version 2.15.0, configured in `build-tools-internal/version.properties` line 9). The dependency is declared in:

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/build.gradle` (line 31)
```java
implementation "com.fasterxml.jackson.dataformat:jackson-dataformat-csv:${versions.jackson}"
```

The reader has two main code paths:
1. **Schema discovery** (`metadata()` at line 69, `readSchema()` at line 75) -- reads first non-comment line, parses `name:type` pairs
2. **Data reading** (`read()` at line 95) -- creates a `CsvBatchIterator` inner class that handles actual parsing

### Jackson CSV Parser Configuration (lines 176-237)

The `CsvBatchIterator` constructor (line 171) creates a `CsvMapper` with these features:
```java
this.csvMapper = new CsvMapper();
this.csvMapper.enable(CsvParser.Feature.TRIM_SPACES);      // line 177
this.csvMapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);  // line 178
this.csvMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);     // line 179
```

Then at line 231, the CsvSchema is constructed:
```java
CsvSchema csvSchema = CsvSchema.emptySchema()
    .withColumnSeparator(',')   // line 232 -- HARDCODED comma
    .withQuoteChar('"')         // line 233
    .withEscapeChar('\\')       // line 234 -- BUG: default should be -1 (none)
    .withNullValue("");         // line 235
```

---

## 2. Confirmed Bugs (from TP-08 Analysis)

### Bug 1: `.withEscapeChar('\\')` (line 234)

Jackson CSV's **default** escape character is `-1` (disabled). Setting it to backslash changes CSV semantics: `\"` becomes an escaped quote instead of requiring the RFC 4180 doubling convention (`""`). This breaks standard CSV files where backslashes appear in data values.

**Impact:** A field containing `C:\Users\file` would be parsed incorrectly because `\U` and `\f` would be interpreted as escape sequences.

**Fix:** Remove `.withEscapeChar('\\')` or use `.withoutEscapeChar()`. If escape support is desired, it should be opt-in via WITH clause configuration.

### Bug 2: `.withColumnSeparator(',')` hardcoded (line 232)

The delimiter is hardcoded to comma. Despite the plugin registering `.tsv` as a supported extension (line 109), tab-separated files will be parsed incorrectly -- the entire TSV line will be read as a single column value.

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/src/main/java/org/elasticsearch/xpack/esql/datasource/csv/CsvFormatReader.java` (line 109):
```java
public List<String> fileExtensions() {
    return List.of(".csv", ".tsv");  // TSV registered but comma delimiter used
}
```

**Fix:** Detect extension from `StorageObject.path()` and set delimiter to `\t` for `.tsv` files, or accept delimiter from config.

### Bug 3: `schemaLine.split(",")` (line 118)

The schema parsing method at line 118:
```java
String[] columns = schemaLine.split(",");
```

This uses a hardcoded comma to split the schema line. For TSV files, the schema line would also use tabs, so this would fail to split columns correctly. It also doesn't account for any user-configured delimiter.

**Impact:** Even if the data delimiter were fixed for TSV, schema discovery would still break because it always splits on comma.

**Fix:** Use the configured delimiter character for schema line splitting.

---

## 3. Jackson CSV Capabilities (What the Library Supports)

Jackson CSV 2.15.0's `CsvSchema` class provides these configuration methods, all of which are currently unused or hardcoded:

### Delimiter and Separator Configuration
| Method | Default | Current Usage |
|--------|---------|---------------|
| `withColumnSeparator(char)` | `,` | Hardcoded to `,` (line 232) |
| `withQuoteChar(char)` | `"` | Hardcoded to `"` (line 233) |
| `withoutQuoteChar()` | N/A | Not used |
| `withEscapeChar(char)` | `-1` (none) | **Bug**: set to `\\` (line 234) |
| `withoutEscapeChar()` | N/A | Not used |
| `withNullValue(String)` | empty string | Set to `""` (line 235) |
| `withLineSeparator(String)` | `\n` | Not used (default) |
| `withArrayElementSeparator(String)` | `;` | Not used |

### Header and Schema Configuration
| Method | Default | Current Usage |
|--------|---------|---------------|
| `withUseHeader(boolean)` | false | Not used -- custom schema parsing instead |
| `withSkipFirstDataRow(boolean)` | false | Not used |
| `withAllowComments(boolean)` | false | Not used -- manual `//` comment handling |
| `withComments(char)` | N/A | Not used |
| `withStrictHeaders(boolean)` | false | Not used |
| `withColumnReordering(boolean)` | false | Not used |

### CsvParser.Feature Flags (Available in Jackson)
| Feature | Default | Current Usage |
|---------|---------|---------------|
| `TRIM_SPACES` | false | **Enabled** (line 177) |
| `SKIP_EMPTY_LINES` | false | **Enabled** (line 178) |
| `WRAP_AS_ARRAY` | false | **Enabled** (line 179) |
| `IGNORE_TRAILING_UNMAPPABLE` | false | Not enabled |
| `ALLOW_TRAILING_COMMA` | true | Default (enabled) |
| `FAIL_ON_MISSING_COLUMNS` | false | Not enabled |
| `INSERT_NULLS_FOR_MISSING_COLUMNS` | false | Not enabled |

### Multi-line Quoted Values

Jackson CSV handles quoted values containing newlines **by default**. When `withQuoteChar('"')` is set (which it is), a value like `"line1\nline2"` will be parsed correctly as a single field spanning two lines. No special configuration is needed.

The current implementation will handle multi-line values correctly because Jackson is doing the line parsing, not `BufferedReader.readLine()` -- after the schema line is consumed via `readLine()`, the remaining data is fed to Jackson via the `reader` object directly (line 237).

---

## 4. Schema and Type Inference

### Current CSV Type System (lines 140-151)

The CSV reader requires an **explicit typed header** as the first line in the format `name:type,name:type,...`. It supports:

```java
case "INTEGER", "INT", "I" -> DataType.INTEGER;
case "LONG", "L" -> DataType.LONG;
case "DOUBLE", "D" -> DataType.DOUBLE;
case "KEYWORD", "K", "STRING", "S" -> DataType.KEYWORD;
case "TEXT", "TXT" -> DataType.TEXT;
case "BOOLEAN", "BOOL" -> DataType.BOOLEAN;
case "DATETIME", "DATE", "DT" -> DataType.DATETIME;
case "NULL", "N" -> DataType.NULL;
```

**No type inference exists.** Every column must have an explicit type in the header. Standard CSV files with plain headers (`id,name,age`) will fail with a `ParsingException` at line 125 because `split(":")` won't find a type component.

### NDJSON Type Inference for Comparison

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-ndjson/src/main/java/org/elasticsearch/xpack/esql/datasource/ndjson/NdJsonSchemaInferrer.java`

NDJSON has a sophisticated multi-line inference system:
- Samples first N lines (default 100, line 51)
- Tracks multiple types per field via `EnumSet<DataType>` (line 185)
- Resolves type conflicts with widening: DATETIME > KEYWORD > DOUBLE > LONG > INTEGER (lines 211-226)
- Handles nested objects via dot notation (line 153)
- Detects arrays as multi-value fields (line 116)
- Supports nullability tracking (line 159)
- Attempts ISO-8601 datetime parsing on strings (lines 124-129)

### Gap: CSV Has No Type Inference

CSV would need a type inference pass for standard CSV files (those with plain headers, no `:type` suffix). This would require:

1. Reading the first line to determine if it's `name:type` format (current) or plain `name` format
2. If plain names, sampling N data rows and applying inference logic similar to NDJSON
3. Type precedence rules: try numeric (INTEGER -> LONG -> DOUBLE), then boolean, then datetime (ISO-8601), then fall back to KEYWORD

The NDJSON inferrer is tightly coupled to JSON token types (`VALUE_NUMBER_INT`, `VALUE_STRING`, etc.) so it cannot be directly reused, but the widening logic in `FieldInfo.resolveType()` (lines 201-227) is the right pattern.

---

## 5. WITH Clause Configuration Flow

### Current Architecture

The config flow from query to format reader has a **critical gap**:

1. **Parser** creates `UnresolvedExternalRelation` with `Map<String, Expression> params` from WITH clause
   - File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnresolvedExternalRelation.java` (line 33)

2. **ExternalSourceResolver** converts params to `Map<String, Object> config` via `paramsToConfigMap()` (line 286)
   - Config is stored in `ExternalSourceMetadata.config()` and flows into `ExternalRelation`

3. **ExternalRelation.toPhysicalExec()** passes config to `ExternalSourceExec` (line 121)
   - File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java`

4. **FileSourceFactory.operatorFactory()** receives config via `context.config()` (line 121)
   - File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java`
   - Uses config only for **StorageProvider** creation (line 125): `storageRegistry.createProvider(path.scheme(), settings, config)`
   - **Config is NOT passed to FormatReader** -- only `formatRegistry.byExtension(path.objectName())` is called (line 130)

5. **FormatReader.read()** signature: `read(StorageObject, List<String> projectedColumns, int batchSize)`
   - File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java` (line 58)
   - **No config parameter.** The format reader has no way to receive WITH clause configuration.

### The Gap

The `FormatReader` SPI does not accept query-level configuration. This means:

```sql
EXTERNAL "s3://bucket/data.csv" WITH delimiter="\t", quote_char="'", has_header=true
```

The `delimiter`, `quote_char`, and `has_header` values would be in `config` at the `FileSourceFactory` level but would never reach `CsvFormatReader`. The `FormatReaderFactory.create(Settings, BlockFactory)` (line 28 in FormatReaderFactory.java) also only receives cluster-level `Settings`, not query-level config.

### Required SPI Changes

To support format-level configuration, one of these approaches is needed:

**Option A: Add config to `FormatReader.read()` signature**
```java
CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize, Map<String, Object> config);
```
This is a breaking SPI change but the cleanest solution.

**Option B: Create format reader per-query with config**
Change `FormatReaderFactory.create()` to accept query config:
```java
FormatReader create(Settings settings, BlockFactory blockFactory, Map<String, Object> queryConfig);
```
This would mean format readers are created per-query rather than being singletons, which changes the lazy-loading model in `FormatReaderRegistry`.

**Option C: Pass config through the `FileSourceFactory` chain**
The factory already has the config. It could create a config-aware wrapper or pass the config when constructing the operator factory. This avoids SPI changes but leaks format concerns into the factory.

**Recommendation:** Option A is cleanest. The `read()` method should accept an optional config map. The default implementation can ignore it. This is backward compatible if done as a default method.

---

## 6. TSV: Separate Format vs. Configurable CSV

### Current Registration

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/src/main/java/org/elasticsearch/xpack/esql/datasource/csv/CsvDataSourcePlugin.java`

The plugin registers:
- Format name: `"csv"` (single format, line 42)
- Extensions: `.csv` and `.tsv` (line 48)
- Single factory: `(s, blockFactory) -> new CsvFormatReader(blockFactory)` (line 52)

The `FormatReaderRegistry` maps both `.csv` and `.tsv` to the same `CsvFormatReader` instance (via lazy supplier in lines 54-68 of FormatReaderRegistry.java).

### Analysis: Configurable CSV is Better Than Separate Format

**Arguments for configurable CSV (recommended):**
- TSV is literally CSV with a different delimiter -- the parsing logic is identical
- Jackson CSV already supports arbitrary delimiters via `withColumnSeparator(char)`
- One reader, one test suite, one maintenance burden
- Other variants (pipe-separated, semicolon-separated) come for free
- DuckDB, Trino, Spark all treat TSV as a CSV dialect, not a separate format

**Arguments against separate TSV format reader:**
- Code duplication (identical logic except for one character)
- Extension mapping in `FormatReaderRegistry` already supports multiple extensions per format
- No type system or parsing differences -- only the separator changes

### Implementation Approach

The `CsvFormatReader` should detect the format from the file extension at read time:
```java
@Override
public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
    char delimiter = inferDelimiter(object.path());
    // ... pass to CsvBatchIterator
}

private char inferDelimiter(StoragePath path) {
    String name = path.objectName().toLowerCase(Locale.ROOT);
    if (name.endsWith(".tsv")) return '\t';
    return ',';  // default
}
```

WITH clause overrides would take priority over extension-based inference.

---

## 7. What Configuration Options Are Needed

Based on Jackson CSV capabilities and common CSV dialects:

### Essential (for production quality)

| Config Key | Type | Default | Purpose |
|-----------|------|---------|---------|
| `delimiter` | char | `,` (or `\t` for .tsv) | Column separator |
| `quote_char` | char | `"` | Quoting character |
| `escape_char` | char | none (-1) | Escape character (none by default, per RFC 4180) |
| `has_header` | boolean | true | Whether first row is header (names only, no types) |
| `skip_rows` | int | 0 | Number of rows to skip before header/data |
| `null_value` | string | `""` | String representation of null |

### Nice-to-Have

| Config Key | Type | Default | Purpose |
|-----------|------|---------|---------|
| `encoding` | string | `UTF-8` | Character encoding |
| `comment_char` | char | none | Comment line prefix (currently hardcoded to `//`) |
| `trim_spaces` | boolean | true | Trim whitespace around values |
| `max_columns` | int | 1024 | Safety limit on column count |
| `sample_rows` | int | 100 | Rows to sample for type inference |

### Example Query Syntax

```sql
-- Standard CSV with header
EXTERNAL "s3://bucket/data.csv"

-- TSV (auto-detected from extension)
EXTERNAL "s3://bucket/data.tsv"

-- Semicolon-delimited (European CSV)
EXTERNAL "s3://bucket/data.csv" WITH delimiter=";"

-- Pipe-delimited with single-quote quoting
EXTERNAL "s3://bucket/data.csv" WITH delimiter="|", quote_char="'"

-- CSV without typed header (needs type inference)
EXTERNAL "s3://bucket/plain.csv" WITH has_header=true

-- Skip metadata rows
EXTERNAL "s3://bucket/export.csv" WITH skip_rows=3
```

---

## 8. Type Inference Design

### Two Header Modes

1. **Typed header** (current): `name:type,name:type,...` -- ES|QL specific, explicit types
2. **Plain header** (new): `name,name,...` -- standard CSV, requires type inference

Detection: if any column header contains `:`, assume typed header. Otherwise, plain header with inference.

### Inference Algorithm

For plain headers, sample the first N data rows and infer types per column:

```
For each column:
  Track: hasInteger, hasLong, hasDouble, hasBoolean, hasDatetime, hasKeyword, hasNull

  For each sample value:
    if empty/null -> hasNull = true; continue
    try Integer.parseInt -> hasInteger = true
    try Long.parseLong -> hasLong = true
    try Double.parseDouble -> hasDouble = true
    try Booleans.parseBoolean (true/false) -> hasBoolean = true
    try Instant.parse (ISO-8601) -> hasDatetime = true
    else -> hasKeyword = true

  Resolve:
    if hasKeyword -> KEYWORD (anything mixed with strings = string)
    if hasDatetime && !hasNumeric -> DATETIME
    if hasDouble -> DOUBLE
    if hasLong -> LONG
    if hasInteger -> INTEGER
    if hasBoolean -> BOOLEAN
    else -> KEYWORD (fallback)
```

This mirrors NDJSON's `FieldInfo.resolveType()` widening logic but operates on string values rather than JSON tokens.

**Estimated effort:** ~1 day for inference, ~0.5 day for tests.

---

## 9. Work Estimate

### Bug Fixes (Pre-requisite, should be MVP TP)

| Item | Effort | Notes |
|------|--------|-------|
| Fix `.withEscapeChar('\\')` bug | 15 min | Remove or use `.withoutEscapeChar()` |
| Fix hardcoded delimiter for TSV | 1h | Detect from extension, pass through iterator |
| Fix `schemaLine.split(",")` | 30 min | Use configured delimiter |
| Tests for all three fixes | 2h | TSV test, escape test, delimiter test |
| **Subtotal** | **~0.5 day** | |

### Configurable Delimiters and Quoting (Post-MVP)

| Item | Effort | Notes |
|------|--------|-------|
| SPI change: add config to `FormatReader.read()` | 2h | Default method, backward compatible |
| Wire config through `FileSourceFactory` | 2h | Pass config from context to format reader |
| `CsvFormatReader` config parsing | 3h | Parse delimiter, quote_char, escape_char, etc. |
| Extension-based delimiter detection | 1h | `.tsv` -> tab, `.csv` -> comma |
| Tests | 3h | Various delimiter/quoting combinations |
| **Subtotal** | **~1.5 days** | |

### Type Inference (Post-MVP)

| Item | Effort | Notes |
|------|--------|-------|
| Plain header detection | 1h | Check for `:` in header columns |
| Type inference engine | 4h | Sample N rows, per-column type tracking |
| Widening/resolution logic | 2h | Similar to NDJSON FieldInfo.resolveType() |
| `has_header=false` support (headerless CSV) | 2h | Auto-generate column names (col0, col1...) |
| Tests | 3h | Various type combinations, edge cases |
| **Subtotal** | **~1.5 days** | |

### Additional Improvements (Post-MVP)

| Item | Effort | Notes |
|------|--------|-------|
| Comment char configuration | 1h | Replace hardcoded `//` with configurable |
| `skip_rows` support | 1h | Skip N lines before header |
| Error handling improvements | 2h | Better error messages for malformed rows |
| `INSERT_NULLS_FOR_MISSING_COLUMNS` | 30 min | Enable Jackson feature for ragged CSVs |
| **Subtotal** | **~0.5 day** | |

### Total Effort

| Category | Effort |
|----------|--------|
| Bug fixes (MVP TP priority) | 0.5 day |
| Configurable delimiters + quoting | 1.5 days |
| Type inference | 1.5 days |
| Additional improvements | 0.5 day |
| **Total** | **~4 days** |

---

## 10. Key Files Reference

| File | Purpose |
|------|---------|
| `x-pack/plugin/esql-datasource-csv/src/main/java/org/elasticsearch/xpack/esql/datasource/csv/CsvFormatReader.java` | Main CSV reader -- all bugs and improvements here |
| `x-pack/plugin/esql-datasource-csv/src/main/java/org/elasticsearch/xpack/esql/datasource/csv/CsvDataSourcePlugin.java` | Plugin registration -- format name, extensions |
| `x-pack/plugin/esql-datasource-csv/src/test/java/org/elasticsearch/xpack/esql/datasource/csv/CsvFormatReaderTests.java` | Existing test suite |
| `x-pack/plugin/esql-datasource-csv/build.gradle` | Jackson CSV dependency |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java` | SPI interface -- needs config parameter |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReaderFactory.java` | Factory interface -- may need config parameter |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FormatReaderRegistry.java` | Extension -> format reader mapping |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java` | Where config is available but not forwarded to format reader |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java` | WITH clause params -> config map conversion |
| `x-pack/plugin/esql-datasource-ndjson/src/main/java/org/elasticsearch/xpack/esql/datasource/ndjson/NdJsonSchemaInferrer.java` | Reference implementation for type inference patterns |

---

## 11. Summary

The CSV reader is functional but has three confirmed bugs and lacks production-quality configuration. The Jackson CSV library already supports everything needed (configurable delimiters, quoting modes, escape handling, comments, header modes) -- the code just doesn't expose these capabilities.

**Priority ordering:**
1. **Fix the three bugs** (0.5 day) -- these should be addressed before MVP TP since they affect correctness
2. **Configurable delimiters** (1.5 days) -- enables TSV, European CSV (`;`), pipe-delimited
3. **Type inference** (1.5 days) -- enables standard CSV files without ES|QL-specific typed headers
4. **Additional config** (0.5 day) -- skip_rows, comment char, encoding

The main architectural challenge is the SPI gap: `FormatReader.read()` does not accept query-level configuration. This requires a backward-compatible SPI change (add a default method with config parameter) that should be coordinated with the broader SPI unification proposal.
