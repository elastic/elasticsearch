# Deep Dive: CSV/TSV RFC 4180 Compliance

## Summary

The `CsvFormatReader` uses Jackson's `jackson-dataformat-csv` library (version 2.15.0) for parsing but explicitly configures a **backslash escape character** (`withEscapeChar('\\')`) which violates RFC 4180. Additionally, the `.tsv` extension is registered but **the delimiter is hardcoded to comma** -- TSV files will be parsed as comma-delimited, which is simply wrong. Schema parsing also hardcodes comma as the column separator, which breaks for TSV. There are no tests for TSV parsing, quoted fields, or RFC 4180 double-quote escaping.

---

## 1. CsvFormatReader: Full Class Analysis

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/src/main/java/org/elasticsearch/xpack/esql/datasource/csv/CsvFormatReader.java`

### Structure

The class implements `FormatReader` (the SPI interface) and contains:

- **Constructor** (line 64): Takes only `BlockFactory`. No delimiter parameter.
- **`metadata()`** (line 69): Opens a stream, reads the first non-comment line as the schema.
- **`read()`** (line 95): Creates a `CsvBatchIterator` for streaming page reads.
- **`formatName()`** (line 104): Returns `"csv"`.
- **`fileExtensions()`** (line 108): Returns `List.of(".csv", ".tsv")` -- claims to handle TSV.
- **`parseSchema()`** (line 117): Splits schema line by comma, then each column by colon.

### Inner Class: CsvBatchIterator (line 158)

The core parsing logic. Uses Jackson CSV with this configuration (lines 176-179, 231-235):

```java
// Constructor setup
this.csvMapper = new CsvMapper();
this.csvMapper.enable(CsvParser.Feature.TRIM_SPACES);
this.csvMapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
this.csvMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);

// Schema setup in readNextBatch()
CsvSchema csvSchema = CsvSchema.emptySchema()
    .withColumnSeparator(',')     // HARDCODED comma
    .withQuoteChar('"')           // Correct per RFC 4180
    .withEscapeChar('\\')         // WRONG: violates RFC 4180
    .withNullValue("");           // Empty string = null
```

### Library Used

Jackson `jackson-dataformat-csv` at version `${versions.jackson}` which resolves to **2.15.0** (from `build-tools-internal/version.properties` line 9).

**Dependency declaration** in `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/build.gradle` (line 31):
```groovy
implementation "com.fasterxml.jackson.dataformat:jackson-dataformat-csv:${versions.jackson}"
```

---

## 2. Delimiter Handling

### Hardcoded to Comma

The delimiter is hardcoded at two levels:

1. **Jackson CSV schema** (line 232): `.withColumnSeparator(',')` -- always comma, never tab.
2. **Schema line parsing** (line 118): `schemaLine.split(",")` -- always splits the header by comma.

**There is no mechanism to specify tab or any other delimiter.** The `CsvFormatReader` constructor takes only `BlockFactory`. The `FormatReaderFactory` interface (line 28 of `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReaderFactory.java`) provides `Settings` and `BlockFactory` but no format-specific parameters.

### TSV Registration Claims

Despite not supporting tab delimiters, TSV is registered at multiple levels:

- **`CsvFormatReader.fileExtensions()`** (line 109): Returns `List.of(".csv", ".tsv")`
- **`CsvDataSourcePlugin.supportedExtensions()`** (line 47 of `CsvDataSourcePlugin.java`): Returns `Set.of(".csv", ".tsv")`
- **`FileSplitProvider.isSplittableFormat()`** (line 112 of `FileSplitProvider.java`): Lists `.tsv` as splittable
- **`FormatReaderRegistry`** (lines 55-62): Auto-registers extension mappings from `fileExtensions()`, so `.tsv` maps to the comma-delimited CSV reader.

**Net result**: A `.tsv` file will be found by the file extension lookup, but parsed with comma as the delimiter. The tab characters in the data will be treated as part of field values, producing one giant column per row.

### Search Results

A search for `delimiter`, `separator`, `\t`, and `TSV` across all ESQL Java source found:

- No configurable delimiter anywhere in `CsvFormatReader`
- No tab character handling in the data source CSV code
- The only tab-delimiter awareness is in the **response formatter** `TextFormat.TSV` (a completely separate code path for formatting query *output*, not reading input files)

---

## 3. Quoting/Escaping

### What RFC 4180 Requires

RFC 4180 Section 2, Rule 7:
> If double-quotes are used to enclose fields, then a double-quote appearing inside a field must be escaped by preceding it with another double-quote. Example: `"aaa","b""bb","ccc"`

Key point: **RFC 4180 does NOT use backslash escaping.** The only escape mechanism is doubling the quote character: `""` inside a quoted field means a literal `"`.

### What The Code Does

Line 234 of `CsvFormatReader.java`:
```java
.withEscapeChar('\\')
```

This tells Jackson to interpret `\"` as an escaped quote character. This is **not RFC 4180 compliant**. In RFC 4180, the string `\"` is not a special sequence -- the backslash would be literal data.

### Jackson's Default Behavior

Jackson's `CsvSchema` **defaults to no escape character** (escape char = -1, meaning disabled). By default, Jackson follows RFC 4180 and handles double-quote escaping (`""`) natively as part of the quoting mechanism. The explicit `.withEscapeChar('\\')` on line 234 **overrides** this correct default and introduces non-standard backslash escaping.

### Concrete Example of the Bug

Given this RFC 4180 compliant CSV:
```
name:keyword,description:keyword
"Product A","Contains ""special"" characters"
```

- **RFC 4180 correct parse**: `description` = `Contains "special" characters`
- **Current code behavior**: Jackson sees `""` as double-quote escape (OK), but also interprets `\"` as escape. If the CSV contains `"path C:\foo"`, the backslash before `f` would be treated as an escape character, consuming the `f` and producing `path C:oo` instead of `path C:\foo`.

### The TextFormat Response Formatter (Separate Code)

For comparison, the ESQL **response formatter** at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/formatter/TextFormat.java` correctly implements RFC 4180 for CSV output (lines 151-172):

```java
// CSV.writeEscaped() -- correctly uses double-quote escaping per RFC 4180
void writeEscaped(String value, Character delimiter, Writer writer) throws IOException {
    // ... scans for " \n \r or delimiter
    // When found, wraps in quotes and doubles any internal quotes
}
```

And TSV output (lines 237-254) correctly uses backslash escaping for `\n` and `\t`, which is the TSV convention.

---

## 4. Line Endings

### How Line Endings Are Handled

The code reads lines via `BufferedReader.readLine()` (lines 82, 97, 221 of `CsvFormatReader.java`):

```java
BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
```

**Schema reading** (line 82): Uses `reader.readLine()` which handles CR, LF, and CRLF transparently -- this works correctly.

**Data reading** (line 237): After schema is parsed, the remaining `BufferedReader` is passed directly to the Jackson CSV parser:
```java
csvIterator = csvMapper.readerFor(List.class).with(csvSchema).readValues(reader);
```

Jackson's CSV parser handles line endings internally. With the current configuration, Jackson handles both LF and CRLF. However, there is a subtle issue: `BufferedReader.readLine()` consumes some bytes for the schema line, then the remaining bytes (with their raw line endings) are handed to Jackson. Since Jackson reads from the `BufferedReader` (not the raw `InputStream`), the `BufferedReader`'s own buffering could interfere. In practice this works because `BufferedReader` presents a stream of characters to Jackson, not individual lines.

### RFC 4180 Requirement

RFC 4180 specifies CRLF as the line ending. Jackson's parser accepts both LF and CRLF, so the reader correctly handles both real-world variants.

**Verdict**: Line ending handling is adequate. No changes needed.

---

## 5. Schema Inference from CSV

### Current Schema Format

The schema is **not inferred** -- it must be explicitly declared on the first non-comment line using a custom format:

```
column_name:type,column_name:type,...
```

Example from the test fixture (line 1 of `employees.csv`):
```
emp_no:integer,first_name:keyword,last_name:keyword,birth_date:date,...
```

### Schema Parsing Code

`parseSchema()` at line 117 of `CsvFormatReader.java`:

1. Splits the line by comma: `schemaLine.split(",")`
2. For each column, splits by colon: `trimmedColumn.split(":")`
3. Maps the type string to `DataType` via `parseDataType()` (line 140)

### Supported Types (lines 141-151)

| Type String(s)                     | DataType        |
|------------------------------------|-----------------|
| `INTEGER`, `INT`, `I`             | `DataType.INTEGER` |
| `LONG`, `L`                       | `DataType.LONG`    |
| `DOUBLE`, `D`                     | `DataType.DOUBLE`  |
| `KEYWORD`, `K`, `STRING`, `S`     | `DataType.KEYWORD` |
| `TEXT`, `TXT`                      | `DataType.TEXT`    |
| `BOOLEAN`, `BOOL`                  | `DataType.BOOLEAN` |
| `DATETIME`, `DATE`, `DT`          | `DataType.DATETIME`|
| `NULL`, `N`                        | `DataType.NULL`    |

### No Automatic Type Detection

There is no automatic type detection from data values. The schema MUST be in the first line. There is no support for standard CSV headers (just column names without types) -- the colon-type suffix is required.

### Schema for TSV is Broken

Since `parseSchema()` uses `schemaLine.split(",")`, a TSV schema line like:
```
id:long\tname:keyword\tscore:double
```
would be parsed as a single column named `id:long\tname:keyword\tscore:double` and fail the colon split check.

---

## 6. Plugin Registration

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/src/main/java/org/elasticsearch/xpack/esql/datasource/csv/CsvDataSourcePlugin.java`

```java
public class CsvDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<String> supportedFormats() {
        return Set.of("csv");                          // line 42
    }

    @Override
    public Set<String> supportedExtensions() {
        return Set.of(".csv", ".tsv");                 // line 47
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("csv", (s, blockFactory) -> new CsvFormatReader(blockFactory));  // line 52
    }
}
```

### Registration Flow

1. `DataSourceModule` discovers `CsvDataSourcePlugin` via plugin loading
2. Calls `formatReaders(settings)` to get the factory map
3. Registers each factory in `FormatReaderRegistry` via `registerLazy()`
4. When the reader is first accessed, `fileExtensions()` is called and `.csv` + `.tsv` are mapped to the same reader instance

The factory at line 52 creates `CsvFormatReader(blockFactory)` with no delimiter parameter. There is only one reader for both extensions.

---

## 7. Jackson CSV Library

### Dependency

- **Group**: `com.fasterxml.jackson.dataformat`
- **Artifact**: `jackson-dataformat-csv`
- **Version**: 2.15.0 (from `build-tools-internal/version.properties`, line 9: `jackson = 2.15.0`)
- **Declared in**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/build.gradle`, line 31
- **Checksum verified in**: `/Users/oleglvovitch/github/root/elasticsearch/gradle/verification-metadata.xml`, lines 420-421 (also 2.17.2 entry at lines 425-426, possibly for a different module)

### No Custom Parser

There is no custom CSV parser. The code relies entirely on Jackson CSV (`CsvMapper`, `CsvParser`, `CsvSchema`) for data row parsing. Only the schema line (first line) is parsed with a custom `String.split()` approach.

### Jackson CSV Classes Used (imports at lines 10-12)

```java
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
```

---

## 8. What Needs to Change for RFC 4180 Compliance and TSV Support

### Bug 1: Remove Backslash Escape Character (RFC 4180 violation)

**File**: `CsvFormatReader.java`, line 234
**Current**: `.withEscapeChar('\\')`
**Fix**: Remove this line entirely, or use `.withEscapeChar(CsvSchema.DEFAULT_ESCAPE_CHAR)` which is -1 (disabled).

Jackson's default behavior (no escape char) correctly implements RFC 4180 double-quote escaping (`""` inside quoted fields). The explicit backslash escape causes:
- `\"` to be interpreted as an escaped quote (non-standard)
- Literal backslashes before certain characters to be consumed (data corruption)

### Bug 2: Hardcoded Comma Delimiter (TSV completely broken)

**File**: `CsvFormatReader.java`, line 232
**Current**: `.withColumnSeparator(',')`
**Fix**: Make the delimiter configurable. Two approaches:

**Option A**: Constructor parameter
```java
public CsvFormatReader(BlockFactory blockFactory, char delimiter) {
    this.blockFactory = blockFactory;
    this.delimiter = delimiter;
}
```

Then register two format readers in `CsvDataSourcePlugin`:
```java
return Map.of(
    "csv", (s, blockFactory) -> new CsvFormatReader(blockFactory, ','),
    "tsv", (s, blockFactory) -> new CsvFormatReader(blockFactory, '\t')
);
```

**Option B**: Detect from file extension at read time. Less clean since `FormatReader.read()` receives a `StorageObject` which has a `path()` method.

### Bug 3: Schema Line Uses Hardcoded Comma Split (TSV schema broken)

**File**: `CsvFormatReader.java`, line 118
**Current**: `String[] columns = schemaLine.split(",");`
**Fix**: Use the same delimiter that's used for data parsing:
```java
String[] columns = schemaLine.split(String.valueOf(delimiter));
```

### Bug 4: Plugin Registration Needs TSV as Separate Format

**File**: `CsvDataSourcePlugin.java`
**Current**: Only one format reader ("csv") handles both `.csv` and `.tsv`
**Fix**: Register "tsv" as a separate format:
```java
@Override
public Set<String> supportedFormats() {
    return Set.of("csv", "tsv");
}

@Override
public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
    return Map.of(
        "csv", (s, blockFactory) -> new CsvFormatReader(blockFactory, ','),
        "tsv", (s, blockFactory) -> new CsvFormatReader(blockFactory, '\t')
    );
}
```

And update `CsvFormatReader.fileExtensions()` to return only the relevant extension based on its configured delimiter.

### Bug 5: `TRIM_SPACES` Feature May Be Unwanted

**File**: `CsvFormatReader.java`, line 177
**Current**: `this.csvMapper.enable(CsvParser.Feature.TRIM_SPACES);`

RFC 4180 does not mandate trimming spaces. Trimming can silently destroy data -- a field value of `"  hello  "` would become `"hello"`. For strict RFC 4180 compliance this should be removed, though it may be intentionally enabled for the typed-schema use case where surrounding spaces are noise.

### Bug 6: No Tests for Quoting, Escaping, or TSV

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/src/test/java/org/elasticsearch/xpack/esql/datasource/csv/CsvFormatReaderTests.java`

The test file has **no tests** for:
- Quoted fields containing commas (e.g., `"hello, world"`)
- Quoted fields containing newlines
- RFC 4180 double-quote escaping (`""`)
- Backslash in data
- Tab-separated data
- CRLF line endings
- Files with `.tsv` extension

### Summary of Required Changes

| # | Severity | File | Line | Issue | Fix |
|---|----------|------|------|-------|-----|
| 1 | **Critical** | `CsvFormatReader.java` | 234 | Backslash escape char violates RFC 4180 | Remove `.withEscapeChar('\\')` |
| 2 | **Critical** | `CsvFormatReader.java` | 232 | Delimiter hardcoded to comma; TSV broken | Make delimiter configurable |
| 3 | **Critical** | `CsvFormatReader.java` | 118 | Schema parsing hardcodes comma split | Use configured delimiter |
| 4 | **High** | `CsvDataSourcePlugin.java` | 41-52 | Only one format reader for both CSV/TSV | Register separate "tsv" format |
| 5 | **Medium** | `CsvFormatReader.java` | 109 | `fileExtensions()` returns both extensions | Return only matching extension |
| 6 | **Low** | `CsvFormatReader.java` | 177 | `TRIM_SPACES` may destroy data | Consider removing for strict compliance |
| 7 | **High** | `CsvFormatReaderTests.java` | -- | No tests for quoting/escaping/TSV | Add comprehensive test coverage |

### Comparison with Response Formatter

The ESQL response formatter (`TextFormat` enum at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/formatter/TextFormat.java`) correctly implements both CSV (lines 95-207) and TSV (lines 209-267) as separate enum values with:
- CSV: comma delimiter, CRLF line endings, double-quote escaping per RFC 4180
- TSV: tab delimiter, LF line endings, backslash escaping for `\n` and `\t`

The data source reader should follow the same pattern: separate handling for CSV and TSV with correct escaping conventions for each.
