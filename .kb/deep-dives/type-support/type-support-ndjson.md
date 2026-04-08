# NDJSON Type Support in ES|QL External Data Sources

NDJSON type handling relies on Jackson Core's streaming API (`JsonParser`) for both schema inference and value decoding. Types are inferred from the first 100 lines (configurable via `maxLines`) by examining `JsonToken` values and mapping them to ESQL `DataType`. NDJSON is the only external source format that supports multi-values (arrays) — Parquet, CSV, and ORC all produce single-valued blocks.

**Source files** (all in `x-pack/plugin/esql-datasource-ndjson/.../ndjson/`):
- `NdJsonSchemaInferrer.java` — schema inference from first N lines
- `NdJsonPageDecoder.java` — JSON-to-Block decoding with tree of `BlockDecoder` nodes
- `NdJsonPageIterator.java` — page-level iteration, split handling
- `NdJsonFormatReader.java` — `SegmentableFormatReader` implementation, entry point
- `NdJsonUtils.java` — shared `JsonFactory`, line recovery utilities

The complete per-type breakdown is in [Appendix](#appendix-per-type-breakdown). The summary and issue tables below focus on what's broken.

## Summary

| Status | Count | Description |
|---|---|---|
| 🟢 Fully works | 19 | All 5 scalar types (string, int, long, double, boolean), null, ISO datetime, numeric widening (int→long, int→double), all homogeneous arrays, field presence/absence, null→value promotion, empty string, double overflow |
| 🔴 Wrong data / crash | 7 | Block misalignment on type mismatch, nested object NPE, array-of-objects NPE, nulls-in-arrays corruption, BigInteger crash, int+bool array crash, flat-then-nested conflict |
| 🟡 Partial | 5 | Date-only strings as KEYWORD, int+string widening to KEYWORD, empty arrays excluded from schema, int+string array stringified, nested arrays flattened wrong |

### MVP

| Status | Type | Prevalence | Description | Issue |
|---|---|---|---|---|
| 🔴 | Type mismatch in data (any column) | Very common. Real-world NDJSON is messy — fields change type across rows. | Schema mismatch silently corrupts data | [NDJSON-1](#ndjson-1-schema-mismatch-silently-corrupts-data) |
| 🔴 | Nested objects (`{"a": {"b": 1}}`) | Extremely common. Nested JSON is the norm, not the exception. | Nested objects and arrays of objects crash | [NDJSON-2](#ndjson-2-nested-objects-and-arrays-of-objects-crash) |
| 🔴 | Arrays of objects (`[{"k": "v"}]`) | Very common. Log events, API responses, database exports. | Nested objects and arrays of objects crash | [NDJSON-2](#ndjson-2-nested-objects-and-arrays-of-objects-crash) |
| 🔴 | Arrays with null elements (`["a", null, "b"]`) | Common. Sparse data, optional values in arrays. | Nulls inside arrays produce wrong row counts | [NDJSON-3](#ndjson-3-nulls-inside-arrays-produce-wrong-row-counts) |
| 🔴 | BigInteger values (> Long.MAX_VALUE) | Rare. uint64 IDs, crypto hashes, snowflake IDs at upper range. | BigInteger overflow crashes query | [NDJSON-4](#ndjson-4-biginteger-overflow-crashes-query) |
| 🟡 | Date-only strings (`"2024-01-15"`) | Common. Many datasets use date-only formats. | Date-only strings not detected as datetime | [NDJSON-5](#ndjson-5-date-only-strings-not-detected-as-datetime) |

### Post-MVP

| Status | Type | Prevalence | Description | Issue |
|---|---|---|---|---|
| 🟡 | Nested arrays (`[[1,2],[3,4]]`) | Rare. Matrix data, batched payloads. | Nested arrays produce wrong data | [NDJSON-6](#ndjson-6-nested-arrays-produce-wrong-data) |
| 🟡 | NULL-typed column then array | Very rare. Only if all 100 sample rows are null for that field. | All-null columns crash when data contains arrays | [NDJSON-7](#ndjson-7-all-null-columns-crash-when-data-contains-arrays) |

---

## Issue Details

### NDJSON-1: Schema mismatch silently corrupts data

**Estimate:** 0.5w
**Priority:** MVP — 🔴 silently wrong data or crash
**Affected patterns:** Any type mismatch between inferred schema and actual data (e.g., string in DATETIME column, boolean in INTEGER column)

#### Prevalence

Very common. Real-world NDJSON is messy — fields change type across rows, upstream producers evolve schemas, and log pipelines mix sources. Any user querying NDJSON files where even one row has a type mismatch will hit this. The block misalignment is particularly dangerous because it silently corrupts all subsequent rows in the page — the user sees plausible-looking but wrong data with no error.

#### User impact

When a field's actual value type differs from what was inferred during schema sampling (e.g., a string appears in a column inferred as DATETIME, or a boolean in an INTEGER column), the query crashes in debug mode (assertions on) with `AssertionError` at `Page.java:92` because blocks have different position counts. In production (assertions off): the query succeeds but every row after the mismatched one has wrong values — column A's row N corresponds to column B's row N+1. A log analysis query silently joins the wrong fields together. There is no error or warning.

**Example:** File has `{"x": "2024-01-01T00:00:00Z", "y": 1}` then `{"x": "not-a-date", "y": 2}`. Schema infers `x` as DATETIME. Row 2's `x` triggers `unexpectedValue()`. Result: `x` block has 1 position, `y` block has 2 positions. Row 2's `y=2` has no corresponding `x` value.

**Testing gap:** `NdJsonSchemaInferrerTests.testInferSchemaForMixedTypeFields` tests inference of heterogeneous columns, but no test exercises the decode path with mixed-type rows. The `employees.ndjson` test data has consistent types across all rows — no field ever changes type between rows.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReproNdjson1Test extends ESTestCase {
    public void testBlockMisalignmentFromUnexpectedValue() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"x": "2024-01-01T00:00:00Z", "y": 1}
            {"x": "not-a-date", "y": 2}
            """;

        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("x", "y"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertEquals(page.getBlock(0).getPositionCount(), page.getBlock(1).getPositionCount());
            assertEquals(2, page.getPositionCount());
        }
    }
}
```

**Fails with:** `AssertionError: expected positionCount=1` at `Page.java:92` — x block has 1 position, y block has 2.

#### Why it happens

When a JSON value doesn't match the inferred type, `unexpectedValue()` (`NdJsonPageDecoder.java:306-314`) is called. This method logs a warning and calls `parser.skipChildren()`, but does **not** append any value (or null) to the block builder. However, `blockTracker.set(blockIdx)` was already called unconditionally at line 235, so the null-fill loop at lines 118-122 skips this block. The block ends up with one fewer position than other blocks.

```java
// NdJsonPageDecoder.java:235 — unconditional tracker set
blockTracker.set(blockIdx);
// ... later, unexpectedValue() does NOT append to block
```

#### Fix

Either (a) move `blockTracker.set(blockIdx)` to after the value is successfully appended, or (b) append a null in `unexpectedValue()` so the block always advances. Option (b) is a one-line fix.

---

### NDJSON-2: Nested objects and arrays of objects crash

**Estimate:** 0.5w
**Priority:** MVP — 🔴 query crashes with NullPointerException
**Affected patterns:** Any nested object (`{"a": {"b": 1}}`), any array of objects (`{"events": [{"type": "click"}]}`), any nesting depth

#### Prevalence

Extremely common. Nested JSON is the norm — virtually every real-world NDJSON file has nested objects. Log events, API responses, database exports, and analytics payloads all contain nested structures. This is the single most impactful NDJSON bug: it makes the format usable only for flat, single-level JSON.

**Testing gap:** Schema inference for nested objects is tested (`testInferSchemaForNestedJson` in `NdJsonSchemaInferrerTests.java` verifies dot-notation flattening), but there is no test for decoding nested objects. All integration tests and the `employees.ndjson` test data are entirely flat — no nested objects, no arrays of objects. The NPE only occurs in the decode path, which has zero test coverage for nested structures.

#### User impact

When an NDJSON file contains nested objects (`{"a": {"b": 1}}`) or arrays of objects (`[{"key": "val"}]`), the query fails with an internal error (NullPointerException). No data is returned. The error message does not indicate that nested objects are the cause.

#### How to reproduce

**Nested objects:**

```java
package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReproNdjson2aTest extends ESTestCase {
    public void testNestedObjectCrash() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"address": {"city": "NYC", "zip": "10001"}}
            {"address": {"city": "London", "zip": "SW1A"}}
            """;

        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("address.city", "address.zip"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());
        }
    }
}
```

**Fails with:** `NullPointerException` at `NdJsonPageDecoder.java` — intermediate `BlockDecoder` node has `attribute=null`.

**Arrays of objects:**

```java
package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReproNdjson2bTest extends ESTestCase {
    public void testArrayOfObjectsCrash() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"events": [{"type": "click"}, {"type": "view"}], "id": 1}
            """;

        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("events.type", "id"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertEquals(1, page.getPositionCount());
        }
    }
}
```

**Fails with:** `NullPointerException: "this.blockBuilder" is null` — `blockBuilder` is null on intermediate decoder nodes.

#### Why it happens

Schema inference correctly flattens nested objects to dot-notation (e.g., `address.city`). But during decoding, `prepareSchema` creates intermediate `BlockDecoder` nodes (for `address`) with `attribute = null` and `blockBuilder = null`.

**Nested objects:** When `decodeValue()` is called on an intermediate node with a `START_OBJECT` token, it falls through to `switch (attribute.dataType())` at line 250. Since `attribute` is null, NPE is thrown.

**Arrays of objects:** When `decodeValue()` is called with `START_ARRAY`, line 237 calls `this.blockBuilder.beginPositionEntry()`. Since `blockBuilder` is null, NPE is thrown.

```java
// NdJsonPageDecoder.java:250 — attribute is null for intermediate nodes
switch (attribute.dataType()) {  // NPE here
```

#### Fix

Add handling at the top of `decodeValue()` for intermediate decoder nodes (those with `children != null` and `attribute == null`): when `token == START_OBJECT`, call `this.decodeObject(parser)`. For `START_ARRAY`, iterate elements and call `decodeObject()` for each. Multi-valued semantics for sub-fields in arrays of objects need design — simplest approach is to flatten all values into a single multi-value position per leaf field.

---

### NDJSON-3: Nulls inside arrays produce wrong row counts

**Estimate:** 0.5w
**Priority:** MVP — 🔴 silently wrong data
**Affected patterns:** Any array containing null elements (e.g., `["a", null, "b"]`)

#### Prevalence

Common. Sparse data, optional values in arrays, and upstream systems that emit null placeholders all produce arrays with nulls. Any NDJSON file from a database export or API response where array elements can be nullable will trigger this.

#### User impact

When an array field contains null elements (e.g., `["a", null, "b"]`), what should be a single row with multi-value `["a", "b"]` becomes 3+ separate rows. All subsequent columns are misaligned. In debug mode: `AssertionError`. In production: silently wrong data for every subsequent row.

**Testing gap:** `NdJsonSchemaInferrerTests.testInferSchemaForJsonWithArrays` infers `{"scores": [70, null]}` correctly as nullable, but no test decodes arrays containing null elements. The `employees.ndjson` arrays (`job_positions`, `salary_change`) have no null elements.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReproNdjson3Test extends ESTestCase {
    public void testNullsInArrayCorruptPositions() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"tags": ["a", null, "b"], "id": 1}
            {"tags": ["c", "d"], "id": 2}
            """;

        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("tags", "id"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertEquals(page.getBlock(0).getPositionCount(), page.getBlock(1).getPositionCount());
            assertEquals(2, page.getPositionCount());
        }
    }
}
```

**Fails with:** Block position count mismatch — tags block has 3+ positions (null splits the multi-value), id block has 2.

#### Why it happens

Inside an array, `beginPositionEntry()` is open. When a null element is encountered, `blockBuilder.appendNull()` is called (`NdJsonPageDecoder.java:245-248`). But `AbstractBlockBuilder.appendNull()` (`AbstractBlockBuilder.java:44-46`) checks `positionEntryIsOpen` — it is true, so it calls `endPositionEntry()`, prematurely closing the multi-value position. The null is then added as a separate position. Subsequent elements become bare values. The final `endPositionEntry()` at line 241 creates a phantom position.

```java
// AbstractBlockBuilder.java:44-46 — appendNull closes position entry
if (positionEntryIsOpen) {
    endPositionEntry();  // premature close!
}
```

#### Fix

Inside `decodeValue()`, when `VALUE_NULL` is encountered and we are inside a position entry (array context), skip the null rather than calling `appendNull()`. ESQL multi-values don't support embedded nulls, so skipping is semantically correct.

---

### NDJSON-4: BigInteger overflow crashes query

**Estimate:** 0.5w
**Priority:** MVP — 🔴 query crashes
**Affected patterns:** Integer values exceeding `Long.MAX_VALUE` (9,223,372,036,854,775,807)

#### Prevalence

Rare. Affects unsigned 64-bit IDs, crypto hashes, and snowflake IDs near the upper range. Most JSON integer values fit in a long. However, when it does occur, the entire query fails — not just the offending row.

#### User impact

When an NDJSON file contains integer values exceeding `Long.MAX_VALUE` (e.g., uint64 IDs, snowflake IDs at the upper range), the query fails with `InputCoercionException: Numeric value (18446744073709551615) out of range of long`. The entire query returns no data, even if only one row has the large value.

**Testing gap:** No test for BigInteger values in either inference or decoding. The `employees.ndjson` contains only regular integers and doubles. Neither `NdJsonSchemaInferrerTests` nor `NdJsonPageIteratorTests` exercise values exceeding `Long.MAX_VALUE`.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReproNdjson4Test extends ESTestCase {
    public void testBigIntegerCrash() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"id": 1, "big": 18446744073709551615}
            {"id": 2, "big": 42}
            """;

        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("id", "big"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertEquals(2, page.getPositionCount());
        }
    }
}
```

**Fails with:** `InputCoercionException: Numeric value (18446744073709551615) out of range of long` — entire query fails.

#### Why it happens

Jackson classifies the token as `VALUE_NUMBER_INT` with `NumberType.BIG_INTEGER`. Calling `parser.getLongValue()` throws `InputCoercionException` (extends `JsonProcessingException`, sibling of `JsonParseException`). The catch blocks in both inference (`NdJsonSchemaInferrer.java:72,82`) and decoding (`NdJsonPageDecoder.java:99,111`) only catch `JsonParseException`, so `InputCoercionException` propagates uncaught.

```java
// NdJsonSchemaInferrer.java:132 — throws InputCoercionException for BIG_INTEGER
long value = parser.getLongValue();  // crash
```

#### Fix

Check `parser.getNumberType() == NumberType.BIG_INTEGER` before calling `getLongValue()`. Either widen to DOUBLE (losing precision for values > 2^53) or fall back to KEYWORD (preserving exact value as string). Also catch `JsonProcessingException` (or `InputCoercionException`) in addition to `JsonParseException`.

---

### NDJSON-5: Date-only strings not detected as DATETIME

**Estimate:** 0.5w
**Priority:** MVP — 🟡 stored as KEYWORD instead of DATETIME
**Affected patterns:** Date-only strings like `"2024-01-15"` (no time component)

#### Prevalence

Common. Many datasets use date-only formats — event dates, birth dates, transaction dates. However, workaround exists: users can convert with ESQL functions after ingestion.

#### User impact

When an NDJSON file contains date-only strings like `"2024-01-15"` (without time component or timezone), a `date` column containing `"2024-01-15"` appears as KEYWORD. Date arithmetic doesn't work: `WHERE date > "2024-01-01"` does string comparison instead of date comparison. `| EVAL days_ago = NOW() - date` fails.

**Testing gap:** No test for date-only strings. The `employees.ndjson` uses full ISO datetime strings (`"1953-09-02T00:00:00Z"`) throughout — no date-only values. `NdJsonSchemaInferrerTests` has no test for `"YYYY-MM-DD"` patterns.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ReproNdjson5Test extends ESTestCase {
    public void testDateOnlyStringNotDetectedAsDatetime() throws IOException {
        String ndjson = """
            {"date": "2024-01-15", "id": 1}
            {"date": "2024-06-30", "id": 2}
            """;

        var inferrer = NdJsonSchemaInferrer.inferSchema(
            new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8))
        );

        var dateAttr = inferrer.stream().filter(a -> a.name().equals("date")).findFirst().orElseThrow();
        assertEquals(DataType.DATETIME, dateAttr.dataType());
    }
}
```

**Fails with:** `AssertionError: expected DATETIME but was KEYWORD` — `Instant.parse()` rejects date-only strings.

#### Why it happens

`Instant.parse("2024-01-15")` throws `DateTimeParseException` because `Instant.parse()` requires a time component (ISO-8601 instant format). The value falls to KEYWORD. The inferrer doesn't try `LocalDate.parse()` or any other date pattern.

#### Fix

After `Instant.parse()` fails, try `LocalDate.parse()` and convert to `LocalDate.atStartOfDay(ZoneOffset.UTC).toInstant()`. Or support a configurable date format pattern.

---

### NDJSON-6: Nested arrays produce wrong data

**Estimate:** 0.5w
**Priority:** Post-MVP — 🟡 block misalignment
**Affected patterns:** Nested arrays like `[[1,2],[3,4]]`

#### Prevalence

Rare. Matrix data, batched payloads, and multi-dimensional arrays. Most NDJSON uses flat arrays.

#### User impact

When an NDJSON file contains nested arrays (e.g., `[[1,2],[3,4]]`), `{"matrix": [[1,2],[3,4]], "id": 1}` becomes multiple rows instead of one. Block misalignment — in debug mode: assertion error. In production: wrong data.

**Testing gap:** No test for nested arrays in either inference or decoding. The `employees.ndjson` arrays are all flat — no array-of-array patterns. `NdJsonSchemaInferrerTests` does not exercise nested array inference.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReproNdjson6Test extends ESTestCase {
    public void testNestedArraysMisalignment() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();

        String ndjson = """
            {"matrix": [[1,2],[3,4]], "id": 1}
            {"matrix": [[5,6]], "id": 2}
            """;

        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("matrix", "id"), 100)) {
            assertTrue(iterator.hasNext());
            var page = iterator.next();
            assertEquals(page.getBlock(0).getPositionCount(), page.getBlock(1).getPositionCount());
            assertEquals(2, page.getPositionCount());
        }
    }
}
```

**Fails with:** Block position count mismatch — matrix block has 3+ positions from nested `beginPositionEntry()` calls, id block has 2.

#### Why it happens

The outer array calls `beginPositionEntry()`. Each inner array also calls `beginPositionEntry()`, which auto-closes the previous position entry (`AbstractBlockBuilder.java:73-75`). This splits one position into multiple positions. The outer `endPositionEntry()` creates a phantom position.

#### Fix

Detect nesting depth and either flatten nested arrays into a single multi-value position or reject them as UNSUPPORTED during inference.

---

### NDJSON-7: All-null columns crash when data contains arrays

**Estimate:** 0.5w
**Priority:** Post-MVP — 🟡 crash on edge case
**Affected patterns:** Field is null in all 100 sample rows, then contains an array in actual data

#### Prevalence

Very rare. Only occurs when all sample rows have null for a field but actual data has arrays. Mitigated by increasing sample size.

#### User impact

When all rows in the schema-sampling window (first 100 lines) have null for a field, but subsequent rows contain arrays for that field, the query crashes with `UnsupportedOperationException`. No data returned.

**Testing gap:** `NdJsonSchemaInferrerTests.testInferSchemaForNullFields` tests inference of all-null columns (correctly resolves to NULL type), but no test decodes data where a NULL-typed column later encounters non-null values. The `employees.ndjson` has no all-null columns.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ReproNdjson7Test extends ESTestCase {
    public void testNullTypedColumnCrashOnArray() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("{\"data\": null, \"id\": ").append(i).append("}\n");
        }
        sb.append("{\"data\": [1, 2, 3], \"id\": 100}\n");
        String ndjson = sb.toString();

        var reader = new NdJsonFormatReader(blockFactory);
        var object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));

        try (var iterator = reader.read(object, List.of("data", "id"), 200)) {
            List<Page> pages = new ArrayList<>();
            while (iterator.hasNext()) {
                pages.add(iterator.next());
            }
            int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
            assertEquals(101, totalRows);
        }
    }
}
```

**Fails with:** `UnsupportedOperationException` at `ConstantNullBlock$Builder.beginPositionEntry()` — NULL-typed builder doesn't support multi-value operations.

#### Why it happens

If all sample values are null, the type resolves to `NULL` and the builder is `ConstantNullBlock.Builder` (`NdJsonPageDecoder.java:195`). When actual data contains an array, `decodeValue()` calls `this.blockBuilder.beginPositionEntry()` at line 237. `ConstantNullBlock.Builder.beginPositionEntry()` throws `UnsupportedOperationException`.

#### Fix

Handle NULL type specially in the array decoding path — skip array elements or convert to a non-null type upon first non-null value.

---

## Appendix: Per-Type Breakdown

Complete mapping of every JSON value pattern to ESQL, organized by category. Rows ordered by status: ⬛ → 🔴 → 🟡 → 🟢.

### Scalar Types

| JSON Pattern | ESQL DataType | Status | Issue |
|---|---|---|---|
| `"hello"` (plain string) | KEYWORD | 🟢 | |
| `42` (integer, fits int32) | INTEGER | 🟢 | |
| `9999999999` (integer, exceeds int32) | LONG | 🟢 | |
| `3.14` (float/decimal) | DOUBLE | 🟢 | |
| `true` / `false` | BOOLEAN | 🟢 | |
| `null` | NULL | 🟢 | |
| `18446744073709551615` (exceeds Long.MAX_VALUE) | N/A | 🔴 | [NDJSON-4](#ndjson-4-biginteger-overflow-crashes-query) |

### Datetime Detection

ISO-8601 strings are auto-detected as DATETIME during schema inference via `Instant.parse()` (`NdJsonSchemaInferrer.java:125`).

| JSON Pattern | ESQL DataType | Status | Issue |
|---|---|---|---|
| Mixed: ISO datetime in row 1, plain string in row 2 | DATETIME | 🔴 | [NDJSON-1](#ndjson-1-schema-mismatch-silently-corrupts-data) |
| `"2024-01-15"` (date only, no time) | KEYWORD | 🟡 | [NDJSON-5](#ndjson-5-date-only-strings-not-detected-as-datetime) |
| `"2024-01-15T10:30:00Z"` | DATETIME | 🟢 | |
| `"2024-01-15T10:30:00+05:00"` (with offset) | DATETIME | 🟢 | |
| `"Jan 15, 2024"` (non-ISO) | KEYWORD | 🟢 | |

### Type Widening (Heterogeneous Scalars)

When a field has multiple scalar types across sample rows, `resolveType()` (`NdJsonSchemaInferrer.java:201-227`) applies priority: DATETIME > KEYWORD > DOUBLE > LONG > INTEGER.

| JSON Pattern | ESQL DataType | Status | Issue |
|---|---|---|---|
| `42` then `"hello"` (int + string) | KEYWORD | 🟡 | Integer silently stringified. No crash but loses numeric type. |
| `42` then `3.14` (int + double) | DOUBLE | 🟢 | |
| `42` then `9999999999` (int + long) | LONG | 🟢 | |

### Arrays

NDJSON is the only external format that produces multi-valued blocks via `beginPositionEntry()`/`endPositionEntry()`.

| JSON Pattern | ESQL DataType | Status | Issue |
|---|---|---|---|
| `["a", null, "b"]` (array with nulls) | KEYWORD | 🔴 | [NDJSON-3](#ndjson-3-nulls-inside-arrays-produce-wrong-row-counts) |
| `[1, true]` (int + boolean array) | INTEGER | 🔴 | [NDJSON-1](#ndjson-1-schema-mismatch-silently-corrupts-data) |
| `[[1,2],[3,4]]` (nested arrays) | INTEGER | 🟡 | [NDJSON-6](#ndjson-6-nested-arrays-produce-wrong-data) |
| `[]` (always empty across all samples) | UNSUPPORTED | 🟡 | Column excluded from schema. |
| `[1, "hello"]` (int + string array) | KEYWORD | 🟡 | Integer elements silently stringified. |
| `[1, 2, 3]` (homogeneous int) | INTEGER | 🟢 | |
| `["a", "b", "c"]` (homogeneous string) | KEYWORD | 🟢 | |
| `[true, false]` (homogeneous boolean) | BOOLEAN | 🟢 | |
| `[1.1, 2.2]` (homogeneous double) | DOUBLE | 🟢 | |
| `["2024-01-01T00:00:00Z", ...]` (datetime array) | DATETIME | 🟢 | |
| `[]` then `[1, 2]` (empty then non-empty) | INTEGER | 🟢 | |

### Nested Objects

Nested JSON objects are flattened to dot-notation during schema inference (`NdJsonSchemaInferrer.java:98-111`). The decoder reconstructs the hierarchy via a tree of `BlockDecoder` nodes.

| JSON Pattern | ESQL DataType | Status | Issue |
|---|---|---|---|
| `{"a": {"b": 1}}` (nested object) | `a.b` = INTEGER | 🔴 | [NDJSON-2](#ndjson-2-nested-objects-and-arrays-of-objects-crash) |
| `{"a": {"b": {"c": 1}}}` (deep nesting) | `a.b.c` = INTEGER | 🔴 | [NDJSON-2](#ndjson-2-nested-objects-and-arrays-of-objects-crash) |
| `{"events": [{"type": "click"}]}` (array of objects) | `events.type` = KEYWORD | 🔴 | [NDJSON-2](#ndjson-2-nested-objects-and-arrays-of-objects-crash) |
| `{"x": 1}` then `{"x": {"y": 2}}` (flat then nested) | `x` = INTEGER, `x.y` = INTEGER | 🔴 | [NDJSON-1](#ndjson-1-schema-mismatch-silently-corrupts-data) |

### Edge Cases

| JSON Pattern | ESQL DataType | Status | Issue |
|---|---|---|---|
| NULL-typed field then array in actual data | NULL | 🔴 | [NDJSON-7](#ndjson-7-all-null-columns-crash-when-data-contains-arrays) |
| Field present in some rows, absent in others | Inferred type | 🟢 | Null-fill loop handles missing fields. |
| `{"data": null}` then `{"data": 42}` | INTEGER | 🟢 | NULL not in priority list; INTEGER wins. |
| `{"id": 1e308}` (double overflow) | DOUBLE | 🟢 | Jackson returns IEEE 754 value. |
| `{"val": ""}` (empty string) | KEYWORD | 🟢 | `Instant.parse("")` fails, falls to KEYWORD. |
