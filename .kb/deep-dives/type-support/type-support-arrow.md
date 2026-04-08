# Arrow/Flight Type Support in ES|QL External Data Sources

There are **two independent inbound paths** for converting Arrow data into ES|QL blocks, plus one **outbound path** for sending ES|QL results as Arrow IPC:

1. **`ArrowToBlockConverter`** (`x-pack/plugin/esql/arrow/.../ArrowToBlockConverter.java`) -- Generic Arrow-to-ESQL converter used for Arrow IPC ingest. Dispatches via a `switch` on `Types.MinorType`. Returns `null` for unhandled types (no exception). Handles **9 types**.

2. **`FlightTypeMapping`** (`x-pack/plugin/esql-datasource-grpc/.../FlightTypeMapping.java`) -- Flight-connector-specific mapper. Has two methods: `toDataType()` (schema-time, uses `instanceof ArrowType`) and `toBlock()` (data-time, uses `instanceof FieldVector`). **These two methods accept different type sets**, creating a schema-data mismatch bug. Handles **6 types** in `toBlock()`.

3. **`BlockConverter`** (`x-pack/plugin/esql/arrow/.../BlockConverter.java`) + **`ArrowResponse`** (`x-pack/plugin/esql/arrow/.../ArrowResponse.java`) -- Outbound converter, maps ESQL types to Arrow types for the `_query` REST endpoint's Arrow IPC format. Handles **20 ESQL type strings** across 13 `BlockConverter` subclass instances.

These are **completely separate codebases** with **different type coverage**. The Flight connector (`FlightTypeMapping`) does **not** use `ArrowToBlockConverter` at all -- the call chain is `FlightResultCursor.next()` (line 51) -> `FlightTypeMapping.toBlock()` (line 54). This means types supported in the generic Arrow path may silently fail in the Flight path, and vice versa. This is itself a design issue.

---

## 1. Boolean

| Arrow MinorType | ArrowToBlockConverter | FlightTypeMapping | Status | What Happens |
|---|---|---|---|---|
| BIT | 🟢 Handled | Handled | 🟢 Both paths work | Both paths convert `BitVector` to `BooleanBlock` via `bitVector.get(i) != 0`. ArrowToBlockConverter: `FromBoolean` class (line 182). FlightTypeMapping: `instanceof BitVector` branch (line 100). Lossless conversion. |

## 2. Integer Types

| Arrow MinorType | ArrowToBlockConverter | FlightTypeMapping | Status | What Happens |
|---|---|---|---|---|
| TINYINT | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: `forType(TINYINT)` returns `null` (line 81, default branch). FlightTypeMapping: `toDataType()` accepts `ArrowType.Int` with bitWidth=8 and maps to `DataType.INTEGER` (line 128), but `toBlock()` checks `instanceof IntVector` (line 55) -- a `TinyIntVector` is NOT an `IntVector`, so it throws `IllegalArgumentException` at line 123. Schema says INTEGER, query crashes at execution time. |
| SMALLINT | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: `forType(SMALLINT)` returns `null` (line 81). FlightTypeMapping: `toDataType()` accepts `ArrowType.Int` with bitWidth=16 and maps to `DataType.INTEGER` (line 128), but `toBlock()` fails because `SmallIntVector` is NOT an `IntVector`. Same schema-data mismatch as TINYINT. |
| INT | 🟢 Handled | Handled | 🟢 Both paths work | Both paths convert `IntVector` to `IntBlock`. ArrowToBlockConverter: `FromInt32` class (line 159), direct `intVector.get(i)`. FlightTypeMapping: `instanceof IntVector` branch (line 55), direct `intVec.get(i)`. Lossless. |
| BIGINT | 🟢 Handled | Handled | 🟢 Both paths work | Both paths convert `BigIntVector` to `LongBlock`. ArrowToBlockConverter: `FromInt64` class (line 136), direct `bigIntVector.get(i)`. FlightTypeMapping: `instanceof BigIntVector` branch (line 66), direct `bigIntVec.get(i)`. Lossless. |
| UINT1 | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null` (line 81). FlightTypeMapping: `toDataType()` accepts `ArrowType.Int` (unsigned, bitWidth=8) and maps to `DataType.INTEGER` (line 128 -- `getBitWidth() <= 32`), but `toBlock()` fails because `UInt1Vector` is NOT an `IntVector`. Schema-data mismatch: schema says INTEGER, query crashes. |
| UINT2 | ❌ Not handled | Not handled | 🔴 Neither path | Same pattern as UINT1. `UInt2Vector` (bitWidth=16) passes schema as INTEGER but crashes in `toBlock()`. |
| UINT4 | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: `toDataType()` maps `ArrowType.Int` (unsigned, bitWidth=32) to `DataType.INTEGER` (line 128), but `UInt4Vector` is NOT an `IntVector` -- crashes in `toBlock()`. Additionally, unsigned 32-bit values can exceed `Integer.MAX_VALUE`, so mapping to INTEGER is semantically wrong; should be LONG. |
| UINT8 | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: `toDataType()` maps `ArrowType.Int` (unsigned, bitWidth=64) to `DataType.LONG` (line 128 -- `getBitWidth() > 32`), but `UInt8Vector` is NOT a `BigIntVector` -- crashes in `toBlock()`. Note: the outbound path (`ArrowResponse`) sends `unsigned_long` as `UINT8` (line 389), but neither inbound path can receive it back. No round-trip for unsigned longs. |

## 3. Floating-Point Types

| Arrow MinorType | ArrowToBlockConverter | FlightTypeMapping | Status | What Happens |
|---|---|---|---|---|
| FLOAT2 | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null` (line 81). FlightTypeMapping: `toDataType()` accepts `ArrowType.FloatingPoint(HALF)` and maps to `DataType.DOUBLE` (line 133, the `default` branch covers HALF), but Arrow Java 18.3.0 does not have a dedicated vector class for half-float, so runtime behavior is unclear. Both paths effectively do not support it. |
| FLOAT4 | 🟢 Handled | ❌ Not handled | 🟡 One path only | ArrowToBlockConverter handles `Float4Vector` -- widens float32 to double via `(double) f4v.get(i)`, stored in `DoubleBlock` (`FromFloat32` class, line 89). Correct values, lossless widening. FlightTypeMapping does NOT handle `Float4Vector` -- `toDataType()` accepts `ArrowType.FloatingPoint(SINGLE)` and maps to `DataType.DOUBLE` (line 132), so the column appears in the schema as DOUBLE. But when data arrives and `toBlock()` is called, `Float4Vector` does not match any of the `instanceof` checks (IntVector, BigIntVector, Float8Vector, VarCharVector, BitVector, TimeStampMilliVector), so it throws `IllegalArgumentException` at line 123. The same data format works via Arrow IPC but fails via Flight. The Flight path silently accepts the schema but crashes at execution time. |
| FLOAT8 | 🟢 Handled | Handled | 🟢 Both paths work | Both paths convert `Float8Vector` to `DoubleBlock`. ArrowToBlockConverter: `FromFloat64` class (line 113), direct `f8v.get(i)`. FlightTypeMapping: `instanceof Float8Vector` branch (line 77), direct `float8Vec.get(i)`. Lossless. |

## 4. String/Binary Types

| Arrow MinorType | ArrowToBlockConverter | FlightTypeMapping | Status | What Happens |
|---|---|---|---|---|
| VARCHAR | 🟢 Handled | Handled | 🟢 Both paths work | Both convert `VarCharVector` to `BytesRefBlock` via `new BytesRef(bytes)`. ArrowToBlockConverter: `FromVarChar` class (line 205). FlightTypeMapping: `instanceof VarCharVector` branch (line 88). No encoding validation -- raw UTF-8 bytes passed through. |
| LARGEVARCHAR | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: `toDataType()` does not match `ArrowType.LargeUtf8` -- throws `IllegalArgumentException` at line 142. Uses 64-bit offsets (>2GB string data per column), otherwise identical to VARCHAR. |
| VARBINARY | 🟢 Handled | ❌ Not handled | 🟡 One path only | ArrowToBlockConverter handles `VarBinaryVector` -- converts to `BytesRefBlock` via `new BytesRef(bytes)` (`FromVarBinary` class, line 229). FlightTypeMapping does NOT handle `VarBinaryVector` -- `toDataType()` does not match `ArrowType.Binary`, so it throws `IllegalArgumentException` at schema time (line 142). Unlike the integer/float/timestamp mismatch bugs, this one fails early at schema resolution. The outbound path sends IP, geo_point, geo_shape, cartesian_point, and cartesian_shape as VARBINARY, but the Flight inbound path cannot receive any of them. |
| LARGEVARBINARY | ❌ Not handled | Not handled | 🔴 Neither path | Neither path handles `LargeVarBinaryVector`. 64-bit offset variant. |
| FIXEDSIZEBINARY | ❌ Not handled | Not handled | 🔴 Neither path | Neither path handles `FixedSizeBinaryVector`. Could be useful for UUIDs (16 bytes), fixed-width hashes. |

## 5. Decimal Types

| Arrow MinorType | ArrowToBlockConverter | FlightTypeMapping | Status | What Happens |
|---|---|---|---|---|
| DECIMAL | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: `toDataType()` does not match `ArrowType.Decimal` -- throws `IllegalArgumentException` at schema time (line 142). ESQL has no native decimal type. Any database with DECIMAL/NUMERIC columns (PostgreSQL, MySQL, Oracle) sends these through Flight. |
| DECIMAL256 | ❌ Not handled | Not handled | 🔴 Neither path | Same as DECIMAL. `Decimal256Vector` (256-bit) also unhandled in both paths. |

## 6. Date/Time Types

| Arrow MinorType | ArrowToBlockConverter | FlightTypeMapping | Status | What Happens |
|---|---|---|---|---|
| DATEDAY | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: `toDataType()` does not match `ArrowType.Date` -- throws `IllegalArgumentException` at schema time (line 142). `DateDayVector` stores days since epoch. Could convert to DATETIME via `days * 86400000L`. Common in JDBC Flight servers for SQL DATE columns. |
| DATEMILLI | ❌ Not handled | Not handled | 🔴 Neither path | Same as DATEDAY. `DateMilliVector` stores millis since midnight epoch. Direct mapping to DATETIME possible. |
| TIMESEC | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: `toDataType()` does not match `ArrowType.Time` -- throws at schema time (line 142). Represents seconds within a day. ESQL has no native TIME type. |
| TIMEMILLI | ❌ Not handled | Not handled | 🔴 Neither path | Same as TIMESEC. Milliseconds within day. |
| TIMEMICRO | ❌ Not handled | Not handled | 🔴 Neither path | Same as TIMESEC. Microseconds within day. |
| TIMENANO | ❌ Not handled | Not handled | 🔴 Neither path | Same as TIMESEC. Nanoseconds within day. |
| TIMESTAMPSEC | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null` (line 81). FlightTypeMapping: `toDataType()` accepts `ArrowType.Timestamp` (all units) and maps to `DataType.DATETIME` (line 139), but `toBlock()` only handles `TimeStampMilliVector` (line 111). A `TimeStampSecVector` fails the `instanceof` chain and throws `IllegalArgumentException` at line 123. Schema says DATETIME, query crashes at execution time. Conversion would be `secs * 1000`. |
| TIMESTAMPMILLI | ❌ Not handled | 🟢 Handled | 🟡 One path only | ArrowToBlockConverter: `forType(TIMESTAMPMILLI)` returns `null` (line 81, default branch). FlightTypeMapping handles `TimeStampMilliVector` directly -- `tsVec.get(i)` with no unit conversion needed (line 117). This creates a round-trip asymmetry: the outbound path (`ArrowResponse` line 399) sends ESQL dates as `TIMESTAMPMILLI`, but if a consumer reads this data and sends it back through `ArrowToBlockConverter`, it fails. |
| TIMESTAMPMICRO | 🟢 Handled | ❌ Not handled | 🟡 One path only | ArrowToBlockConverter handles `TimeStampMicroVector` -- divides micros by 1000 to get millis, stored in `LongBlock` (`FromTimestampMicro` class, line 253, conversion at line 266). Sub-millisecond precision truncated. FlightTypeMapping: `toDataType()` accepts `ArrowType.Timestamp(MICROSECOND)` and maps to `DataType.DATETIME` (line 139), but `toBlock()` only handles `TimeStampMilliVector` -- `TimeStampMicroVector` fails the `instanceof` chain and throws at line 123. **This is the most impactful schema-data mismatch**: Parquet's default timestamp type is `TIMESTAMP(MICROS)`, so any Flight server reading Parquet data (DuckDB, DataFusion, Spark, Dremio) sends `TimeStampMicroVector` or `TimeStampMicroTZVector`. The Flight path accepts the schema but crashes on data. |
| TIMESTAMPNANO | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: `toDataType()` accepts `ArrowType.Timestamp(NANOSECOND)` but `toBlock()` crashes (same schema-data mismatch). Conversion would be `nanos / 1_000_000`. Appears in trace/observability data. |
| TIMESTAMPSECTZ | ❌ Not handled | Not handled | 🔴 Neither path | Same pattern as TIMESTAMPSEC. `toDataType()` accepts it (any `ArrowType.Timestamp`), `toBlock()` crashes. |
| TIMESTAMPMILLITZ | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: `toDataType()` accepts it, but `toBlock()` only checks `instanceof TimeStampMilliVector` (no TZ variant). `TimeStampMilliTZVector` is NOT a `TimeStampMilliVector` -- crashes at line 123. |
| TIMESTAMPMICROTZ | 🟢 Handled | ❌ Not handled | 🟡 One path only | ArrowToBlockConverter handles `TimeStampMicroTZVector` -- divides micros by 1000, discards timezone (`FromTimestampMicroTZ` class, line 279, conversion at line 291). FlightTypeMapping: same crash as TIMESTAMPMICRO -- passes schema, fails at data. This is the **canonical Parquet timestamp type** (`TIMESTAMP(isAdjustedToUTC=true, unit=MICROS)`). |
| TIMESTAMPNANOTZ | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: `toDataType()` accepts it, `toBlock()` crashes. |

## 7. Complex Types

| Arrow MinorType | ArrowToBlockConverter | FlightTypeMapping | Status | What Happens |
|---|---|---|---|---|
| LIST | ❌ Not handled | Not handled | Neither path handles it (inbound) | ArrowToBlockConverter: returns `null` (line 81). FlightTypeMapping: `toDataType()` does not match `ArrowType.List` -- throws at schema time (line 142). The outbound path uses LIST for multi-valued fields (ArrowResponse `SchemaResponse`, line 195), but neither inbound path can convert `ListVector` back. No round-trip for multi-valued fields. |
| LARGELIST | ❌ Not handled | Not handled | 🔴 Neither path | Neither path handles `LargeListVector`. |
| FIXEDSIZELIST | ❌ Not handled | Not handled | 🔴 Neither path | Neither path handles `FixedSizeListVector`. |
| MAP | ❌ Not handled | Not handled | 🔴 Neither path | Neither path handles `MapVector`. FlightTypeMapping throws at schema time. |
| STRUCT | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: `forType(STRUCT)` hits the default branch and returns `null` (line 81). FlightTypeMapping: `toDataType()` does not match `ArrowType.Struct` -- throws at schema time (line 142). ESQL has no native struct type. |
| UNION | ❌ Not handled | Not handled | 🔴 Neither path | Neither path handles `UnionVector`. |
| DENSEUNION | ❌ Not handled | Not handled | 🔴 Neither path | Neither path handles `DenseUnionVector`. |

## 8. Interval/Duration Types

| Arrow MinorType | ArrowToBlockConverter | FlightTypeMapping | Status | What Happens |
|---|---|---|---|---|
| INTERVALDAY | ❌ Not handled | Not handled | 🔴 Neither path | ArrowToBlockConverter: returns `null`. FlightTypeMapping: does not match `ArrowType.Interval` -- throws at schema time (line 142). Stores days + milliseconds. ESQL has `TIME_DURATION` that could partially represent the millisecond component. |
| INTERVALYEAR | ❌ Not handled | Not handled | 🔴 Neither path | Same pattern. Stores months. ESQL has `DATE_PERIOD` that could represent this. |
| INTERVALMONTHDAYNANO | ❌ Not handled | Not handled | 🔴 Neither path | Same pattern. Stores months + days + nanoseconds. Spans both ESQL period types. |
| DURATION | ❌ Not handled | Not handled | 🔴 Neither path | Same pattern. FlightTypeMapping does not match `ArrowType.Duration` -- throws at schema time. |

---

## The Two-Path Problem

Having two completely separate type-mapping codepaths is a design issue with several consequences:

1. **Divergent type coverage**: ArrowToBlockConverter handles 9 types; FlightTypeMapping handles 6. Only 5 types overlap (BIT, INT, BIGINT, FLOAT8, VARCHAR). The 4 types unique to ArrowToBlockConverter (FLOAT4, VARBINARY, TIMESTAMPMICRO, TIMESTAMPMICROTZ) and the 1 type unique to FlightTypeMapping (TIMESTAMPMILLI) create user-visible inconsistencies where the same Arrow data format works in one path but fails in the other.

2. **No shared abstraction**: `FlightTypeMapping.toBlock()` (file: `FlightTypeMapping.java`, lines 54-124) duplicates the same pattern as `ArrowToBlockConverter.convert()` (file: `ArrowToBlockConverter.java`, lines 63 and subclasses) -- iterate rows, check nulls, append to builder -- but with different type coverage. Changes to one path are not reflected in the other.

3. **Schema-data mismatch in FlightTypeMapping only**: `ArrowToBlockConverter.forType()` returns `null` for unsupported types (line 81), allowing callers to handle gracefully. `FlightTypeMapping` has a split design where `toDataType()` (lines 126-143) accepts broader types than `toBlock()` (lines 54-124) can handle, creating time-delayed crashes: schema resolution succeeds, but the query fails at execution time when actual data vectors arrive. The affected type families are:
   - **Integers**: `ArrowType.Int` with bitWidth 8 or 16 passes `toDataType()` but `TinyIntVector`/`SmallIntVector` fail `instanceof IntVector` in `toBlock()`
   - **Unsigned integers**: All unsigned types pass `toDataType()` but their vector classes (`UInt1Vector` through `UInt8Vector`) match neither `IntVector` nor `BigIntVector`
   - **Float32**: `ArrowType.FloatingPoint(SINGLE)` passes `toDataType()` but `Float4Vector` fails `instanceof Float8Vector`
   - **Timestamps non-milli**: `ArrowType.Timestamp` (all units) passes `toDataType()` but only `TimeStampMilliVector` is handled in `toBlock()` -- sec, micro, nano, and all TZ variants crash

4. **The Flight connector is the higher-value path**: Arrow IPC ingest via `ArrowToBlockConverter` is used for the REST `_query` endpoint's arrow format. The Flight connector is the primary mechanism for integrating with external data systems (DuckDB, Spark, DataFusion, Dremio). Yet it has **fewer** supported types. The types most commonly sent by Flight servers -- especially `TimeStampMicro[TZ]Vector` (the Parquet default) and `Float4Vector` -- are supported in the generic converter but crash in the Flight path.

---

## Summary

### ArrowToBlockConverter (inbound, generic Arrow IPC)
- **9 types handled**: FLOAT4, FLOAT8, INT, BIGINT, BIT, VARCHAR, VARBINARY, TIMESTAMPMICRO, TIMESTAMPMICROTZ
- **~30+ types not handled**: returns `null` from `forType()` (file: `ArrowToBlockConverter.java`, line 81)

### FlightTypeMapping (inbound, Flight connector)
- **6 types handled in `toBlock()`**: IntVector, BigIntVector, Float8Vector, VarCharVector, BitVector, TimeStampMilliVector
- **~5 additional types accepted by `toDataType()` but crash in `toBlock()`**: TinyIntVector, SmallIntVector, UInt1-8Vector, Float4Vector, TimeStampSec/Micro/Nano[TZ]Vector
- **All other Arrow types**: throw `IllegalArgumentException` at schema time from `toDataType()` (file: `FlightTypeMapping.java`, line 142)

### BlockConverter (outbound, ESQL to Arrow IPC)
- **20 ESQL type strings handled** across 13 converter instances: null, unsupported, boolean, integer, counter_integer, long, counter_long, unsigned_long, double, counter_double, keyword, text, date, ip, geo_point, geo_shape, cartesian_point, cartesian_shape, version, _source (file: `ArrowResponse.java`, lines 371-418)
- Maps to **8 distinct Arrow MinorTypes**: NULL, BIT, INT, BIGINT, UINT8, FLOAT8, TIMESTAMPMILLI, VARCHAR, VARBINARY

### Round-trip Gaps
- `unsigned_long` -> Arrow UINT8 (outbound) but no UINT8 inbound handler in either path
- `date` -> Arrow TIMESTAMPMILLI (outbound) but ArrowToBlockConverter cannot read TIMESTAMPMILLI back
- Multi-valued fields -> Arrow LIST (outbound) but no LIST inbound handler in either path

---

## Recommendations

### P0: Critical (blocks common Flight server interop)

1. **Fix schema-data mismatch in FlightTypeMapping** (file: `FlightTypeMapping.java`). Either (a) tighten `toDataType()` to reject types that `toBlock()` cannot handle, failing early with a clear error, or (b) add handlers in `toBlock()` for all types that `toDataType()` accepts. Option (b) is strictly better because it adds capability.

2. **Add TIMESTAMPMICRO[TZ] to FlightTypeMapping**. This is the default Parquet timestamp type. Any Flight server reading Parquet data (DuckDB, DataFusion, Spark, Dremio) sends `TimeStampMicroTZVector`. The current code accepts the schema but crashes on data. Conversion: `micros / 1000` (same as ArrowToBlockConverter line 266).

3. **Add Float4Vector to FlightTypeMapping**. Flight servers sometimes send float32 for bandwidth efficiency. The schema path already accepts SINGLE precision but the data path crashes. Conversion: `(double) f4v.get(i)` (same as ArrowToBlockConverter line 101).

4. **Unify or bridge FlightTypeMapping and ArrowToBlockConverter**. The Flight connector should either delegate to `ArrowToBlockConverter` (which has more types) or at minimum share a common type registry. The current duplication is a maintenance risk -- every type added to one path must be independently remembered and added to the other.

### P1: Important (improves compatibility with common data sources)

5. **Add VarBinaryVector to FlightTypeMapping**. Needed for binary data, IP addresses, and geo types from Flight servers.

6. **Add TIMESTAMPMILLI to ArrowToBlockConverter**. Closes the round-trip gap -- the outbound path sends dates as TIMESTAMPMILLI, but the generic inbound path cannot read them back.

7. **Add all remaining timestamp variants** (both paths). TIMESTAMPSEC: `secs * 1000`. TIMESTAMPNANO: `nanos / 1_000_000`. TIMESTAMPMILLITZ: direct. All TZ variants: discard timezone. Each is a one-line conversion.

8. **Add narrow integer types** (TinyIntVector, SmallIntVector). Widen to IntBlock. Databases with TINYINT/SMALLINT columns (MySQL, PostgreSQL, SQL Server) send these through Flight.

9. **Add unsigned integer types**. UInt1/UInt2: widen to IntBlock. UInt4: widen to LongBlock (values can exceed Integer.MAX_VALUE). UInt8: map to UNSIGNED_LONG.

10. **Add Decimal support**. Financial databases universally use DECIMAL. Map to DOUBLE for precision <= 15, KEYWORD for higher precision.

11. **Add DateDay/DateMilli support**. SQL DATE columns map to DateDayVector by JDBC Flight servers. DateDay: `days * 86400000L`. DateMilli: direct.

### P2: Nice-to-have (uncommon but correct)

12. **Add LargeVarChar/LargeVarBinary**. Same as VARCHAR/VARBINARY but casting to Large variants. Only needed for >2GB string/binary data per column batch.

13. **Add FixedSizeBinary**. Map to BytesRefBlock. Useful for UUIDs, fixed-width hashes.

14. **Add LIST inbound**. Map to multi-valued ESQL blocks. Closes the round-trip gap for multi-valued fields.

15. **Add Time types**. Map to KEYWORD (formatted) or LONG (millis within day). ESQL has no native TIME type.

16. **Add Interval/Duration types**. Map to DATE_PERIOD/TIME_DURATION where possible. Uncommon in Flight.

17. **Add NullVector inbound**. Map to constant-null block of appropriate type.

---

## Key File Paths

| File | Purpose |
|---|---|
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowToBlockConverter.java` | Generic Arrow -> ESQL block converter (9 types, lines 70-83) |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/BlockConverter.java` | ESQL block -> Arrow converter base class (outbound) |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowResponse.java` | Arrow IPC response formatting + `ESQL_CONVERTERS` map (lines 371-418) |
| `x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightTypeMapping.java` | Flight-specific type mapping: `toDataType()` lines 126-143, `toBlock()` lines 54-124 |
| `x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightResultCursor.java` | FlightStream -> ESQL Page adapter, calls `FlightTypeMapping.toBlock()` at line 51 |
| `x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnector.java` | Flight client connection management, creates `FlightResultCursor` at line 85 |
