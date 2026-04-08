# Deep Dive: Circuit Breaker Memory Control for External Sources

## 1. BlockFactory Circuit Breaker Integration

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/BlockFactory.java`

### How it tracks memory

BlockFactory wraps a `CircuitBreaker` (line 35) and calls it on every block allocation. The breaker defaults to `CircuitBreaker.REQUEST` if none is provided (line 52).

**Key method** -- `adjustBreaker()` at line 107-115:
```java
public void adjustBreaker(final long delta) throws CircuitBreakingException {
    if (delta > 0) {
        breaker.addEstimateBytesAndMaybeBreak(delta, "<esql_block_factory>");
    } else {
        breaker.addWithoutBreaking(delta);
    }
}
```

Every block creation method calls `adjustBreaker()`:
- `newBooleanArrayBlock()` (line 157-161): `adjustBreaker(b.ramBytesUsed() - preAdjustedBytes)`
- `newIntArrayBlock()` (line 202-206): same pattern
- `newLongArrayBlock()` (line 320-325): same pattern
- `newDoubleArrayBlock()` (line 374-378): same pattern
- `newBytesRefArrayBlock()` (line 422-426): `adjustBreaker(b.ramBytesUsed() - values.bigArraysRamBytesUsed())`
- `newConstantNullBlock()` (line 450-453): `adjustBreaker(b.ramBytesUsed())`
- All builder `.build()` methods also adjust the breaker (see generated-src files, e.g. `DoubleBlockBuilder.java:179`)

**When blocks are released** (via `Block.close()`/`releaseBlocks()`), the corresponding block's `close()` calls `blockFactory().adjustBreaker(-ramBytesUsedOnlyBlock())` to release the memory.

### What circuit breaker is used

The `CircuitBreaker.REQUEST` breaker by default (line 52). In production ESQL, the `DriverContext` provides a `BlockFactory` that is backed by either:
- The parent (global) `REQUEST` circuit breaker, or
- A `LocalCircuitBreaker` wrapping the parent breaker

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/DriverContext.java`

The `DriverContext` (line 106-108) exposes `breaker()` which delegates to `blockFactory.breaker()`.

### LocalCircuitBreaker

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/LocalCircuitBreaker.java`

This is an optimization layer (line 29) that over-reserves memory to reduce atomic CAS contention on the parent breaker. Key behavior:
- Default over-reserve: 8KB (`LOCAL_BREAKER_OVER_RESERVED_DEFAULT_SIZE`, BlockFactory line 24)
- Max over-reserve: 512KB (`LOCAL_BREAKER_OVER_RESERVED_DEFAULT_MAX_SIZE`, line 27)
- `addEstimateBytesAndMaybeBreak()` (line 67-75): If requested bytes <= reserved, uses local cache. Otherwise, goes to parent breaker requesting `bytes - reservedBytes + overReservedBytes`.
- Single-threaded assertion (line 154-163): Asserts only one thread accesses it at a time.
- `close()` (line 142-146): Returns unused reserved bytes to parent.

### Summary

**BlockFactory's breaker integration is complete and correct for all ESQL blocks.** Every block allocation goes through `adjustBreaker()`, and every block release returns memory. This is the core compute engine's memory accounting.

---

## 2. AsyncExternalSourceBuffer

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceBuffer.java`

### Backpressure mechanism

Backpressure is **page-count-based, not byte-based**. The buffer tracks:
- `queueSize` (AtomicInteger, line 34): count of pages in queue
- `maxSize` (int, line 35): maximum page count

### `waitForSpace()` (line 152-165)

Returns a `SubscribableListener<Void>` that completes when `queueSize < maxSize`. Used by producers (background reader threads) to pause when the buffer is full.

```java
public SubscribableListener<Void> waitForSpace() {
    if (queueSize.get() < maxSize || noMoreInputs) {
        return SubscribableListener.newSucceeded(null);
    }
    // ... returns notFullFuture
}
```

### `waitForWriting()` (line 127-141)

Same logic but returns an `IsBlockedResult` instead. Used by `waitForReading()` (line 171-184) from the operator side.

### `maxSize` field

Set in constructor (line 48-53). Must be >= 1.

### Critical gap

**There is zero byte-based tracking.** The buffer has no concept of how many bytes the queued pages consume. A page with 10 rows of integers and a page with 100,000 rows of strings are both counted as "1 page". With `maxSize=10`, the buffer could theoretically hold up to 10 pages of arbitrary size -- potentially gigabytes of data -- with no memory limit.

### `waitForReading()` (line 171-184)

Returns `IsBlockedResult` that completes when pages are available or `noMoreInputs` is set. The `AsyncExternalSourceOperator.isBlocked()` (line 65-73) uses this to signal the Driver.

---

## 3. SourceOperatorContext

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceOperatorContext.java`

### `maxBufferSize` field

Declared in the record at line 47: `int maxBufferSize`. Validated at line 72-73 to be positive.

### Default value

The Builder at line 207 sets `maxBufferSize = 10` as default.

### Where it's used

In `LocalExecutionPlanner.java` (line 1266), it's hardcoded:
```java
.maxBufferSize(10)
```

This value is passed to `AsyncConnectorSourceOperatorFactory` (via `OperatorFactoryRegistry.factory()` at line 81 of OperatorFactoryRegistry.java):
```java
return new AsyncConnectorSourceOperatorFactory(connector, request, context.maxBufferSize(), executor, context.sliceQueue());
```

Which then creates `AsyncExternalSourceBuffer(maxBufferSize)` (AsyncConnectorSourceOperatorFactory.java line 67).

### No byte-based limit

There is **no byte-based buffer limit anywhere** in SourceOperatorContext. The only limit is page count (10 pages by default).

### No BlockFactory in context

SourceOperatorContext does **not** carry a `BlockFactory` or `CircuitBreaker`. The BlockFactory is either:
- Embedded in the `FormatReader` at construction time (e.g., `ParquetFormatReader(blockFactory)`)
- Passed via `QueryRequest.withBlockFactory(driverContext.blockFactory())` for connector paths

---

## 4. Parquet Row Group Memory

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`

### `readNextRowGroup()` (line 212)

Called in `ParquetPageIterator.hasNext()`:
```java
currentRowGroup = reader.readNextRowGroup();  // line 212
```

This calls Apache Parquet's `ParquetFileReader.readNextRowGroup()` which:
1. Reads the entire next row group from the underlying input stream
2. Decompresses all column pages within the row group
3. Returns a `PageReadStore` holding all the data in memory

### How much memory does a row group consume?

A Parquet row group is typically **128MB uncompressed** (the Parquet default). After decompression, the Parquet library allocates:
- Compressed column data read from storage
- Decompressed column pages (potentially 128MB+ per row group)
- The `GroupRecordConverter` and `RecordReader` overhead

### Is any of it tracked?

**No.** The Parquet library allocates memory using plain Java arrays and internal buffers. None of this is registered with any ES `CircuitBreaker`. The critical flow is:

1. `reader.readNextRowGroup()` -- Parquet allocates 128MB+ of Java heap (untracked)
2. Records are read into `Group` objects (line 237-241) -- more Java heap (untracked)
3. `Group` data is converted to ESQL blocks via `blockFactory.newXxxBlockBuilder()` -- **this IS tracked**
4. The `Group` objects and Parquet internal buffers are eventually GC'd -- never subtracted from any breaker

So there's a **double-memory window**: Parquet's internal buffers + ESQL blocks coexist in memory until the Parquet buffers are GC'd. Plus the Parquet buffers themselves are never tracked.

### Additional concern: batch accumulation (line 233-241)

```java
List<Group> batch = new ArrayList<>(batchSize);
int rowsToRead = (int) Math.min(batchSize, rowsRemainingInGroup);
for (int i = 0; i < rowsToRead; i++) {
    Group group = recordReader.read();
    if (group != null) {
        batch.add(group);
    }
}
```

With `batchSize=1000` (the default), up to 1000 `Group` objects are accumulated in memory before conversion to blocks. Each Group holds all column values as Java objects. This is untracked.

---

## 5. ORC Memory

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/src/main/java/org/elasticsearch/xpack/esql/datasource/orc/OrcFormatReader.java`

### How ORC allocates memory

The ORC reader uses `VectorizedRowBatch` (line 203):
```java
this.batch = schema.createRowBatch(batchSize);  // line 203
```

`createRowBatch()` pre-allocates column vectors with arrays sized to `batchSize`. For example, a `LongColumnVector` allocates a `long[batchSize]` array, a `BytesColumnVector` allocates `byte[][]` arrays.

### `rows.nextBatch(batch)` (line 221)

This reuses the pre-allocated `VectorizedRowBatch`, filling it in place. The ORC library:
1. Reads compressed stripes from the input stream
2. Decompresses stripe data into internal buffers
3. Fills the `VectorizedRowBatch` column vectors

### Is memory tracked?

**No.** Same as Parquet:
- `VectorizedRowBatch` and its column vectors are plain Java arrays (untracked)
- ORC internal decompression buffers are untracked
- Only the final ESQL blocks created via `blockFactory.newXxxBlockBuilder()` are tracked

### VectorizedRowBatch internals

The batch (line 185-186) holds `ColumnVector[] cols`. Each `ColumnVector` subtype:
- `LongColumnVector`: `long[] vector` (8 bytes * batchSize)
- `DoubleColumnVector`: `double[] vector` (8 bytes * batchSize)
- `BytesColumnVector`: `byte[][] vector`, `int[] start`, `int[] length` (~24 bytes * batchSize + actual byte data)
- `TimestampColumnVector`: `long[] time`, `int[] nanos` (12 bytes * batchSize)

With `batchSize=1000` and 10 columns, the pre-allocated batch is ~80KB-200KB (reasonable). The ORC library's internal stripe buffers are the bigger concern.

---

## 6. Flight Memory

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnector.java`

### RootAllocator (line 55)

```java
this.allocator = new RootAllocator();  // line 55
```

Apache Arrow's `RootAllocator` is created with **no memory limit** (default `Long.MAX_VALUE`). It manages Arrow's off-heap (`DirectByteBuffer`) memory for Flight buffers.

### How Flight allocates memory

1. `FlightClient.getStream(ticket)` (line 84) opens a gRPC stream
2. `FlightStream.next()` reads the next Arrow RecordBatch from the network
3. Arrow deserializes the batch into `VectorSchemaRoot` backed by `ArrowBuf` (off-heap)
4. `FlightResultCursor.next()` (FlightResultCursor.java line 47-54) converts Arrow vectors to ESQL blocks via `FlightTypeMapping.toBlock()`

### Is it tracked by ES circuit breakers?

**No.** The Arrow `RootAllocator` is completely independent of Elasticsearch's circuit breaker system:
- Arrow memory is off-heap (`DirectByteBuffer`) -- not even on the Java heap
- The `RootAllocator` has no limit, so it will allocate until `OutOfMemoryError`
- After `FlightTypeMapping.toBlock()` copies data into ESQL blocks (which ARE tracked), the Arrow buffers are released when `FlightStream.next()` is called again

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightTypeMapping.java`

The `toBlock()` method (line 54-123) copies data element-by-element from Arrow vectors into ESQL block builders. The builders use `blockFactory`, so the final ESQL blocks ARE tracked. But during conversion, both the Arrow buffer and the ESQL builder coexist in memory -- the Arrow buffer is untracked.

---

## 7. S3/GCS/Azure/HTTP Download Memory

### S3

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-s3/src/main/java/org/elasticsearch/xpack/esql/datasource/s3/S3StorageObject.java`

`newStream()` (line 99-117) returns a `ResponseInputStream<GetObjectResponse>` -- a streaming response. The AWS SDK buffers internally (typically 8KB-64KB).

`readBytesAsync()` (line 216-265) uses `AsyncResponseTransformer.toBytes()` which **buffers the entire range into a byte array** (line 263: `ByteBuffer.wrap(responseBytes.asByteArray())`). This is untracked memory.

### HTTP

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-http/src/main/java/org/elasticsearch/xpack/esql/datasource/http/HttpStorageObject.java`

`newStream()` (line 91-98) returns an `InputStream` from `HttpResponse.BodyHandlers.ofInputStream()` -- streaming, low memory.

`readBytesAsync()` (line 176-218) uses `HttpResponse.BodyHandlers.ofByteArray()` which **buffers the entire response range into a byte array**. This is untracked.

### GCS and Azure

Verified via grep: **zero CircuitBreaker references** in either plugin. Same pattern: streaming `newStream()` has low overhead; async byte reads buffer into arrays.

### StorageObject default async

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/StorageObject.java`

Default `readBytesAsync()` (line 74-83):
```java
default void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
    executor.execute(() -> {
        try (InputStream stream = newStream(position, length)) {
            byte[] bytes = stream.readAllBytes();  // ENTIRE range buffered!
            listener.onResponse(ByteBuffer.wrap(bytes));
        }
    });
}
```

`stream.readAllBytes()` buffers the entire range into a single byte array. For Parquet footer reads this is fine (small). For row group column chunk reads this could be significant.

### Summary of download memory

| Provider | Sync stream | Async byte read | Tracked? |
|----------|-------------|-----------------|----------|
| S3 | Streaming (~64KB buffer) | Full range in byte[] | No |
| GCS | Streaming | Full range in byte[] | No |
| Azure | Streaming | Full range in byte[] | No |
| HTTP | Streaming | Full range in byte[] | No |

Currently, only the sync streaming path is used (all format readers use `newStream()`). The async byte read path exists but is not yet wired for production reads.

---

## 8. Page.ramBytesUsedByBlocks()

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/Page.java`

### Does it exist?

Yes. At line 286-288:
```java
public long ramBytesUsedByBlocks() {
    return Arrays.stream(blocks).mapToLong(Accountable::ramBytesUsed).sum();
}
```

### What does it measure?

It sums `ramBytesUsed()` across all blocks in the page. Each block type implements `Accountable.ramBytesUsed()` which returns the block's memory footprint including:
- The block's own object overhead
- The backing array (e.g., `long[]` for LongArrayVector)
- For BytesRef blocks: the `BytesRefArray` overhead

This is the same value used by `BlockFactory.adjustBreaker()` when blocks are created, so it accurately reflects the circuit-breaker-tracked memory.

### Where is it used?

In `SessionUtils.checkPagesBelowSize()` (SessionUtils.java line 45-51) -- used to enforce maximum result size for ESQL query responses, not for external source buffering.

It is **NOT** used anywhere in the external sources data path.

---

## 9. What Exactly Needs to Change for Minimal TP Circuit Breaker Integration

### Problem summary

The external sources data path has **three categories of untracked memory**:

1. **Format reader internal buffers** -- Parquet row groups (~128MB), ORC stripes, Arrow Flight off-heap buffers. These are library-internal allocations completely invisible to ES circuit breakers.

2. **AsyncExternalSourceBuffer** -- Counts pages, not bytes. Can hold 10 pages of arbitrary size. No circuit breaker check when adding pages.

3. **FormatReader block creation** -- The ESQL blocks created via `BlockFactory` ARE tracked (this part works). But the format readers are constructed with a `BlockFactory` at plugin init time, not with the per-driver `BlockFactory` from `DriverContext`. Need to verify this is correct.

### Category 1: Format reader internals (DEFER to post-MVP)

Fixing Parquet row group memory tracking requires either:
- Wrapping Parquet's allocator (not easily possible with the current Parquet library)
- Pre-computing row group sizes from Parquet metadata and accounting before read
- Switching to an Arrow-native Parquet reader (already planned for MVP TP item "Arrow-native Parquet reader")

**Recommendation**: Defer. The Arrow-native Parquet reader will replace this code entirely. For ORC, the pre-allocated batch is bounded by `batchSize`.

### Category 2: AsyncExternalSourceBuffer (MUST FIX for TP)

This is the highest-risk gap. Changes needed:

**A. Add byte-based tracking to AsyncExternalSourceBuffer**

In `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceBuffer.java`:

1. Add `AtomicLong bytesUsed` field alongside `queueSize`
2. Add `long maxBytes` constructor parameter (e.g., 100MB default)
3. In `addPage()`: compute `page.ramBytesUsedByBlocks()` and add to `bytesUsed`
4. In `pollPage()`: subtract `page.ramBytesUsedByBlocks()` from `bytesUsed`
5. In `waitForSpace()`/`waitForWriting()`: check both `queueSize < maxSize` AND `bytesUsed < maxBytes`

**B. Wire circuit breaker through the buffer**

Option 1 (simpler): Add a `CircuitBreaker` parameter to the buffer. Call `breaker.addEstimateBytesAndMaybeBreak()` in `addPage()` and `breaker.addWithoutBreaking(-bytes)` in `pollPage()`.

Option 2 (pragmatic): Just enforce a byte-based cap in the buffer without circuit breaker integration. This prevents unbounded buffering even if it doesn't integrate with the node-level breaker.

**C. Propagate byte limit to SourceOperatorContext**

In `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceOperatorContext.java`:

Add a `long maxBufferBytes` field (or derive it from node settings).

In `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/LocalExecutionPlanner.java` (line 1260-1274):

Add `.maxBufferBytes(settings.getAsBytesSize("esql.external_source.max_buffer_bytes", ByteSizeValue.ofMb(100)))` to the SourceOperatorContext builder.

### Category 3: BlockFactory propagation (VERIFY for TP)

The file-based path creates FormatReaders at module init with a global `BlockFactory`:
- `DataSourceModule` (line 63): receives `BlockFactory blockFactory` in constructor
- `formatReaderRegistry.registerLazy(format, delegating, settings, blockFactory)` (line 143)
- This BlockFactory is shared across all queries

The connector path correctly uses the per-driver BlockFactory:
- `AsyncConnectorSourceOperatorFactory.get()` (line 66): `QueryRequest request = baseRequest.withBlockFactory(driverContext.blockFactory())`
- `FlightResultCursor` uses this per-driver BlockFactory

**Concern**: The file-based FormatReaders (Parquet, ORC, CSV, NDJSON) use the global BlockFactory, not the per-driver BlockFactory. This means:
1. Memory tracking goes against the global breaker (correct for accounting)
2. But blocks created on the background thread by the global BlockFactory may conflict with the LocalCircuitBreaker's single-thread assertion

For the synchronous file-based path (`ExternalSourceOperatorFactory.ExternalSourceOperator`), the FormatReader runs on the driver thread -- this is fine since the driver thread has exclusive access.

For the async connector path, `driverContext.blockFactory()` is correctly passed -- and `page.allowPassingToDifferentDriver()` is called in `ExternalSourceDrainUtils` (line 37) before passing pages to the buffer.

**Action item**: Verify that the ExternalSourceOperator (synchronous file path) creates blocks on the driver thread using the driver's BlockFactory. Currently it appears to use the global BlockFactory from module init, which means memory accounting hits the parent breaker directly (not the per-driver LocalCircuitBreaker). This works but bypasses the optimization layer.

### Minimal TP changes (ranked by impact)

1. **Add byte-based cap to AsyncExternalSourceBuffer** -- Prevents unbounded buffering. Add `maxBytes` parameter, track with `AtomicLong`, check in `waitForSpace()`.

2. **Add circuit breaker parameter to AsyncConnectorSourceOperatorFactory** -- Pass `DriverContext.breaker()` through to the buffer so it can trip the node-level breaker.

3. **Log row group sizes in Parquet** -- Add DEBUG logging of `currentRowGroup.getRowCount()` and estimated byte size to ParquetPageIterator, so operators have visibility.

4. **Limit RootAllocator in FlightConnector** -- Change `new RootAllocator()` to `new RootAllocator(maxFlightBytes)` with a configurable limit (e.g., 256MB). This prevents unbounded Arrow off-heap allocation. (Note: Flight is disabled for MVP TP, so this is low priority.)

### What does NOT need to change

- **BlockFactory itself** -- Already correctly tracks all ESQL block allocations. No changes needed.
- **LocalCircuitBreaker** -- Working correctly as an optimization layer.
- **ExternalSourceDrainUtils** -- Already calls `page.allowPassingToDifferentDriver()` before cross-thread transfer.
- **Page.ramBytesUsedByBlocks()** -- Exists and works. Just needs to be called by the buffer.
