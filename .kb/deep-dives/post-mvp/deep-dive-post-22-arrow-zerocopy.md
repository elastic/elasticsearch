# Deep Dive #22: Zero-Copy Arrow-to-ESQL Conversion

## Executive Summary

Arrow-to-ESQL conversion currently copies every value element-by-element through Builders. For fixed-width types (int, long, double), this is a memcpy that could be eliminated entirely. For variable-width types (VarChar, VarBinary), the situation is harder due to different internal representations. The biggest blocker is that ESQL's `Vector` interfaces are **sealed**, meaning new implementations require modifying the code generator templates and the sealed permits list. This is achievable but non-trivial -- it requires changes to the compute module itself, not just the arrow module.

**Verdict: Feasible for fixed-width types with moderate effort. Hard but possible for variable-width types. Requires compute module changes.**

---

## 1. ArrowToBlockConverter: Current Implementation

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowToBlockConverter.java`

### Conversion Pattern (all types follow the same pattern)

Every converter:
1. Casts the `FieldVector` to the concrete Arrow type
2. Gets `valueCount` from the vector
3. Creates a Builder via `BlockFactory` (e.g., `factory.newIntBlockBuilder(valueCount)`)
4. Iterates position-by-position checking `isNull(i)` and calling `builder.appendXxx(vector.get(i))`
5. Returns `builder.build()`

Example from `FromInt32` (line 159-176):
```java
public Block convert(FieldVector vector, BlockFactory factory) {
    IntVector intVector = (IntVector) vector;
    int valueCount = intVector.getValueCount();
    try (IntBlock.Builder builder = factory.newIntBlockBuilder(valueCount)) {
        for (int i = 0; i < valueCount; i++) {
            if (intVector.isNull(i)) {
                builder.appendNull();
            } else {
                builder.appendInt(intVector.get(i));
            }
        }
        return builder.build();
    }
}
```

### Types Handled

| Arrow Type | Arrow Class | ESQL Block | Converter | Notes |
|---|---|---|---|---|
| FLOAT4 | Float4Vector | DoubleBlock | FromFloat32 | Widens float->double, can't zero-copy |
| FLOAT8 | Float8Vector | DoubleBlock | FromFloat64 | Direct match, zero-copy candidate |
| BIGINT | BigIntVector | LongBlock | FromInt64 | Direct match, zero-copy candidate |
| INT | IntVector | IntBlock | FromInt32 | Direct match, zero-copy candidate |
| BIT | BitVector | BooleanBlock | FromBoolean | Bit-packed vs byte-per-value, can't zero-copy |
| VARCHAR | VarCharVector | BytesRefBlock | FromVarChar | Different internal format, hard |
| VARBINARY | VarBinaryVector | BytesRefBlock | FromVarBinary | Different internal format, hard |
| TIMESTAMPMICRO | TimeStampMicroVector | LongBlock | FromTimestampMicro | Requires micros->millis division, can't zero-copy |
| TIMESTAMPMICROTZ | TimeStampMicroTZVector | LongBlock | FromTimestampMicroTZ | Same micros->millis issue |

### Key Observation

The conversion is **element-by-element**, not batch-oriented. Each element goes through: Arrow null check -> Arrow get -> ESQL Builder append. For a 10,000-row batch of longs, this means 10,000 calls to `isNull()`, 10,000 calls to `get()`, and 10,000 calls to `appendLong()`.

### Second Conversion Site: FlightTypeMapping

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightTypeMapping.java`

This is a duplicate implementation (lines 54-124) that uses the same element-by-element pattern. The Flight connector uses `FlightTypeMapping.toBlock()` directly instead of `ArrowToBlockConverter`. Any zero-copy optimization would need to address both sites (or unify them).

---

## 2. ESQL Block and Vector Internal Storage

### Vector Implementations (all in compute module, all generated)

**IntArrayVector** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/data/IntArrayVector.java`):
```java
final class IntArrayVector extends AbstractVector implements IntVector {
    private final int[] values;           // <=== plain Java array

    public int getInt(int position) {
        return values[position];          // <=== direct array access
    }
}
```

**LongArrayVector** — same pattern: `private final long[] values;`
**DoubleArrayVector** — same pattern: `private final double[] values;`
**BooleanArrayVector** — stores `boolean[] values` (one byte per boolean, not bit-packed)

**BytesRefArrayVector** — different:
```java
final class BytesRefArrayVector extends AbstractVector implements BytesRefVector {
    private final BytesRefArray values;   // <=== Elasticsearch's BytesRefArray, NOT a plain array

    public BytesRef getBytesRef(int position, BytesRef dest) {
        return values.get(position, dest);
    }
}
```

### BigArray Variants

For large blocks, there are also `IntBigArrayVector`, `LongBigArrayVector`, etc. that wrap `IntArray`, `LongArray` (Elasticsearch's paginated big arrays using `sun.misc.Unsafe`-backed off-heap or recycled heap pages).

### Key Insight: Plain Java Arrays

For fixed-width types (int, long, double), ESQL vectors are just thin wrappers around **plain Java arrays**. This is the perfect match for zero-copy: if we can extract the underlying `int[]`/`long[]`/`double[]` from an Arrow vector, we can pass it directly to `new IntArrayVector(values, positionCount, blockFactory)`.

---

## 3. Arrow FieldVector Memory Layout

### Fixed-Width Vectors (IntVector, BigIntVector, Float8Vector)

Arrow vectors store data in two `ArrowBuf` buffers:
- **Validity buffer**: Bit-packed null bitmap (1 bit per value; 1 = valid, 0 = null)
- **Data buffer**: Contiguous array of fixed-width values in little-endian byte order

An Arrow `IntVector.get(i)` reads 4 bytes from `dataBuffer` at offset `i * 4`. An Arrow `BigIntVector.get(i)` reads 8 bytes from `dataBuffer` at offset `i * 8`.

**Memory model**: `ArrowBuf` wraps a `long` address (off-heap via `sun.misc.Unsafe`) with reference counting through Arrow's `AllocationManager`. Arrow vectors use **off-heap memory** (via `RootAllocator` -> `sun.misc.Unsafe`), NOT Java arrays.

### Variable-Width Vectors (VarCharVector, VarBinaryVector)

Three buffers:
- **Validity buffer**: Bit-packed null bitmap
- **Offset buffer**: `int[]` array of byte offsets (length = valueCount + 1)
- **Data buffer**: Contiguous byte data

### BitVector (Boolean)

One buffer plus validity:
- **Validity buffer**: Null bitmap
- **Data buffer**: Bit-packed boolean values (1 bit per value)

### Critical Difference: Off-Heap vs On-Heap

Arrow vectors use **off-heap memory** (`ArrowBuf` wraps a native memory address). ESQL vectors use **on-heap Java arrays** (`int[]`, `long[]`, `double[]`). This is the fundamental incompatibility that must be bridged.

**However**, Arrow's `IntVector.get(i)` ultimately does `MemoryUtil.UNSAFE.getInt(address + offset)` which reads from the native buffer. We can extract the same data as a Java array via `ArrowBuf.nioBuffer()` or by bulk-copying from the ArrowBuf into a Java array.

---

## 4. Block Ownership and Lifecycle

### Reference Counting

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/AbstractNonThreadSafeRefCounted.java`

All vectors and blocks extend `AbstractNonThreadSafeRefCounted` which maintains a simple `int references` counter (starts at 1). When it reaches 0, `closeInternal()` is called.

### Circuit Breaker Integration

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/AbstractVector.java` (line 38-40):
```java
protected void closeInternal() {
    blockFactory.adjustBreaker(-ramBytesUsed());
}
```

When a vector is created, `BlockFactory.adjustBreaker(+bytes)` is called. When released, `adjustBreaker(-bytes)` is called. This is the circuit breaker contract.

### Block Construction via Factory

`BlockFactory` (lines 238-242 in BlockFactory.java) creates vectors:
```java
public IntVector newIntArrayVector(int[] values, int positionCount, long preAdjustedBytes) {
    var b = new IntArrayVector(values, positionCount, this);
    adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
    return b;
}
```

The factory provides the array, registers with the breaker, and the vector takes ownership.

### Lifecycle Implication for Zero-Copy

If an Arrow vector's data were wrapped directly, the Arrow vector **must not be freed** while the ESQL Block is in use. This requires one of:
1. **Transfer ownership**: Copy the Arrow data buffer into a Java array (not zero-copy, but efficient bulk copy)
2. **Reference pinning**: Hold a reference to the Arrow vector, releasing it in `closeInternal()`
3. **Shared memory**: Use a custom `closeInternal()` that decrements the ArrowBuf's refcount

---

## 5. Page and Block Contract

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/Page.java`

Pages are simply arrays of Blocks:
```java
public final class Page implements Writeable, Releasable {
    private final Block[] blocks;
    private final int positionCount;
}
```

Page makes these assumptions about blocks:
- All blocks have the same `positionCount`
- Blocks are reference counted (Page calls `incRef()` when sharing, `close()`/`releaseBlocks()` when done)
- Blocks must not be already released when constructing a Page
- Blocks can be serialized via `writeTypedBlock`/`readTypedBlock`

**No assumption about backing memory**: Pages don't know or care whether a block uses Java arrays, BigArrays, or anything else. The only contract is through the Block/Vector interfaces.

---

## 6. Existing Wrapper Patterns

### OrdinalBytesRefVector: Composition over External Data

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/OrdinalBytesRefVector.java`

This is a precedent for a vector that wraps other vectors without owning their raw data:
```java
public final class OrdinalBytesRefVector extends AbstractNonThreadSafeRefCounted implements BytesRefVector {
    private final IntVector ordinals;
    private final BytesRefVector bytes;

    protected void closeInternal() {
        Releasables.close(ordinals, bytes);
    }
}
```

### IntBigArrayVector: Wrapping External Memory

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/data/IntBigArrayVector.java`

Wraps `IntArray` (Elasticsearch's paginated big array) and releases it on close:
```java
public final class IntBigArrayVector extends AbstractVector implements IntVector {
    private final IntArray values;

    public void closeInternal() {
        values.close();  // Releases the BigArray pages
    }
}
```

The javadoc says: "Does not take ownership of the array and does not adjust circuit breakers to account for it." The circuit breaker adjustment happens externally.

### AllocationManagerShim: Arrow Memory is Disabled in Production

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java`

In production, Arrow's memory allocator is **completely disabled** via a shim that throws on any allocation attempt. This means the arrow module currently never uses Arrow's memory management at all -- it only uses Arrow's serialization types (for response formatting in `ArrowResponse`).

For zero-copy to work with Arrow vectors that **do** use Arrow's memory (as in Flight/Iceberg), the shim would need to be adjusted or the code path would need to bypass it.

---

## 7. ArrowReader Usage in Iceberg

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java`

### Current Flow (lines 127-174)

1. Creates an Iceberg `ArrowReader` with a `RootAllocator` (real Arrow allocator, not the shim)
2. `ArrowReader.open(tasks)` returns `CloseableIterator<ColumnarBatch>`
3. `ColumnarBatch` contains `ColumnVector` wrappers, each holding a `FieldVector`
4. `ColumnarBatchToVectorSchemaRootIterable.convertColumnarBatchToVectorSchemaRoot()` extracts `FieldVector`s and wraps them in a `VectorSchemaRoot`

```java
private VectorSchemaRoot convertColumnarBatchToVectorSchemaRoot(ColumnarBatch batch) {
    List<FieldVector> fieldVectors = new ArrayList<>(numColumns);
    for (int col = 0; col < numColumns; col++) {
        ColumnVector columnVector = batch.column(col);
        FieldVector fieldVector = columnVector.getFieldVector();
        fieldVectors.add(fieldVector);
    }
    return new VectorSchemaRoot(fieldVectors);
}
```

**Critical Note**: The `get()` method on the `SourceOperator` throws `UnsupportedOperationException` (line 103). This code is **not yet wired into the compute pipeline**. The factory exists but data reading is not implemented end-to-end for Iceberg.

### What Iceberg's ArrowReader Produces

Iceberg's `ArrowReader` produces `ColumnarBatch` objects. Each `ColumnVector` in the batch wraps an Arrow `FieldVector` (e.g., `IntVector`, `BigIntVector`, `VarCharVector`). These FieldVectors use a **real** `RootAllocator` (line 167: `new RootAllocator(Long.MAX_VALUE)`), so their data buffers are off-heap Arrow memory.

### Conversion to Pages: Not Yet Implemented

There is no Page conversion in the Iceberg code. The `IcebergSourceOperatorFactory` produces `VectorSchemaRoot` objects but never converts them to ESQL Pages. When this is implemented, it will need the same Arrow-to-Block conversion that `ArrowToBlockConverter` provides.

---

## 8. Feasibility Analysis

### Approach A: Bulk Array Copy (Practical, Moderate Effort)

Instead of element-by-element copying, extract the raw data from Arrow's `ArrowBuf` as a bulk array copy:

```java
// For Arrow IntVector -> ESQL IntBlock
public Block convert(FieldVector vector, BlockFactory factory) {
    org.apache.arrow.vector.IntVector arrowVec = (org.apache.arrow.vector.IntVector) vector;
    int valueCount = arrowVec.getValueCount();

    if (hasNoNulls(arrowVec, valueCount)) {
        // Fast path: bulk copy into Java array
        int[] values = new int[valueCount];
        ArrowBuf dataBuffer = arrowVec.getDataBuffer();
        for (int i = 0; i < valueCount; i++) {
            values[i] = dataBuffer.getInt((long) i * Integer.BYTES);
        }
        return factory.newIntArrayVector(values, valueCount).asBlock();
    }
    // Slow path: element-by-element with null handling (existing code)
    ...
}
```

**Pros**: No compute module changes needed. Works with existing `IntArrayVector`.
**Cons**: Still copies all data (bulk copy, not zero-copy). But eliminates per-element overhead.
**Estimated speedup**: 3-5x for the conversion step (eliminates null check per element, Builder overhead, and enables potential JVM loop vectorization).

### Approach B: True Zero-Copy Wrapper (High Effort, Maximum Performance)

Create new Vector implementations that wrap Arrow's `ArrowBuf` directly:

```java
// New class: ArrowIntVector implements IntVector
final class ArrowIntVector extends AbstractVector implements IntVector {
    private final ArrowBuf dataBuffer;
    private final ArrowBuf validityBuffer;  // may be null for no-nulls case
    private final int valueCount;
    private final Releasable arrowOwner;    // holds the Arrow vector alive

    public int getInt(int position) {
        return dataBuffer.getInt((long) position * Integer.BYTES);
    }

    protected void closeInternal() {
        arrowOwner.close();  // releases the Arrow vector
        blockFactory.adjustBreaker(-ramBytesUsed());
    }
}
```

#### Blockers for Approach B

1. **Sealed Interfaces**: `IntVector` is declared as:
   ```java
   public sealed interface IntVector extends Vector
       permits ConstantIntVector, IntArrayVector, IntBigArrayVector, IntRangeVector, ConstantNullVector
   ```
   Adding `ArrowIntVector` requires modifying the template (`X-Vector.java.st`) and regenerating. Same for `LongVector`, `DoubleVector`, etc.

2. **Serialization**: `IntVector.writeTo()` uses `instanceof` checks:
   ```java
   } else if (this instanceof IntArrayVector v) {
       out.writeByte(SERIALIZE_VECTOR_ARRAY);
       v.writeArrayVector(positions, out);
   } else if (this instanceof IntBigArrayVector v) {
       ...
   } else {
       out.writeByte(SERIALIZE_VECTOR_VALUES);
       writeValues(this, positions, out);  // generic fallback, works but slower
   }
   ```
   A new `ArrowIntVector` would fall through to the generic `writeValues` path. This is correct but suboptimal.

3. **Module Boundaries**: The Arrow vector types live in the `esql/arrow` module. The Vector interfaces live in `esql/compute`. The `esql/compute` module does NOT depend on Arrow. Creating `ArrowIntVector` in the `compute` module would introduce an unwanted Arrow dependency. Creating it in the `arrow` module is impossible because the sealed interface is in `compute`.

4. **Memory Model Mismatch**: Arrow uses off-heap memory; ESQL expects on-heap Java arrays. `ArrowBuf.getInt()` does an unsafe memory read, which is slower than a Java array access due to missed JVM optimizations (no bounds check elimination, no loop vectorization).

5. **Endianness**: Arrow uses **little-endian** byte order. Java uses big-endian for its primitives. However, `ArrowBuf.getInt()` already handles the endianness conversion internally (Arrow Java always stores LE). When using `Unsafe.getInt(address)` on x86/ARM, this is native order (LE), so there's no conversion cost on modern hardware.

6. **AllocationManagerShim**: In production, Arrow's allocator is disabled. For Flight connector data (which arrives with real Arrow allocators), this works. For Iceberg, the factory already creates a `RootAllocator`. But the shim needs to not interfere.

### Approach C: Hybrid -- New Vector in Compute Module (Best Practical Approach)

Create a `ForeignMemoryIntVector` (or similar) in the compute module that wraps any foreign memory source (not Arrow-specific):

```java
// In compute module -- no Arrow dependency
final class ForeignMemoryIntVector extends AbstractVector implements IntVector {
    private final int[] values;      // Java array extracted from foreign source
    private final Releasable owner;  // releases foreign memory when done

    // Constructor takes pre-extracted Java array + owner
    ForeignMemoryIntVector(int[] values, int positionCount, BlockFactory factory, Releasable owner) {
        super(positionCount, factory);
        this.values = values;
        this.owner = owner;
    }

    public int getInt(int position) {
        return values[position];
    }

    protected void closeInternal() {
        if (owner != null) owner.close();
        blockFactory.adjustBreaker(-ramBytesUsed());
    }
}
```

Wait -- this is just `IntArrayVector` with an extra `Releasable`. The simpler approach: **add an optional `Releasable` to `IntArrayVector`**.

Actually, the simplest approach is **Approach A with optimization**: bulk-copy the Arrow data into a Java array, create a standard `IntArrayVector`, and let the normal lifecycle manage it. The Arrow vector is released after the copy. No compute module changes needed.

---

## 9. Type-by-Type Zero-Copy Assessment

| Type | Zero-Copy Possible? | Reason |
|---|---|---|
| Arrow INT -> ESQL IntBlock | **Bulk copy only** | Arrow off-heap (ArrowBuf) vs ESQL on-heap (int[]). Can bulk-copy via `ArrowBuf.nioBuffer()`. |
| Arrow BIGINT -> ESQL LongBlock | **Bulk copy only** | Same as INT. |
| Arrow FLOAT8 -> ESQL DoubleBlock | **Bulk copy only** | Same as INT. |
| Arrow FLOAT4 -> ESQL DoubleBlock | **No** | Requires float-to-double widening per element. |
| Arrow BIT -> ESQL BooleanBlock | **No** | Arrow: 1 bit per value. ESQL: 1 byte (boolean) per value. Must unpack. |
| Arrow VARCHAR -> ESQL BytesRefBlock | **No** | Arrow: offset+data buffer pair. ESQL: `BytesRefArray` (Elasticsearch's custom format). Completely different layouts. |
| Arrow VARBINARY -> ESQL BytesRefBlock | **No** | Same as VARCHAR. |
| Arrow TIMESTAMPMICRO -> ESQL LongBlock | **No** | Requires micros / 1000 per element. |
| Arrow TIMESTAMPMICROTZ -> ESQL LongBlock | **No** | Same as TIMESTAMPMICRO. |

### True Zero-Copy (Wrapping ArrowBuf Directly)

Would require:
1. Adding Arrow-wrapping Vector impls to the sealed interface permits list
2. Dealing with off-heap memory reads (slightly slower per-access than Java arrays)
3. Lifecycle management (Arrow vector must live as long as ESQL block)
4. Breaking the module boundary (compute would need Arrow dependency, OR arrow module would need special access)

**Not recommended** for V1. The per-access overhead of `ArrowBuf.getInt()` vs `values[position]` means individual element access is slower. The win from zero-copy is only avoiding the one-time bulk copy, which is fast for typical batch sizes (5K-50K rows).

---

## 10. Recommended Implementation Strategy

### Phase 1: Optimized Bulk Copy (Low effort, high impact)

Modify `ArrowToBlockConverter` to:

1. **Check for no-nulls fast path**: If `vector.getNullCount() == 0`, skip per-element null checks entirely.

2. **Bulk-copy into Java array**: Use `ArrowBuf.getBytes()` or `ArrowBuf.nioBuffer()` + `IntBuffer.get(int[])` to copy the entire data buffer into a Java array in one operation.

3. **Use factory methods directly**: Call `factory.newIntArrayVector(values, count).asBlock()` instead of going through Builder.

Example optimized converter for INT:
```java
public Block convert(FieldVector vector, BlockFactory factory) {
    org.apache.arrow.vector.IntVector arrowVec = (org.apache.arrow.vector.IntVector) vector;
    int valueCount = arrowVec.getValueCount();

    if (valueCount == 0) {
        return factory.newConstantNullBlock(0);
    }

    if (arrowVec.getNullCount() == 0) {
        // Fast path: no nulls, bulk copy
        long preAdjusted = factory.preAdjustBreakerForInt(valueCount);
        int[] values = new int[valueCount];
        ArrowBuf dataBuf = arrowVec.getDataBuffer();
        // Bulk read from ArrowBuf into Java array
        dataBuf.getBytes(0, values);  // <-- check if this API exists; if not, use nio
        return factory.newIntArrayVector(values, valueCount, preAdjusted).asBlock();
    }

    // Slow path with nulls: use builder (existing logic)
    try (IntBlock.Builder builder = factory.newIntBlockBuilder(valueCount)) {
        for (int i = 0; i < valueCount; i++) {
            if (arrowVec.isNull(i)) {
                builder.appendNull();
            } else {
                builder.appendInt(arrowVec.get(i));
            }
        }
        return builder.build();
    }
}
```

**Note on ArrowBuf API**: `ArrowBuf` does not have a direct `getBytes(offset, int[])` method. The approach would be:
```java
java.nio.ByteBuffer nioBuf = dataBuf.nioBuffer(0, (long) valueCount * Integer.BYTES);
nioBuf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
nioBuf.asIntBuffer().get(values);
```

### Phase 2: Unify Conversion Sites

Merge `FlightTypeMapping.toBlock()` to use `ArrowToBlockConverter`, eliminating the duplicate code.

### Phase 3 (Post-MVP): True Zero-Copy

If profiling shows bulk-copy is still a bottleneck:
1. Add `ArrowIntVector` etc. to the sealed permits list in the code generator
2. Create wrapper vectors in a new shared module
3. Use `ArrowBuf` directly with lifecycle management

**Estimated effort**: Phase 1 is ~1 day. Phase 2 is ~0.5 day. Phase 3 is ~2 weeks.

---

## 11. Key Files Reference

| File | Purpose |
|---|---|
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowToBlockConverter.java` | Current element-by-element Arrow->Block converter (9 type converters) |
| `x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightTypeMapping.java` | Duplicate Arrow->Block converter for Flight (6 type converters) |
| `x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/data/IntArrayVector.java` | ESQL IntVector backed by `int[]` |
| `x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/data/LongArrayVector.java` | ESQL LongVector backed by `long[]` |
| `x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/data/DoubleArrayVector.java` | ESQL DoubleVector backed by `double[]` |
| `x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/data/BooleanArrayVector.java` | ESQL BooleanVector backed by `boolean[]` |
| `x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/data/BytesRefArrayVector.java` | ESQL BytesRefVector backed by `BytesRefArray` |
| `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/X-Vector.java.st` | Code generator template for sealed Vector interfaces |
| `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/Block.java` | Block interface (RefCounted, Releasable) |
| `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/Page.java` | Page = array of Blocks, no memory assumptions |
| `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/BlockFactory.java` | Factory creates vectors, manages circuit breaker |
| `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/AbstractVector.java` | Base vector: positionCount, blockFactory, closeInternal adjusts breaker |
| `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/AbstractNonThreadSafeRefCounted.java` | Reference counting (int counter, not atomic) |
| `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/OrdinalBytesRefVector.java` | Precedent for composition-based vector wrapping |
| `x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/data/IntBigArrayVector.java` | Precedent for wrapping external memory (IntArray) |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/AllocationManagerShim.java` | Disables Arrow allocator in production |
| `x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/BlockConverter.java` | Block->Arrow converter (reverse direction, has TODO comments about bulk copy) |
| `x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java` | Iceberg ArrowReader usage (not yet wired to compute) |
| `x-pack/plugin/esql/arrow/build.gradle` | Arrow version: 18.3.0 |

---

## 12. Interesting TODOs Already in Codebase

In `BlockConverter.java` (the reverse direction, Block->Arrow), there are several TODO comments suggesting the authors already considered bulk memory operations:

- Line 104: `// TODO could we "just" get the memory of the array and dump it?` (AsFloat64)
- Line 142: `// TODO could we "just" get the memory of the array and dump it?` (AsInt32)
- Line 183: `// TODO could we "just" get the memory of the array and dump it?` (AsInt64)
- Line 268: `// TODO could we "just" get the memory of the array and dump it?` (BytesRefConverter offsets)
- Line 291: `// TODO could we "just" get the memory of the array and dump it?` (BytesRefConverter data)

This confirms the team has been thinking about bulk-copy optimizations in both directions.

---

## 13. Summary of Incompatibilities

| Dimension | Arrow | ESQL | Impact |
|---|---|---|---|
| Memory location | Off-heap (ArrowBuf/Unsafe) | On-heap (Java arrays) | Must copy; direct wrapping slower per-access |
| Null representation | Validity bitmap (1 bit/value) | `BitSet nulls` in Block, or no nulls in Vector | Must convert bitmap format |
| Boolean encoding | Bit-packed (1 bit/value) | `boolean[]` (1 byte/value) | Must unpack, no zero-copy possible |
| String/binary | Offset buffer + data buffer | `BytesRefArray` (custom format) | Different formats, no zero-copy possible |
| Timestamp precision | Microseconds | Milliseconds | Requires division per element |
| Float precision | Float32 supported | No Float32 (only Double) | Requires widening per element |
| Interface extensibility | N/A | Sealed interfaces | Must modify code generator to add implementations |
| Module dependency | arrow module | compute module | compute must not depend on Arrow |
