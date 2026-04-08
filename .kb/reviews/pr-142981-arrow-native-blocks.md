# PR #142981 Review — Arrow-native Block & Vector Implementations

**PR**: https://github.com/elastic/elasticsearch/pull/142981
**Author**: swallez (Sylvain Wallez)
**Branch**: `esql/arrow-native`
**Stats**: +4,687 / -136 across 64 files
**Status**: Draft (author TODOs still open)
**Reviewed**: 2026-03-06

---

## Summary

This PR adds `Block` and `Vector` implementations backed by Apache Arrow's `ArrowBuf` to ESQL's compute engine. The key value proposition is **zero-copy interoperability**: data arriving from Arrow-native sources (e.g., Arrow Flight, future Arrow Dataset Parquet reader) can be used directly without copying into ESQL's array-backed blocks.

The PR also introduces a `libs/arrow` module that centralizes Arrow 18.3.0 dependencies previously scattered across `esql/arrow`, `esql-datasource-grpc`, and QA modules — a meaningful cleanup that also fixes a pre-existing Jackson version mismatch (2.19.2 hardcoded in grpc vs 2.15.0 in version.properties).

**Architecture**: New type hierarchy rooted in `AbstractArrowBufVector` and `AbstractArrowBufBlock`, both extending `AbstractNonThreadSafeRefCounted`. Six types each for Block and Vector — Long, Int, Double, Float (code-generated), Boolean and BytesRef (hand-written for bit-packing and variable-length). All sealed `permits` clauses on the existing Block/Vector interfaces are extended to allow the new types.

**Benchmarks** (from PR description): For dense double data, Arrow blocks are the fastest implementation — 1.614 ns/op sequential (vs 1.691 array, 1.523 vector) and 2.026 ns/op random (vs 2.272 array).

---

## Bugs

### 1. `AbstractArrowBufVector.filter()` writes to wrong output positions

**Severity**: Critical
**Location**: [`AbstractArrowBufVector.java:96-97`](https://github.com/elastic/elasticsearch/pull/142981/files#diff-AbstractArrowBufVector)

```java
for (int pos : positions) {
    buffer.setBytes((long) pos * size, valueBuffer, (long) pos * size, size);
}
```

The **destination offset** uses `pos * size` (the source position) instead of a sequential output index `i * size`. Filtering positions [0, 2, 4] writes to buffer offsets 0, 2, 4 instead of 0, 1, 2 — leaving gaps in the output. Affects Long, Int, Double, and Float vectors (Boolean and BytesRef have their own correct overrides).

**No test catches this** — `LongArrowBufTests` has no `testVectorFilter` method. Only Boolean and BytesRef have vector-level filter tests, and both have their own override that does it correctly.

### 2. Buffer leaks in `lookup().next()` on partial allocation failure

**Severity**: High
**Location**: [`AbstractArrowBufBlock.java` inner class `ArrowBufLookup.next()`](https://github.com/elastic/elasticsearch/pull/142981/files#diff-AbstractArrowBufBlock), and same pattern in `BooleanArrowBufBlock` and `BytesRefArrowBufBlock`

```java
ArrowBuf newValues = allocator.buffer(...);
ArrowBuf newOffsets = allocator.buffer(...);    // if this fails, newValues leaks
ArrowBuf newValidity = allocator.buffer(...);   // if this fails, newValues+newOffsets leak
```

Multi-buffer allocation is not wrapped in try-finally. If the second or third `allocator.buffer()` throws (OOM or circuit breaker), previously allocated buffers leak.

### 3. `doesHaveMultivaluedFields()` is imprecise

**Severity**: Medium
**Location**: [`AbstractArrowBufBlock.java:172-179`](https://github.com/elastic/elasticsearch/pull/142981/files#diff-AbstractArrowBufBlock)

Both `mayHaveMultivaluedFields()` and `doesHaveMultivaluedFields()` return `this.offsetBuffer != null`. The latter should be a stronger guarantee — scanning offset buffer to confirm any position actually has multiple values. The existing ESQL blocks do scan. This could cause incorrect optimization decisions in the query planner (operators that check `doesHaveMultivaluedFields()` to skip MV handling).

### 4. `FromFloat32` type mismatch in ArrowToBlockConverter

**Severity**: Medium
**Location**: [`ArrowToBlockConverter.java` — `FromFloat32` inner class](https://github.com/elastic/elasticsearch/pull/142981/files#diff-ArrowToBlockConverter)

The class Javadoc says "Arrow FLOAT4 → ESQL double (DoubleBlock)" but the implementation returns a `FloatArrowBufBlock` (implements `FloatBlock`, not `DoubleBlock`). ESQL's Analyzer widens float to double, so downstream code may expect `DoubleBlock` and get a `ClassCastException`.

---

## Risks

### 1. Arrow is now a hard dependency of compute

**Impact**: High (architectural, irreversible)

The `api project(':libs:arrow')` in compute's `build.gradle` + sealed `permits` clauses make Arrow a compile-time and runtime requirement for the entire compute engine. Every module depending on compute transitively depends on Arrow (~7.5 MB of JARs). The `requires transitive org.apache.arrow.memory.core` in `module-info.java` exposes Arrow's memory API to all downstream modules.

This is a one-way door — removing Arrow from the permits clause later would be a breaking change. Worth asking: should the Arrow block implementations live in a separate module (e.g., `compute-arrow`) with a registration mechanism, rather than being baked into compute's sealed interfaces?

### 2. No circuit breaker integration for externally-created blocks

**Impact**: High (production safety)

The `CircuitBreakerAllocationListener` only bridges Arrow's `allocator.buffer()` calls to ES's breaker. But when Arrow blocks are constructed from external `ArrowBuf`s (e.g., from Flight streams), the constructor calls `retain()` — which is invisible to the breaker. The breaker has no awareness of memory held by Arrow blocks wrapping external data.

The author acknowledges this: *"figure out the correct lifecycle for the Arrow allocator and its connection with the BlockFactory's circuit breaker"*. This must be resolved before any production use.

### 3. `allowPassingToDifferentDriver()` is a no-op

**Impact**: Medium (correctness under concurrency)

Both `AbstractArrowBufVector` and `AbstractArrowBufBlock` have empty `allowPassingToDifferentDriver()` with FIXME comments. In the existing ESQL implementation, this method adjusts circuit breaker accounting when blocks move between drivers. Since Arrow blocks don't register with the breaker at all (Risk #2), this is internally consistent but means Arrow blocks moving between drivers have zero memory tracking.

### 4. `AbstractNonThreadSafeRefCounted` made public

**Impact**: Low (API surface)

Changed from package-private to `public` to allow the Arrow implementations (in a different package) to extend it. This is a one-way API widening — once public, it can't be made package-private without breaking downstream code.

### 5. `--add-opens=java.base/java.nio=ALL-UNNAMED` required

**Impact**: Low (currently tests only)

Arrow's `arrow-memory-unsafe` uses `sun.misc.Unsafe` for direct ByteBuffer access. Currently only needed in tests and benchmarks. In production, violations are suppressed via `thirdPartyAudit.ignoreViolations`. Worth confirming this works correctly in all deployment modes (Docker, serverless).

---

## Gaps

### Functional Gaps (acknowledged in PR TODOs)

1. **BytesRef null+MV incomplete** — `BytesRefArrowBufBlock` cannot handle null values in multivalued entries
2. **No builders** — Arrow blocks can only wrap existing ArrowBufs, not be built incrementally
3. **No `toArrow()`** — Cannot convert Arrow blocks back to Arrow vectors (needed for Flight output)
4. **No dedicated serialization** — Falls back to slow value-by-value `writeTo` path
5. **Vector `keepMask()` and `lookup()` throw `UnsupportedOperationException`** — will crash if these code paths are reached with Arrow vectors
6. **`GroupingAggregatorFunction` rejects Arrow IntBlocks** — `default` case throws `IllegalStateException`, meaning aggregations fail if group IDs arrive in Arrow format
7. **Missing `equals()`/`hashCode()`** on generated types (Long, Int, Double, Float) — inherit from `Object`, will fail in any equality comparison

### Test Gaps

1. **No `testVectorFilter` for Long/Int/Double/Float** — the filter bug (Bug #1) is uncaught
2. **No dedicated tests for Int, Double, Float Arrow blocks** — relies on code generation being correct
3. **No integration tests** — Arrow blocks never flow through ESQL operators (eval, filter, aggregation, TopN)
4. **No empty-block edge case tests** (zero positions)
5. **No serialization tests** (`writeTo` path)
6. **`NoopCircuitBreaker` in tests** — ES-side memory accounting is not validated, only Arrow-side leak detection
7. **Benchmarks only cover `double/arrow`** — no comparative data for other types

---

## Positive Aspects

1. **Zero-copy design is sound** — `retain()` on construction, `release()` on close, enabling buffer sharing between Arrow sources and ESQL blocks without copying
2. **Dependency consolidation** — 3 scattered locations → 1 central `libs/arrow` module. Jackson version conflict fixed as a side effect
3. **Boolean bit-packing correct** — properly handles Arrow's 1-bit-per-value encoding with LSB-first ordering
4. **BytesRef dual-offset architecture** — correctly manages Arrow's variable-length offsets alongside ESQL's multi-value position offsets
5. **`CircuitBreakerAllocationListener` design** — the `onPreAllocation()` → `addEstimateBytesAndMaybeBreak()` bridge is the right pattern, just needs to be applied consistently
6. **Comprehensive test coverage for covered types** — ~2,200 lines testing single-valued, multi-valued, nulls, filter, keepMask, expand, lookup, toMask
7. **Code generation via StringTemplates** — ensures consistent implementation across numeric types
8. **Benchmark methodology is fair** — same data, same interface, same traversal patterns

---

## Specific Feedback

### Should Arrow be a hard dependency of compute?

The sealed `permits` approach is the simplest integration but creates an irreversible coupling. An alternative: keep the Arrow blocks in a separate module, use a service-loader or registry pattern for the block constructors, and have the sealed interfaces permit a generic "external" implementation class. This would keep compute Arrow-free for deployments that don't need it.

Counterargument: the benchmarks show Arrow blocks are the fastest implementation even for non-Arrow data. If the long-term direction is Arrow-everywhere, making it a hard dependency now avoids the complexity of an indirection layer.

### Circuit breaker integration needs a clear design

The current state is: `CircuitBreakerAllocationListener` exists and works for buffers allocated through a listener-equipped allocator, but blocks wrapping external ArrowBufs bypass the breaker entirely. Two paths forward:

1. **Constructor-side registration**: When an Arrow block is constructed, calculate the memory footprint from the ArrowBuf sizes and register with the breaker. On close, deregister. Simple, correct, doesn't require the source allocator to have the listener.
2. **Allocator-side enforcement**: Require all ArrowBufs entering ESQL to be allocated through a breaker-aware allocator. More principled but harder to enforce at the boundary.

Option 1 is probably the pragmatic choice. The memory is already allocated; the breaker just needs to know about it so it can reject new allocations if the node is under pressure.

### The filter bug fix is trivial

In `AbstractArrowBufVector.filter()`, change:
```java
for (int pos : positions) {
    buffer.setBytes((long) pos * size, valueBuffer, (long) pos * size, size);
}
```
to:
```java
for (int i = 0; i < positions.length; i++) {
    buffer.setBytes((long) i * size, valueBuffer, (long) positions[i] * size, size);
}
```
And add `testVectorFilter` to `LongArrowBufTests`.

### BytesRef null+MV is the hardest remaining problem

The author's TODO about BytesRef null values in multivalued entries is the most complex remaining work. The challenge is that Arrow's `ListVector<VarCharVector>` stores nulls at the list level (a null list entry) and at the value level (a null string within a list), while ESQL's `BytesRefBlock` has a single validity bitmap that covers positions. The offset mapping between these two representations when both nulls and multi-values are present is non-trivial.

---

## Summary Verdict

Good foundational work. The zero-copy architecture is sound and the performance results are real. The PR is correctly marked as draft — the circuit breaker gap, the filter bug, and the BytesRef null+MV limitation must be resolved before merge. The architectural question of whether Arrow should be a hard dependency of compute deserves explicit team alignment before this lands.
