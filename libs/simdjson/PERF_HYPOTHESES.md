# simdjson Performance Hypotheses

Benchmark results on AWS EC2, `nativeStage1=true`, JDK 26.0.1.

## Benchmark Results — commit `36e3a656ea1` (H2+H3+B1+B4)

### x64 (AMD EPYC 9R14, 8 threads, AVX-512 / ICE_LAKE)

| Method         | clickbench_flat | otel_nested | small_sparse |
|----------------|-----------------|-------------|--------------|
| jacksonEncode  |  86.5 ops/s     | 365.3 ops/s |  863.3 ops/s |
| simdJsonEncode | 110.3 ops/s     | 443.4 ops/s | 1071.3 ops/s |

### ARM (Graviton, 4 threads, NEON)

| Method         | clickbench_flat | otel_nested | small_sparse |
|----------------|-----------------|-------------|--------------|
| jacksonEncode  |  31.7 ops/s     | 176.2 ops/s |  346.8 ops/s |
| simdJsonEncode |  54.7 ops/s     | 239.9 ops/s |  548.6 ops/s |

**Note:** Previous ARM results (48.7 / 237.3 / 445.9 for simdJsonEncode) were
collected with JDK 21. JDK 26 improves per-doc throughput by 10-20%.
The removed multi-doc `simdJsonBatchEncode` benchmark path is no longer applicable;
production encoding uses per-document `parseToScratch` / `commitScratchTo`.

### Document shapes

- **clickbench_flat**: ~2500-2800 bytes, ~100 fields, mostly numeric, flat structure
- **otel_nested**: ~700-900 bytes, ~20 fields, 3 levels of nesting, mix of types
- **small_sparse**: ~100-150 bytes, 6-7 fields, 3 rotating variants

## Running the benchmarks

### Prerequisites

1. Build the native stage1 library:
   ```bash
   cd libs/simdjson/native
   make local CLANG_CXX=clang++   # may need: sed -i 's/-fuse-ld=lld//g' Makefile
   mkdir -p release/linux-$(uname -m | sed 's/x86_64/x64/')
   cp build/linux-*/libes_simdjson.so release/linux-*/
   ```

2. Download async-profiler 4.5+ for flamegraph profiling:
   ```bash
   wget https://github.com/async-profiler/async-profiler/releases/download/v4.5/async-profiler-4.5-linux-$(uname -m).tar.gz
   tar xf async-profiler-4.5-linux-*.tar.gz
   ```

3. Set kernel profiling permissions:
   ```bash
   sudo sysctl -w kernel.perf_event_paranoid=1
   ```

### Running benchmarks

```bash
cd benchmarks
NATIVE_LIBS="$PWD/../libs/simdjson/native/release/linux-$(uname -m | sed 's/x86_64/x64/')"
PROF_LIB="$HOME/async-profiler-4.5-linux-*/lib/libasyncProfiler.so"

# CPU profiling (8 threads):
../gradlew --no-daemon run --args \
  'org.elasticsearch.benchmark.xcontent.SimdJsonParserBenchmark \
   -t 8 -wi 3 -i 5 \
   -prof async:output=flamegraph;dir=/tmp/bench;event=cpu;libPath='$PROF_LIB' \
   -jvmArgs -Des.nativelibs.path='$NATIVE_LIBS' \
   -rf json -rff /tmp/bench/bench_8t.json'

# Allocation profiling:
../gradlew --no-daemon run --args \
  'org.elasticsearch.benchmark.xcontent.SimdJsonParserBenchmark \
   -t 8 -wi 3 -i 5 \
   -prof async:output=flamegraph;dir=/tmp/bench;event=alloc;libPath='$PROF_LIB' \
   -jvmArgs -Des.nativelibs.path='$NATIVE_LIBS' \
   -rf json -rff /tmp/bench/bench_8t_alloc.json'

# Single method, disassembly:
../gradlew --no-daemon run --args \
  'org.elasticsearch.benchmark.xcontent.SimdJsonParserBenchmark.simdJsonEncode \
   -t 1 -wi 5 -i 3 -f 1 -p shape=clickbench_flat \
   -prof perfasm \
   -jvmArgs -Des.nativelibs.path='$NATIVE_LIBS' \
   -rf json -rff /tmp/bench/bench_perfasm.json'
```

### AWS instance access

```bash
# x64
ssh -i "~/.ssh/chegar-elastic.pem" ubuntu@ec2-98-81-2-141.compute-1.amazonaws.com
# ARM
ssh -i "~/.ssh/chegar-elastic.pem" ubuntu@ec2-3-238-29-35.compute-1.amazonaws.com
```

## Perfasm Results (single-threaded, x64, `simdJsonEncode`, `clickbench_flat`)

After-inlining method breakdown:

| %     | Method                                                    |
|-------|-----------------------------------------------------------|
| 28.03 | SimdJsonDirectWalker::resolveFieldName                    |
| 16.86 | EscfBatchBuilder::drainScratchValue                       |
| 16.42 | SimdJsonDirectWalker::walkObject                          |
| 15.04 | SimdJsonDirectWalker::handleNumber                        |
|  6.51 | libes_simdjson.so  stage1                                 |
|  3.03 | EscfEncoder::parseToScratch                               |
|  2.85 | EscfDocumentHandler::stringField                          |
|  2.59 | SimdJsonParserBenchmark::simdJsonEncode (benchmark harness)|
|  1.82 | StubRoutines::vectorizedMismatch_stub                     |
|  1.03 | StubRoutines::jint_disjoint_arraycopy_stub                |
|  0.52 | es_stage1_run (JNI wrapper)                               |
|  0.52 | StubRoutines::jbyte_disjoint_arraycopy_stub               |

Source distribution: 85.7% C2, 7.0% native, 3.6% runtime stubs, 1.5% kernel.

## Hypotheses

### H1: VarHandle guard dispatch overhead — DISPROVEN

`VarHandleGuards.guard_LI_J` appears in async-profiler flamegraphs at 6-13%,
but **this is a frame-attribution artifact, not actual overhead**.

Perfasm disassembly (commit `dd3b9c970e4`, x64 JDK 26) confirms that C2 fully
inlines the VarHandle dispatch chain:

```
VarHandleGuards::guard_LI_J -> ArrayHandle::get -> Unsafe::getLongUnaligned
```

The guard check (`checkAccessModeThenIsDirect`) and type comparison are
constant-folded away because the VarHandle is a `static final` field of a
concrete type (`VarHandleByteArrayAsLongs$ArrayHandle`). The emitted code is
just a raw unaligned load instruction.

**Endianness is irrelevant**: `ByteOrder.LITTLE_ENDIAN` vs `nativeOrder()` on
a little-endian platform produces **exactly the same** `VarHandle` instance.
Both evaluate to `new VarHandleByteArrayAsLongs.ArrayHandle(be=false)`. The
`be` field is read by `Unsafe.getLongUnaligned(ba, offset, be)` which is an
intrinsic — on x64 it emits a plain `MOV`, on ARM a `LDR`.

**True cost breakdown** for `resolveFieldName` (28.03% total per perfasm):
- `hashName` computation (wymix, readSmall, readLE8) — hash is the dominant cost
- `FrozenFieldNameTable$Frozen.lookup` — prefix8 + hash-probe loop
- `Preconditions.checkIndex` — bounds checking on every VarHandle access
- The 8-byte word scan loop itself is cheap; most time is in hash + lookup

### H2: Field name resolution dominates CPU (28%)

`resolveFieldName` at 28% is the single largest cost. The breakdown (from
perfasm inline traces) is:

1. **`hashName` (wymix + readSmall + readLE8)**: ~12-15%. Every field name
   gets a full wyhash computation. For len <= 8 (common case), this is
   1 readSmall + 1 wymix. The readSmall path does 3 byte reads + shifts for
   len < 4, or 2 INT_LE VarHandle reads for len 4-8.

2. **`FrozenFieldNameTable$Frozen.lookup`**: ~8-10%. After hashing, the
   probe loop reads `hashes[slot]`, compares, then does `readPrefix8` +
   `Arrays.mismatch`. The `vectorizedMismatch_stub` at 1.82% is from this.

3. **`Preconditions.checkIndex`**: ~3-5%. Bounds checks on every VarHandle
   access. There are 2 checkIndex calls in readSmall (for len 4-8), plus
   1 in readLE8.

**Potential fixes**:
- Merge scan + hash: `scanAndHash` already exists but is not used in
  `resolveFieldName`. Using it would avoid reading the field name bytes
  twice (once to find the closing quote, once to hash).
- For len <= 8, skip `Arrays.mismatch` in the lookup if prefix8 + len
  match (the 8-byte prefix fully covers the name).
- Pre-compute bounds for the inner loop to hoist checkIndex out.

### H2b: drainScratchValue is #2 hotspot (16.86%)

`EscfBatchBuilder::drainScratchValue` is the second most expensive method
after `resolveFieldName`. This is the ESCF column builder draining values
from the scratch buffer into column storage. This is outside the simdjson
parser itself and represents the cost of the ESCF encoding side.

**Potential fix**: This is likely memory-copy bound. Reducing the number of
intermediate copies or using bulk operations could help.

### H3: Number parsing overhead (13-15%) — IMPLEMENTED, marginal gain

`handleNumber` in the direct walker parsed digits one-at-a-time in a loop. For
`clickbench_flat` (~80 numeric fields), this was the #2 hotspot.

**Fix applied**: SWAR 8-digit integer parsing — read 8 bytes via `LONG_LE`, check
all are ASCII digits (`(t & 0xF0F0F0F0F0F0F0F0L) != 0`), convert 8 digits in
parallel using pair/quad widening (3 multiplies instead of 8 scalar `*10+` steps).
Falls back to byte-at-a-time for the remaining digits.

**Result (x64, 8-thread):**

| Method               | clickbench_flat  | otel_nested  | small_sparse  |
|----------------------|------------------|--------------|---------------|
| Before (H2)          | 107.8 ops/s      | 419.7 ops/s  |  988.7 ops/s  |
| After (H3 SWAR)      | 109.6 ops/s      | 430.1 ops/s  | 1054.4 ops/s  |
| Delta                | +1.7%            | +2.5%        | +6.6%         |

Modest improvement. `handleNumber` / `parse8Digits` no longer appears in the top-30
CPU hotspots in the flamegraph, confirming it's been effectively eliminated as a
bottleneck. The remaining CPU is dominated by field name resolution (~12%),
ESCF column building (~10%), and `commitScratchTo`/`drainScratchValue` (~8%).

### H4: BitIndexes.getAndAdvance (2-4%)

Structural index iteration shows up consistently. May be due to bounds checking
or cache misses on large index arrays.

**Potential fix**: Cache the position in a local variable to help the JIT keep it
in a register.

### H5: EscfDocumentHandler string/long field handling (7-8%)

Handler methods copy bytes for column builders. `stringField` at 7% on
clickbench copies string bytes to the column builder.

**Potential fix**: Pass byte[] + offset + length directly to column builders,
avoiding intermediate copies. For unescaped strings still in the input buffer,
a zero-copy reference could avoid copying entirely.

### H6: Batch commitScratchTo overhead (22-33%)

In batch mode, `commitScratchTo` is the #2 cost after the walker itself.

**Potential fix**: Batch multiple row commits, or defer commit to amortize the
cost.

### H7: ARM batch mode bottlenecked by native stage1 (30-46%)

ARM NEON processes 16 bytes per SIMD iteration vs x64 AVX-512 at 64 bytes. The
batch path runs stage1 over the entire batch buffer (~25MB for clickbench), making
stage1 disproportionately expensive on ARM.

This explains why `simdJsonEncode` (per-doc stage1) outperforms
`simdJsonBatchEncode` on ARM — single-doc only indexes ~2.5KB per document.

**Potential fix**: Reduce `CHUNK_BYTE_LIMIT` on ARM, or use per-doc stage1 on ARM
and batch stage1 only on x64. Consider a hybrid that detects SIMD width at
startup.

### H8: Field name table lookup chain (8-12% total)

`readPrefix8` + `Child.lookup` + `Arrays.mismatch` together form the field name
resolution cost. Better than the old `lookupName` (22%), but still significant.

**Potential fix**: For small stable field sets, a direct-mapped table using
first-4-byte index could skip hash computation entirely.

### H9: Object allocation pressure (2-9%)

`C2 Runtime new_instance_blob` at 2-9%. In simdjson, likely from
`EscfDocumentHandler` or per-doc objects. In Jackson, from parser + context
creation per document.

**Potential fix**: Pool or reuse handler/row objects across documents in a batch.

---

## Batch ARM Investigations (B-series)

Context: On ARM, `simdJsonBatchEncode` was consistently slower than
`simdJsonEncode` (per-doc), while on x64 batch was faster. Investigated
with H7 as the starting hypothesis.

### B1: Configurable CHUNK_BYTE_LIMIT — IMPLEMENTED

Made `CHUNK_BYTE_LIMIT` configurable via system property
`es.simdjson.chunk_byte_limit` (default 256KB). Allows testing smaller
chunk sizes (32KB, 64KB) to keep stage1 working set in L1 cache on ARM.

**Chunk size sweep** (`simdJsonBatchEncode` only, JDK 26, `-wi 2 -i 3`):

ARM (4 threads):

| Chunk | clickbench_flat | otel_nested | small_sparse |
|-------|-----------------|-------------|--------------|
| 32KB  | 40.7            | 195.0       | 517.1        |
| 64KB  | 39.3            | 194.7       | **556.6**    |
| 128KB | **47.3**        | 183.0       | 536.9        |
| 256KB | 46.5            | 189.0       | 546.4        |
| 512KB | 43.9            | 175.8       | 530.3        |

x64 (8 threads):

| Chunk | clickbench_flat | otel_nested | small_sparse |
|-------|-----------------|-------------|--------------|
| 32KB  | 97.1            | 453.5       | 1213.7       |
| 64KB  | 96.1            | 476.2       | **1275.8**   |
| 128KB | **107.3**       | 476.7       | 1269.1       |
| 256KB | 105.7           | **489.5**   | 1268.5       |
| 512KB | **107.4**       | 483.3       | 1230.4       |

**Findings**: Effect is modest (~5–10% swing). Smaller chunks (32/64KB) do
*not* help ARM batch on large documents — `clickbench_flat` is worst at
64KB and best at 128KB. Very small chunks add stage1 invocation overhead.
256KB default is reasonable; 128KB may marginally help ARM `clickbench_flat`
(+2%). No chunk size closes the ARM batch-vs-per-doc gap on
`clickbench_flat` or `otel_nested`.

**Status**: Merged. Default remains 256KB.

### B2: Move offset-add from native to Java — REVERTED

The native `es_stage1_run` has an offset-add loop: when `offset != 0`,
each structural index is incremented by `offset` in a scalar C++ loop
instead of using `memcpy`. Hypothesis: moving this to Java would let JIT
auto-vectorize it.

**Result**: Massive regression on ARM (-39% to -53% for batch). JDK 21
did not vectorize the Java loop effectively. The native compiler handles
it better. Reverted.

### B4: Right-size BitIndexes capacity — IMPLEMENTED

Reduced initial `BitIndexes` capacity from `Math.max(capacity, 1024)` to
`Math.max(capacity / 4, 1024)`. The old formula over-allocated (1 index
per byte), when actual structural density is ~1 per 4 bytes. This reduces
cache footprint and avoids unnecessary resizing.

**Status**: Merged.

### JDK 26 Impact on ARM

Switching from JDK 21 to JDK 26 on ARM produced significant improvements:

| Method              | JDK 21 (ops/s) | JDK 26 (ops/s) | Change |
|---------------------|-----------------|-----------------|--------|
| simdJsonEncode (cb) | 48.7            | 54.7            | +12%   |
| simdJsonEncode (ot) | 237.3           | 239.9           | +1%    |
| simdJsonEncode (ss) | 445.9           | 548.6           | +23%   |
| simdJsonBatch  (cb) | 42.7            | 46.6            | +9%    |
| simdJsonBatch  (ot) | 191.7           | 191.5           | flat   |
| simdJsonBatch  (ss) | 418.8           | 575.1           | +37%   |

Key finding: batch now beats per-doc on `small_sparse` (575.1 vs 548.6,
+5%). The `clickbench_flat` and `otel_nested` gaps remain — batch is
still ~15% and ~20% slower than per-doc respectively on those shapes.
