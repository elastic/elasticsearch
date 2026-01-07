# Field Encoding Pipeline Format - Implementation Guide

> This document covers implementation details for the FieldEncodingPipelineFormat TSDB DocValues format.
> For the format specification, see [Field Encoding Pipeline Format - High-level Design.md](Field%20Encoding%20Pipeline%20Format%20-%20High-level%20Design.md).

---

## Table of Contents

1. [Implementation Plan](#1-implementation-plan)
2. [Pull Request Breakdown](#2-pull-request-breakdown)
3. [Testing Strategy](#3-testing-strategy)
4. [Benchmark Infrastructure](#4-benchmark-infrastructure)
5. [References](#5-references)

---

## 1. Implementation Plan

### 1.1 Overview

The implementation is divided into four phases, each extending the pipeline architecture to different doc value types:

| Phase | Focus | Doc Value Types | PRs |
|-------|-------|-----------------|-----|
| **Phase 1** | Numeric Pipeline | NUMERIC, SORTED_NUMERIC | PR1-PR4 |
| **Phase 2** | Ordinal Pipeline | SORTED, SORTED_SET (ordinals) | PR5-PR6 |
| **Phase 3** | Binary Pipeline | BINARY, Terms Dictionary | PR7-PR8 |
| **Phase 4** | Scaled Doubles | Floating-point values | PR9-PR10 |

### 1.2 PR Dependency Graph

```
Phase 1 (Numeric):
PR1 ──► PR2 ──► PR3 ──► PR4
                         │
                         ├──► Phase 2 (Ordinal): PR5 ──► PR6
                         │
                         ├──► Phase 3 (Binary):  PR7 ──► PR8
                         │
                         └──► Phase 4 (Scaled):  PR9 ──► PR10
```

After PR4 (Numeric Integration), the ordinal/binary/scaled-double phases can proceed in parallel.

### 1.3 Stage ID Registry

Stage IDs are **type-agnostic**: the same ID can be used across different pipeline types. The decoder selects the appropriate implementation based on the field type from Lucene metadata.

```java
public enum StageId {
    // Numeric stages
    DELTA((byte) 0x01),
    OFFSET((byte) 0x02),
    GCD((byte) 0x03),

    // Ordinal stages
    RUN_LENGTH((byte) 0x10),
    CYCLE((byte) 0x11),

    // Terminal stages (shared across types)
    BIT_PACK((byte) 0x20),

    // Compression stages
    LZ4((byte) 0x30),
    ZSTD((byte) 0x31),

    // Scaled double stages
    SCALING((byte) 0x40),
    XOR((byte) 0x41);

    public final byte id;
    StageId(byte id) { this.id = id; }
}
```

---

## 2. Pull Request Breakdown

Each PR section below is written as a **self-contained prompt** that can be used to instruct Claude to implement that PR.

---

### Phase 1: Numeric Pipeline

---

### PR1: Pipeline Infrastructure

#### Prompt

```
Implement PR1: Pipeline Infrastructure for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. This PR implements the core pipeline infrastructure that will be reused across all doc value types (numeric, ordinal, binary).

## Requirements

Create the following files in `server/src/main/java/org/elasticsearch/index/codec/tsdb/pipeline/`:

### 1. MetadataBuffer.java

A reusable byte buffer for stage metadata with auto-grow capability.

Requirements:
- Initial capacity of 64 bytes
- Auto-grow when capacity exceeded (double size)
- Sequential write methods: `writeByte`, `writeVInt`, `writeVLong`, `writeZLong`
- Sequential read methods: `readByte`, `readVInt`, `readVLong`, `readZLong`
- `reset()` to reset position to 0 for reuse
- `array()` and `size()` to access underlying data for I/O
- Zero-allocation in steady state (buffer grows once and stays)

### 2. EncodeContext.java

Per-thread mutable state for encoding.

Requirements:
- Contains a `MetadataBuffer`
- `reset()` method to prepare for encoding a new block
- NOT thread-safe (one context per thread)

### 3. DecodeContext.java

Per-thread mutable state for decoding.

Requirements:
- Contains a `MetadataBuffer`
- `reset()` method to prepare for decoding a new block
- NOT thread-safe (one context per thread)

### 4. numeric/NumericEncoder.java

Interface for encoding numeric values.

```java
public interface NumericEncoder {
    byte id();
    String name();
    void encode(long[] values, int numValues, MetadataBuffer buffer);
}
```

### 5. numeric/NumericDecoder.java

Interface for decoding numeric values.

```java
public interface NumericDecoder {
    byte id();
    String name();
    void decode(long[] values, int numValues, MetadataBuffer buffer);
}
```

### 6. numeric/NumericCodecStage.java

Combined interface for stateless singleton stages.

```java
public interface NumericCodecStage extends NumericEncoder, NumericDecoder {}
```

### 7. numeric/NumericCodecPipeline.java

Chains multiple `NumericCodecStage` instances. Itself implements `NumericCodecStage` (Composite pattern).

Requirements:
- Immutable and thread-safe
- `encode()` applies stages in forward order, each stage decides if it should apply
- `decode()` applies stages in reverse order based on stage IDs read from buffer
- Stages write their ID + params to MetadataBuffer only if they apply

## Tests

Create tests in `server/src/test/java/org/elasticsearch/index/codec/tsdb/pipeline/`:

### MetadataBufferTests.java
- Test sequential write then read of all primitive types
- Test auto-grow when exceeding initial capacity
- Test reset() clears position but retains grown buffer
- Test edge cases: empty buffer, max values

### NumericCodecPipelineTests.java
- Test building pipelines with mock stages
- Test encode/decode with stages that skip (don't apply)
- Test pipeline with single stage
- Test pipeline with multiple stages

### EncodeContextTests.java / DecodeContextTests.java
- Test context reset and reuse
- Test MetadataBuffer lifecycle within context

## Review Checklist
- [ ] All interfaces have Javadoc
- [ ] MetadataBuffer auto-grow works correctly
- [ ] Zero allocations after initial grow
- [ ] Pipeline correctly chains stages
- [ ] All tests pass
```

**Size estimate**: ~800 LOC

---

### PR2: Numeric Codec Stages

#### Prompt

```
Implement PR2: Numeric Codec Stages for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. This PR implements the four numeric codec stages as stateless singletons.

**Depends on**: PR1 (Pipeline Infrastructure) must be merged first.

## Reference Implementation

Study the existing ES819 encoder at:
- `server/src/main/java/org/elasticsearch/index/codec/tsdb/TSDBDocValuesEncoder.java`

The new stages must produce identical encoding decisions (when to apply) and transformations as the existing implementation.

## Requirements

Create the following files:

### 1. StageId.java in `server/src/main/java/org/elasticsearch/index/codec/tsdb/es94/`

```java
public enum StageId {
    DELTA((byte) 0x01),
    OFFSET((byte) 0x02),
    GCD((byte) 0x03),
    BIT_PACK((byte) 0x20);

    public final byte id;
    StageId(byte id) { this.id = id; }

    public static StageId fromId(byte id) { ... }
}
```

### 2. NumericStages.java in `server/src/main/java/org/elasticsearch/index/codec/tsdb/es94/numeric/`

Registry to lookup stages by ID:
```java
public static NumericCodecStage lookup(byte stageId) { ... }
```

### 3. stages/DeltaCodecStage.java

Delta encoding for monotonic sequences.

Requirements:
- Stateless singleton
- **Apply condition**: Same as ES819 - `(gts == 0 && lts >= 2) || (lts == 0 && gts >= 2)` where gts/lts count values greater/less than previous
- **Encode**: Write stage ID (0x01) + first value (ZLong), then transform `values[i] = values[i] - values[i-1]` in reverse order
- **Decode**: Read first value, then restore `values[i] = values[i] + values[i-1]`
- **Skip**: If condition not met, don't write anything, leave values unchanged

### 4. stages/OffsetCodecStage.java

Removes minimum offset.

Requirements:
- Stateless singleton
- **Apply condition**: Same as ES819 - `min != 0` AND no overflow when computing `max - min`
- **Encode**: Write stage ID (0x02) + min value (ZLong), then transform `values[i] = values[i] - min`
- **Decode**: Read min, then restore `values[i] = values[i] + min`
- **Skip**: If condition not met, don't write anything, leave values unchanged

### 5. stages/GcdCodecStage.java

Divides by greatest common divisor.

Requirements:
- Stateless singleton
- **Apply condition**: Same as ES819 - `gcd > 1` (using unsigned comparison)
- **Encode**: Write stage ID (0x03) + (gcd - 2) as VLong, then transform `values[i] = values[i] / gcd`
- **Decode**: Read gcd, then restore `values[i] = values[i] * gcd`
- **Skip**: If gcd <= 1, don't write anything, leave values unchanged

### 6. stages/BitPackCodecStage.java

Bit-packs values using minimum bits per value.

Requirements:
- Stateless singleton
- **Always applies** (terminal stage)
- **Encode**: Write stage ID (0x20) + bitsPerValue (VInt), values are ready for bit-packing
- **Decode**: Read bitsPerValue, values contain bit-packed data to be unpacked
- Use `DocValuesForUtil` for actual bit-packing/unpacking

## Tests

Create tests in `server/src/test/java/org/elasticsearch/index/codec/tsdb/es94/numeric/stages/`:

### DeltaCodecStageTests.java
- Test monotonic increasing sequence (should apply)
- Test monotonic decreasing sequence (should apply)
- Test non-monotonic sequence (should skip)
- Test overflow handling near Long.MIN_VALUE/MAX_VALUE
- Test round-trip: encode then decode restores original

### OffsetCodecStageTests.java
- Test positive min removal
- Test negative min removal
- Test min=0 (should skip)
- Test overflow guard when max-min overflows
- Test round-trip

### GcdCodecStageTests.java
- Test gcd > 1 case
- Test gcd = 1 (should skip)
- Test mixed positive/negative values
- Test all zeros
- Test round-trip

### BitPackCodecStageTests.java
- Test all bitsPerValue from 1 to 64
- Test partial blocks (numValues < blockSize)
- Test round-trip

## Cross-Validation

Add `ES94CrossValidationTests.java` that:
1. Generates random blocks
2. Encodes with ES819's TSDBDocValuesEncoder
3. Encodes with new stages
4. Verifies identical apply/skip decisions
5. Verifies identical decoded values

## Review Checklist
- [ ] All stages are stateless singletons
- [ ] Apply conditions match ES819 exactly
- [ ] Transformations match ES819 exactly
- [ ] Edge cases handled (overflow, zeros, etc.)
- [ ] All tests pass
- [ ] Cross-validation passes
```

**Size estimate**: ~1200 LOC

---

### PR3: ES94 Numeric Format (Writer + Reader)

#### Prompt

```
Implement PR3: ES94 Numeric Format for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. This PR implements the consumer (writer) and producer (reader) for NUMERIC and SORTED_NUMERIC doc values using the pipeline-based encoding.

**Depends on**: PR2 (Numeric Codec Stages) must be merged first.

## Reference Implementation

Study the existing ES819 implementation at:
- `server/src/main/java/org/elasticsearch/index/codec/tsdb/es819/ES819TSDBDocValuesFormat.java`
- `server/src/main/java/org/elasticsearch/index/codec/tsdb/es819/ES819TSDBDocValuesConsumer.java`
- `server/src/main/java/org/elasticsearch/index/codec/tsdb/es819/ES819TSDBDocValuesProducer.java`

## Requirements

Create the following files in `server/src/main/java/org/elasticsearch/index/codec/tsdb/es94/`:

### 1. ES94TSDBDocValuesFormat.java

Codec entry point.

Requirements:
- Extends `DocValuesFormat`
- Constants: VERSION_START=0, VERSION_CURRENT=0
- File extensions: `.dvm` (metadata), `.dvd` (data)
- `fieldsConsumer()` returns `ES94TSDBDocValuesConsumer`
- `fieldsProducer()` returns `ES94TSDBDocValuesProducer`

### 2. ES94TSDBDocValuesConsumer.java

Writer for doc values.

Requirements for NUMERIC/SORTED_NUMERIC:
- Implement `addNumericField()` and `addSortedNumericField()`
- Write self-describing block format:
  ```
  [num_stages: VInt]
  For each applied stage (in reverse pipeline order):
    [stage_id: byte]
    [stage_params...]
  [payload: bit-packed values]
  ```
- Use `NumericCodecPipeline` for encoding
- Write block index using DirectMonotonicWriter (same as ES819)
- Write DISI for sparse fields (same as ES819)

For other doc value types (SORTED, SORTED_SET, BINARY):
- Throw `UnsupportedOperationException` for now (will be added in later PRs)

### 3. ES94TSDBDocValuesProducer.java

Reader for doc values.

Requirements for NUMERIC/SORTED_NUMERIC:
- Implement `getNumeric()` and `getSortedNumeric()`
- Read self-describing block format:
  1. Read `num_stages`
  2. For each stage: read stage_id, lookup stage, read params
  3. Read payload
  4. Apply stages in reverse order (last written = first applied)
- Validate stage IDs - fail fast on unknown IDs

For other doc value types:
- Throw `UnsupportedOperationException` for now

### 4. numeric/NumericPipelines.java

Pre-built pipeline singletons.

```java
public class NumericPipelines {
    public static final NumericCodecPipeline DEFAULT_NUMERIC_PIPELINE =
        new NumericCodecPipeline(
            DeltaCodecStage.INSTANCE,
            OffsetCodecStage.INSTANCE,
            GcdCodecStage.INSTANCE,
            BitPackCodecStage.INSTANCE
        );
}
```

## Wire Format

Each block in `.dvd`:
```
+---------------------------------------------------------------------+
| [num_stages: VInt] : number of stages applied to this block         |
| For each stage (in reverse pipeline order):                         |
|   [stage_id: byte] : identifies the stage type                      |
|   [stage_params...] : stage-specific parameters                     |
| [payload] : bit-packed values                                       |
+---------------------------------------------------------------------+
```

## Tests

Create tests in `server/src/test/java/org/elasticsearch/index/codec/tsdb/es94/`:

### ES94TSDBDocValuesFormatTests.java
- Extend `BaseDocValuesFormatTestCase`
- Override codec to use ES94TSDBDocValuesFormat

### ES94NumericRoundTripTests.java
- Write numeric doc values, read back, verify identical
- Test various data patterns: monotonic, constant, random, sparse
- Test both 128 and 512 block sizes

### SelfDescribingBlockTests.java
- Verify block format: num_stages, stage_ids, params, payload
- Test unknown stage ID rejection
- Test stage ID validation

### PerBlockPipelineTests.java
- Verify different blocks can have different stages applied
- Test blocks where some stages skip

## Review Checklist
- [ ] Wire format matches specification
- [ ] Self-describing blocks work correctly
- [ ] Stage ID validation works
- [ ] Round-trip tests pass
- [ ] BaseDocValuesFormatTestCase passes
```

**Size estimate**: ~1500 LOC

---

### PR4: Numeric Integration & Version Gating

#### Prompt

```
Implement PR4: Numeric Integration & Version Gating for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. This PR adds version gating to ensure the new format is only used for new indices, and adds comprehensive integration tests.

**Depends on**: PR3 (ES94 Numeric Format) must be merged first.

## Requirements

### 1. Version Gating

Modify `ES94TSDBDocValuesFormat.java`:
- Add index version check to gate usage
- New format should only be used for indices created on or after the version where this is introduced
- Update `PerFieldMapperCodec` or equivalent to select ES94 format based on index version

### 2. Cross-Validation with ES819

Create `ES94CrossValidationTests.java`:
- For the same input data, verify ES94 produces identical decoded values as ES819
- Test various data patterns
- This ensures the new format is a drop-in replacement

### 3. Integration Tests

Create comprehensive integration tests:

#### ES94NumericIntegrationTests.java
- Create TSDB index with ES94 codec
- Index documents with numeric fields
- Verify values via doc values iterator
- Verify values via search queries (range queries, aggregations)

#### ES94BlockSizeTests.java
- Test with block_shift=7 (128 values)
- Test with block_shift=9 (512 values)
- Verify correct block size handling

### 4. Performance Benchmarks

Extend the existing TSDB codec benchmarks to compare ES94 vs ES819.

#### Existing Benchmark Infrastructure

The benchmark infrastructure is already in place at:
- `benchmarks/src/main/java/org/elasticsearch/benchmark/index/codec/tsdb/`

Key classes to use:
- `CompressionMetrics` - Tracks encoding efficiency (bytes/value, compression ratio, bits/value, overhead)
- `ThroughputMetrics` - Tracks throughput (values/sec, blocks/sec, bytes/sec)
- `AbstractDocValuesForUtilBenchmark` - Base class for ForUtil benchmarks

#### New Benchmarks to Create

Create `ES94PipelineBenchmark.java`:
- Compare ES94 pipeline encoding/decoding vs ES819
- Use `CompressionMetrics` for compression efficiency comparison
- Use `ThroughputMetrics` for throughput comparison
- Test data patterns: constant, monotonic, random, mixed

#### How to Run

```bash
# Run encode benchmarks with compression metrics
./gradlew -p benchmarks run --args 'EncodeNonSortedIntegerBenchmark'

# Run ES94 pipeline benchmarks (after implementation)
./gradlew -p benchmarks run --args 'ES94PipelineBenchmark'
```

## Tests

### VersionGatingTests.java
- Verify ES94 format used for new indices (version >= gate)
- Verify ES819 format used for old indices (version < gate)

### ES94CrossValidationTests.java
- Random blocks: encode with both, decode with both, compare
- Monotonic blocks
- Constant blocks
- Mixed-sign blocks
- Extreme values (Long.MIN_VALUE, Long.MAX_VALUE)

### ES94NumericIntegrationTests.java
- Full index lifecycle: create, index, search, read doc values
- Sparse fields (not all docs have values)
- Multi-valued fields (sorted numeric)

## Review Checklist
- [ ] Version gating works correctly
- [ ] Cross-validation passes for all data patterns
- [ ] Integration tests pass
- [ ] No regressions vs ES819
```

**Size estimate**: ~600 LOC

---

### Phase 2: Ordinal Pipeline

---

### PR5: Ordinal Pipeline Infrastructure

#### Prompt

```
Implement PR5: Ordinal Pipeline Infrastructure for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. This PR implements the ordinal-specific pipeline infrastructure for SORTED and SORTED_SET doc values.

**Depends on**: PR4 (Numeric Integration) must be merged first.

## Reference Implementation

Study the existing ES819 ordinal encoding at:
- `server/src/main/java/org/elasticsearch/index/codec/tsdb/TSDBDocValuesEncoder.java` - `encodeOrdinals()` method

## Requirements

Create the following files in `server/src/main/java/org/elasticsearch/index/codec/tsdb/pipeline/ordinal/`:

### 1. OrdinalEncoder.java

```java
public interface OrdinalEncoder {
    byte id();
    String name();
    void encode(long[] ordinals, int numValues, int bitsPerOrd, MetadataBuffer buffer);
}
```

Note: `bitsPerOrd` is passed because ordinal encoding strategies depend on the ordinal range.

### 2. OrdinalDecoder.java

```java
public interface OrdinalDecoder {
    byte id();
    String name();
    void decode(long[] ordinals, int numValues, int bitsPerOrd, MetadataBuffer buffer);
}
```

### 3. OrdinalCodecStage.java

```java
public interface OrdinalCodecStage extends OrdinalEncoder, OrdinalDecoder {}
```

### 4. OrdinalCodecPipeline.java

Chains `OrdinalCodecStage` instances (Composite pattern).

Requirements:
- Same design as `NumericCodecPipeline`
- Immutable and thread-safe
- Stages decide whether to apply based on ordinal patterns

## Tests

### OrdinalCodecPipelineTests.java
- Test building ordinal pipelines with mock stages
- Test encode/decode with stages that skip
- Test pipeline with single stage
- Test bitsPerOrd parameter is passed correctly

## Review Checklist
- [ ] Interface design matches numeric pattern
- [ ] bitsPerOrd parameter handled correctly
- [ ] Pipeline chains stages correctly
- [ ] All tests pass
```

**Size estimate**: ~400 LOC

---

### PR6: Ordinal Codec Stages & Format

#### Prompt

```
Implement PR6: Ordinal Codec Stages & Format for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. This PR implements ordinal encoding stages and adds SORTED/SORTED_SET support to the consumer/producer.

**Depends on**: PR5 (Ordinal Pipeline Infrastructure) must be merged first.

## Reference Implementation

Study the existing ES819 ordinal encoding at:
- `server/src/main/java/org/elasticsearch/index/codec/tsdb/TSDBDocValuesEncoder.java` - `encodeOrdinals()` method

The current implementation supports:
- Single run encoding (all ordinals identical)
- Two runs encoding
- Cycle detection (repeating patterns)
- Bit-packing

## Requirements

### 1. Add Stage IDs to StageId.java

```java
RUN_LENGTH((byte) 0x10),
CYCLE((byte) 0x11),
```

### 2. Create stages in `server/src/main/java/org/elasticsearch/index/codec/tsdb/es94/ordinal/stages/`:

#### RunLengthCodecStage.java
- Encodes consecutive identical ordinals as (value, count) pairs
- Apply condition: Long runs of identical ordinals
- Match ES819 behavior

#### CycleCodecStage.java
- Detects repeating patterns and stores one cycle
- Apply condition: Repeating pattern detected
- Match ES819 behavior

#### OrdinalBitPackCodecStage.java
- Bit-packs ordinals using bitsPerOrd
- Terminal stage (always applies)

### 3. OrdinalStages.java

Registry to lookup ordinal stages by ID.

### 4. OrdinalPipelines.java

```java
public static final OrdinalCodecPipeline DEFAULT_ORDINAL_PIPELINE = ...;
```

### 5. Update ES94TSDBDocValuesConsumer.java

Implement:
- `addSortedField()` - ordinals + terms dictionary
- `addSortedSetField()` - ordinals + terms dictionary

Use OrdinalCodecPipeline for ordinal encoding. Terms dictionary encoding remains unchanged from ES819.

### 6. Update ES94TSDBDocValuesProducer.java

Implement:
- `getSorted()` - read ordinals + terms dictionary
- `getSortedSet()` - read ordinals + terms dictionary

## Tests

### RunLengthCodecStageTests.java
- Test single run (all same ordinal)
- Test two runs
- Test no runs (should skip)
- Test round-trip

### CycleCodecStageTests.java
- Test cycle detection
- Test no cycle (should skip)
- Test round-trip

### OrdinalBitPackCodecStageTests.java
- Test all bitsPerOrd values
- Test round-trip

### ES94SortedDocValuesTests.java
- Round-trip for SORTED doc values
- Round-trip for SORTED_SET doc values
- Terms dictionary correctly written/read
- Cross-validation with ES819

## Review Checklist
- [ ] Ordinal stages match ES819 behavior
- [ ] SORTED/SORTED_SET round-trip works
- [ ] Terms dictionary handling unchanged
- [ ] All tests pass
```

**Size estimate**: ~1200 LOC

---

### Phase 3: Binary Pipeline

---

### PR7: Binary Pipeline Infrastructure

#### Prompt

```
Implement PR7: Binary Pipeline Infrastructure for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. This PR implements the binary-specific pipeline infrastructure for BINARY doc values.

**Depends on**: PR4 (Numeric Integration) must be merged first.

## Reference Implementation

Study the existing ES819 binary encoding at:
- `server/src/main/java/org/elasticsearch/index/codec/tsdb/es819/ES819TSDBDocValuesConsumer.java` - `addBinaryField()` method

## Requirements

Create the following files in `server/src/main/java/org/elasticsearch/index/codec/tsdb/pipeline/binary/`:

### 1. BinaryEncoder.java

```java
public interface BinaryEncoder {
    byte id();
    String name();
    void encode(byte[] data, int offset, int length, MetadataBuffer buffer, DataOutput output) throws IOException;
}
```

Note: Binary encoding writes directly to output (may compress).

### 2. BinaryDecoder.java

```java
public interface BinaryDecoder {
    byte id();
    String name();
    void decode(byte[] data, int offset, int length, MetadataBuffer buffer, DataInput input) throws IOException;
}
```

### 3. BinaryCodecStage.java

```java
public interface BinaryCodecStage extends BinaryEncoder, BinaryDecoder {}
```

### 4. BinaryCodecPipeline.java

Chains `BinaryCodecStage` instances.

Requirements:
- Similar to numeric/ordinal pipelines
- Handles block-wise compression

## Tests

### BinaryCodecPipelineTests.java
- Test building binary pipelines with mock stages
- Test encode/decode
- Test compression stages

## Review Checklist
- [ ] Interface design handles streaming I/O
- [ ] Pipeline chains stages correctly
- [ ] All tests pass
```

**Size estimate**: ~400 LOC

---

### PR8: Binary Codec Stages & Format

#### Prompt

```
Implement PR8: Binary Codec Stages & Format for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. This PR implements binary compression stages and adds BINARY support to the consumer/producer.

**Depends on**: PR7 (Binary Pipeline Infrastructure) must be merged first.

## Reference Implementation

Study the existing ES819 binary encoding at:
- `server/src/main/java/org/elasticsearch/index/codec/tsdb/es819/ES819TSDBDocValuesConsumer.java` - `CompressedBinaryBlockWriter`

## Requirements

### 1. Add Stage IDs to StageId.java

```java
LZ4((byte) 0x30),
ZSTD((byte) 0x31),
```

### 2. Create stages in `server/src/main/java/org/elasticsearch/index/codec/tsdb/es94/binary/stages/`:

#### LZ4CodecStage.java
- LZ4 compression for binary blocks
- Use Lucene's LZ4 implementation

#### ZSTDCodecStage.java
- ZSTD compression for binary blocks
- Configurable compression level

### 3. BinaryStages.java

Registry to lookup binary stages by ID.

### 4. BinaryPipelines.java

```java
public static final BinaryCodecPipeline LZ4_PIPELINE = ...;
public static final BinaryCodecPipeline ZSTD_PIPELINE = ...;
```

### 5. Update ES94TSDBDocValuesConsumer.java

Implement `addBinaryField()`:
- Block-wise compression using BinaryCodecPipeline
- Match ES819 block thresholds

### 6. Update ES94TSDBDocValuesProducer.java

Implement `getBinary()`:
- Read compressed blocks
- Decompress using appropriate stage

## Tests

### LZ4CodecStageTests.java
- Compression/decompression round-trip
- Various data sizes

### ZSTDCodecStageTests.java
- Compression/decompression round-trip
- Various compression levels

### ES94BinaryDocValuesTests.java
- Round-trip for BINARY doc values
- Various binary sizes
- Cross-validation with ES819

## Review Checklist
- [ ] Compression stages work correctly
- [ ] BINARY round-trip works
- [ ] Block thresholds match ES819
- [ ] All tests pass
```

**Size estimate**: ~1000 LOC

---

### Phase 4: Scaled Doubles

---

### PR9: Scaling Stage

#### Prompt

```
Implement PR9: Scaling Stage for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. Also read `docs/design/es94-tsdb-scaled-doubles.md` for the scaled doubles design.

This PR implements the SCALING stage for converting doubles to longs via decimal scaling.

**Depends on**: PR4 (Numeric Integration) must be merged first.

## Requirements

### 1. Add Stage ID to StageId.java

```java
SCALING((byte) 0x40),
```

### 2. Create ScalingCodecStage.java in `server/src/main/java/org/elasticsearch/index/codec/tsdb/es94/numeric/stages/`

Scaling stage for double→long conversion.

Requirements:
- Converts doubles to longs by multiplying by 10^N (where N is the scaling factor)
- **Encode**:
  1. Determine optimal scaling factor N (0-12) that preserves precision
  2. Write stage ID (0x40) + scaling factor (VInt)
  3. Transform: `longValue = (long)(doubleValue * 10^N)`
- **Decode**:
  1. Read scaling factor
  2. Transform: `doubleValue = longValue / 10^N`
- **Precision**: Epsilon of 1e-13 for validation
- **Fallback**: If scaling cannot preserve precision, store raw double bits as long

### 3. Precision Handling

```java
private int findScalingFactor(double[] values) {
    // Find smallest N where all values can be represented as integers
    // within epsilon tolerance
    for (int n = 0; n <= 12; n++) {
        if (canScale(values, n)) return n;
    }
    return -1; // fallback to raw bits
}
```

## Tests

### ScalingCodecStageTests.java
- Test various scaling factors (0-12)
- Test precision preservation
- Test fallback for high-precision values
- Test special values: NaN, Infinity, -0.0
- Test round-trip: scale then unscale matches original within epsilon

## Review Checklist
- [ ] Scaling factor selection is optimal
- [ ] Precision preserved within epsilon
- [ ] Fallback works for unscalable values
- [ ] Special values handled
- [ ] All tests pass
```

**Size estimate**: ~400 LOC

---

### PR10: XOR Stage

#### Prompt

```
Implement PR10: XOR Stage for the FieldEncodingPipelineFormat codec.

## Context

Read the design document at `docs/design/Field Encoding Pipeline Format - High-level Design.md` for full context. Also read `docs/design/es94-tsdb-scaled-doubles.md` for the scaled doubles design.

This PR implements the XOR stage for floating-point bit similarity (Gorilla paper technique).

**Depends on**: PR9 (Scaling Stage) must be merged first.

## Reference

- Pelkonen, T. et al. (2015). "Gorilla: A Fast, Scalable, In-Memory Time Series Database"

## Requirements

### 1. Add Stage ID to StageId.java

```java
XOR((byte) 0x41),
```

### 2. Create XorCodecStage.java in `server/src/main/java/org/elasticsearch/index/codec/tsdb/es94/numeric/stages/`

XOR stage for exploiting bit-level similarity.

Requirements:
- **Encode**:
  1. Write stage ID (0x41) + first value (VLong)
  2. Transform: `values[i] = values[i] XOR values[i-1]` (in reverse order)
- **Decode**:
  1. Read first value
  2. Transform: `values[i] = values[i] XOR values[i-1]`
- **Apply condition**: Always apply (or check if XOR reduces bits needed)

### 3. Update NumericPipelines.java

```java
public static final NumericCodecPipeline SCALED_DOUBLE_PIPELINE =
    new NumericCodecPipeline(
        ScalingCodecStage.INSTANCE,
        XorCodecStage.INSTANCE,
        DeltaCodecStage.INSTANCE,
        OffsetCodecStage.INSTANCE,
        GcdCodecStage.INSTANCE,
        BitPackCodecStage.INSTANCE
    );
```

## Tests

### XorCodecStageTests.java
- Test XOR encoding for similar values
- Test XOR encoding for dissimilar values
- Test round-trip

### ScaledDoublePipelineTests.java
- Full pipeline test with double[] input
- Various data patterns: constant, slowly changing, random
- Compare compression ratio vs raw storage
- Round-trip precision verification

## Review Checklist
- [ ] XOR encoding correct
- [ ] Full pipeline works end-to-end
- [ ] Precision preserved for doubles
- [ ] All tests pass
```

**Size estimate**: ~400 LOC

---

## 2.12 PR Summary Table

| PR | Title | Phase | Depends On | Size | Risk |
|----|-------|-------|------------|------|------|
| PR1 | Pipeline Infrastructure | 1 | - | ~800 LOC | Low |
| PR2 | Numeric Stages | 1 | PR1 | ~1200 LOC | Medium |
| PR3 | ES94 Numeric Format | 1 | PR2 | ~1500 LOC | Medium |
| PR4 | Numeric Integration | 1 | PR3 | ~600 LOC | Medium |
| PR5 | Ordinal Pipeline | 2 | PR4 | ~400 LOC | Low |
| PR6 | Ordinal Stages & Format | 2 | PR5 | ~1200 LOC | Medium |
| PR7 | Binary Pipeline | 3 | PR4 | ~400 LOC | Low |
| PR8 | Binary Stages & Format | 3 | PR7 | ~1000 LOC | Medium |
| PR9 | Scaling Stage | 4 | PR4 | ~400 LOC | Medium |
| PR10 | XOR Stage | 4 | PR9 | ~400 LOC | Medium |

**Total**: ~7900 LOC across 10 PRs

---

## 3. Testing Strategy

### 3.1 Stage Unit Tests

Each stage tested in isolation:
- Apply/skip predicates
- Transformation correctness
- Round-trip
- Edge cases

### 3.2 Cross-Validation Tests

Verify ES94 produces identical decoded values as ES819:
- Round-trip equivalence
- Encoding decisions match

### 3.3 Integration Tests

- Index round-trip for all doc value types
- Block size coverage (128 and 512)

---

## 4. Benchmark Infrastructure

### 4.1 Overview

The TSDB codec benchmarks measure both encoding/decoding performance (throughput) and compression efficiency. These benchmarks use JMH (Java Microbenchmark Harness) with custom AuxCounters for additional metrics.

> **Note:** Low-level SIMD vectorization optimizations (e.g., expand8 loop splitting) are out of scope for this implementation plan and will be addressed separately as cross-cutting performance improvements to the underlying ForUtil bit-packing operations.

### 4.2 Existing Benchmark Classes

The following benchmark infrastructure is available for reuse:

#### `benchmarks/src/main/java/org/elasticsearch/benchmark/index/codec/tsdb/`

| File | Purpose |
|------|---------|
| `EncodeConstantIntegerBenchmark.java` | Benchmark encoding of constant values |
| `EncodeIncreasingIntegerBenchmark.java` | Benchmark encoding of monotonically increasing values |
| `EncodeDecreasingIntegerBenchmark.java` | Benchmark encoding of monotonically decreasing values |
| `EncodeNonSortedIntegerBenchmark.java` | Benchmark encoding of random/unsorted values |
| `DecodeConstantIntegerBenchmark.java` | Benchmark decoding of constant values |
| `DecodeIncreasingIntegerBenchmark.java` | Benchmark decoding of monotonically increasing values |
| `DecodeDecreasingIntegerBenchmark.java` | Benchmark decoding of monotonically decreasing values |
| `DecodeNonSortedIntegerBenchmark.java` | Benchmark decoding of random/unsorted values |

#### `benchmarks/src/main/java/org/elasticsearch/benchmark/index/codec/tsdb/internal/`

| File | Purpose |
|------|---------|
| `CompressionMetrics.java` | JMH AuxCounters for compression efficiency metrics |
| `ThroughputMetrics.java` | JMH AuxCounters for throughput metrics |
| `AbstractDocValuesForUtilBenchmark.java` | Base class for ForUtil benchmarks |
| `EncodeBenchmark.java` | Encode benchmark implementation |
| `DecodeBenchmark.java` | Decode benchmark implementation |
| `ConstantIntegerSupplier.java` | Generates constant value test data |
| `IncreasingIntegerSupplier.java` | Generates monotonically increasing test data |
| `DecreasingIntegerSupplier.java` | Generates monotonically decreasing test data |
| `NonSortedIntegerSupplier.java` | Generates random/unsorted test data |

### 4.3 Compression Metrics

The `CompressionMetrics` class captures encoding efficiency:

```java
// Metrics reported by JMH
public double bytesPerValue;     // Average encoded bytes per input value
public double compressionRatio;  // Ratio of raw bytes to encoded bytes (higher is better)
public double bitsPerValue;      // Average bits used per input value
public long overheadBytes;       // Total overhead bytes (metadata) per iteration
public double overheadRatio;     // Ratio of actual to theoretical minimum bytes
public long totalBytesWritten;   // Total encoded bytes written per iteration
```

Usage in benchmarks:

```java
@Benchmark
public void benchmark(Blackhole bh, CompressionMetrics compression, ThroughputMetrics throughput) throws IOException {
    encode.benchmark(bitsPerValue, bh);

    int outputBytes = encode.getEncodedBytes();
    compression.record(BLOCK_SIZE, outputBytes, bitsPerValue);
    throughput.record(BLOCK_SIZE, outputBytes);
}
```

### 4.4 Throughput Metrics

The `ThroughputMetrics` class captures processing throughput:

```java
// Metrics reported by JMH (operations/sec)
public long valuesProcessed;  // Total number of values encoded/decoded
public long blocksProcessed;  // Total number of blocks processed
public long bytesWritten;     // Total bytes written (for encode benchmarks)
```

### 4.5 Running Benchmarks

```bash
# Run all encode benchmarks
./gradlew -p benchmarks run --args 'Encode*Benchmark'

# Run specific benchmark with JSON output
./gradlew -p benchmarks run --args 'EncodeNonSortedIntegerBenchmark -rf json -rff results.json'

# Run with specific bits per value
./gradlew -p benchmarks run --args 'EncodeNonSortedIntegerBenchmark -p bitsPerValue=8'

# Run with more iterations for stable results
./gradlew -p benchmarks run --args 'EncodeNonSortedIntegerBenchmark -wi 5 -i 20'
```

### 4.6 Benchmark Output Example

```
Benchmark                                                    Mode  Cnt      Score     Error   Units
EncodeNonSortedIntegerBenchmark.benchmark                   sample 1000    245.32 ±   12.34   ns/op
EncodeNonSortedIntegerBenchmark.benchmark:bitsPerValue      sample        8.00                bits/value
EncodeNonSortedIntegerBenchmark.benchmark:bytesPerValue     sample        1.00                bytes/value
EncodeNonSortedIntegerBenchmark.benchmark:compressionRatio  sample        8.00                ratio
EncodeNonSortedIntegerBenchmark.benchmark:overheadRatio     sample        1.00                ratio
EncodeNonSortedIntegerBenchmark.benchmark:valuesProcessed   sample   128.00                   ops
```

### 4.7 ES94 Pipeline Benchmarks (To Be Added)

For the ES94 pipeline implementation, create additional benchmarks:

```java
/**
 * Benchmark comparing ES94 pipeline encoding vs ES819.
 */
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class ES94PipelineBenchmark {

    @Param({ "128", "512" })
    private int blockSize;

    @Param({ "constant", "monotonic", "random" })
    private String dataPattern;

    private NumericCodecPipeline es94Pipeline;
    private long[] values;
    private MetadataBuffer buffer;

    @Setup(Level.Iteration)
    public void setup() {
        es94Pipeline = NumericPipelines.DEFAULT_NUMERIC_PIPELINE;
        values = generateData(dataPattern, blockSize);
        buffer = new MetadataBuffer();
    }

    @Benchmark
    public void es94Encode(Blackhole bh, CompressionMetrics compression) {
        buffer.reset();
        es94Pipeline.encode(values, values.length, buffer);
        compression.record(blockSize, buffer.size(), computeBitsPerValue(values));
        bh.consume(buffer);
    }
}
```

---

## 5. References

### Elasticsearch / Lucene

- [ES819 TSDB DocValues Format](../server/src/main/java/org/elasticsearch/index/codec/tsdb/es819/)
- [Lucene DocValuesFormat](https://lucene.apache.org/core/9_0_0/core/org/apache/lucene/codecs/DocValuesFormat.html)

### Time Series Compression

- Pelkonen, T. et al. (2015). [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf). VLDB.
