# TSDB Doc Values Codec: Modular Architecture Design

## 1. Introduction

The ES819 TSDB doc values codec is a monolithic implementation: a single consumer
class (~1300 lines) owns the full write path for all field types, and a single
producer class (~2900 lines) owns the full read path. Introducing a new codec
version (ES94) requires duplicating or modifying large sections of these classes,
even when the change is limited to one concern (e.g., numeric block encoding).

This document describes a modular architecture where the base classes define the
structure and order in which sections are written and read, while each codec
version provides its own encoding and decoding for each section via dependency
injection. The refactoring PR (#144324) is the first step toward this architecture.

## 2. Current State

### What the refactoring PR (#144324) does

Extracts shared wire-format logic from ES819 into abstract base classes:

- `AbstractTSDBDocValuesConsumer` orchestrates the write path for all field types
- `AbstractTSDBDocValuesProducer` orchestrates the read path and metadata parsing
- `NumericFieldWriter` and `NumericFieldReader` interfaces let each codec own its
  numeric encoding end-to-end (entry metadata, block encoding, ordinal encoding)
- `DocOffsetsCodec` is injected via constructor instead of an abstract method
- Shared segment write state bundled into a context record
- `TSDBDocValuesFormatConfig` groups all format parameters into sub-records

### What ES94 changes vs ES819

ES94 replaces the monolithic `TSDBDocValuesEncoder` with a composable pipeline of
encoding stages (delta, offset, GCD, bit-pack) for numeric fields. It writes a
self-describing `FieldDescriptor` to metadata so the decoder can reconstruct the
pipeline without compile-time knowledge. Non-numeric field types (binary, sorted,
sorted-set) remain identical to ES819.

## 3. Target Architecture

### Principle

The base class defines **what** gets written and in **what order**. Each codec
defines **how** each section is encoded and decoded. The format class is the
composition root that wires codec-specific factory implementations to the base
class via constructor injection.

This follows Lucene's existing separation: `DocValuesConsumer` (write) and
`DocValuesProducer` (read) are independent. Each side receives only the
dependencies it needs. Writer factories are injected into the consumer,
reader factories into the producer.

### Why factories, not instances

Section writers and readers are created per field and hold mutable per-field state
(encoding contexts, block decoders, ordinal encoders). The base class controls
their lifecycle: when to create them, how many instances are needed, and when
they are done. Injecting factories gives the base class this control while letting
the codec define what gets created.

On the write side, a `NumericFieldWriterFactory` creates a writer per field,
each owning its own encoding state. On the read side, a `NumericFieldReaderFactory`
creates a reader per field, each owning its own decoding state. The base class
calls the factory when it needs an instance, and the codec never needs to know
about field lifecycle details.

### Wire format sections

A TSDB doc values segment consists of these sections, each a candidate for
codec-specific encoding:

| Section | Description | Pluggable today? |
|---|---|---|
| Numeric blocks | Block-encoded numeric values | Yes (NumericFieldWriter/Reader) |
| Numeric ordinals | Ordinal encoding for sorted fields | Yes (NumericFieldWriter/Reader) |
| Numeric entry header | Per-field metadata (stats, offsets, codec header) | Yes (NumericFieldWriter/Reader) |
| Skip index | Multi-level min/max tree for range skipping | No (hardcoded in base) |
| DISI | Document-level presence bitset | No (hardcoded IndexedDISI) |
| Binary data | Binary field values with optional compression | No (hardcoded in base) |
| Binary block compression | Per-block compression of binary data | Partially (mode configurable) |
| Doc offsets | Document offset encoding in binary blocks | Yes (DocOffsetsCodec injected) |
| Terms dict | LZ4-compressed term dictionary for sorted fields | No (hardcoded LZ4 in base) |
| Terms dict reverse index | Reverse lookup structure for terms | No (hardcoded in base) |
| DirectMonotonic index | Block-to-offset index for numeric values | No (Lucene fixed) |
| Ordinal range encoding | Compact encoding for low-cardinality ordinals | No (hardcoded in base) |

### Composition model

The format class is the composition root. It wires codec-specific factories into
the base classes. Each factory creates per-field instances on demand. Sections that
a codec does not customize use default factories matching the current ES819 behavior.

```
TSDBDocValuesFormat (codec-specific)
    |
    +-- fieldsConsumer(state) --> AbstractTSDBDocValuesConsumer(
    |       config,
    |       numericFieldWriterFactory,   // codec-specific or default
    |       binaryFieldWriterFactory,    // codec-specific or default
    |       termsDictWriterFactory,      // codec-specific or default
    |       skipIndexWriterFactory,      // codec-specific or default
    |       disiWriterFactory,           // codec-specific or default
    |       docOffsetsEncoder            // codec-specific or default
    |   )
    |
    +-- fieldsProducer(state) --> AbstractTSDBDocValuesProducer(
            config,
            numericFieldReaderFactory,   // codec-specific or default
            binaryFieldReaderFactory,    // codec-specific or default
            termsDictReaderFactory,      // codec-specific or default
            skipIndexReaderFactory,      // codec-specific or default
            disiReaderFactory,           // codec-specific or default
            docOffsetsDecoder,           // codec-specific or default
            entryFactory                 // codec-specific or default
        )
```

The base class orchestrates field dispatch, type tags, and section ordering. It
calls each factory to obtain a per-field writer or reader, then delegates encoding
and decoding to it. The base class never touches encoding logic directly.

The existing optimized merge path (`XDocValuesConsumer` merge methods with
pre-computed `MergeStats`) remains in the base class since merge orchestration
is format-agnostic. The merge path passes `MergeStats` through
`TsdbDocValuesProducer` into the codec's writer, which uses them to skip
stats computation and fuse DISI accumulation into the block loop.

## 4. End State: What a New Codec Looks Like

When the architecture is fully realized, introducing a new codec version requires
no changes to the base classes. The codec provides a format class that wires its
specific factories.

A hypothetical codec that changes numeric encoding and binary compression:

```java
public class CustomTSDBDocValuesFormat extends DocValuesFormat {

    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) {
        return new AbstractTSDBDocValuesConsumer(
            state, CUSTOM_CONFIG,
            ctx -> new CustomNumericFieldWriter(ctx),     // custom numeric encoder
            field -> new CustomBinaryFieldWriter(field),  // custom binary compressor
            TermsDictWriterFactory.DEFAULT,                // reuse default terms dict
            SkipIndexWriterFactory.DEFAULT,                // reuse default skip index
            DISIWriterFactory.DEFAULT,                     // reuse default DISI
            DocOffsetsCodec.GROUPED_VINT.getEncoder()      // reuse default doc offsets
        );
    }

    public DocValuesProducer fieldsProducer(SegmentReadState state) {
        return new AbstractTSDBDocValuesProducer(
            state, CUSTOM_CONFIG,
            (entry, bs) -> new CustomNumericFieldReader(entry, bs),
            entry -> new CustomBinaryFieldReader(entry),
            TermsDictReaderFactory.DEFAULT,
            SkipIndexReaderFactory.DEFAULT,
            DISIReaderFactory.DEFAULT,
            DocOffsetsCodec.GROUPED_VINT.getDecoder(),
            CustomEntryFactory.INSTANCE
        );
    }
}
```

What the codec author provides:
- Format class: wiring (the composition root)
- Custom writer and reader implementations for sections that change
- Custom entry factory if codec-specific metadata fields are needed

What the codec author does NOT touch:
- `AbstractTSDBDocValuesConsumer` / `AbstractTSDBDocValuesProducer`
- Default factories for sections that are unchanged
- Wire format ordering, field dispatch, type tags, merge orchestration

A codec that only changes numeric encoding (like ES94) provides a writer factory
and a reader factory. One that customizes every section provides more, but most
codecs only need to provide factories for the sections they change.

Each section is independently testable: unit test a writer by feeding it values
and verifying the output bytes, without constructing a full segment.

## 5. Incremental Path

Each step is self-contained, delivers value, and does not require subsequent steps.

**Step 1 (done, PR #144324): Numeric read/write abstraction**
- `NumericFieldWriter` / `NumericFieldReader` interfaces
- `DocOffsetsCodec` injected via constructor
- `NumericWriteContext` for shared write state
- ES819 as the first implementation

**Step 2 (ES94 format PR): Pipeline codec + entry factory**
- `EntryFactory` for codec-specific entry subclasses
- ES94 consumer/producer with pipeline encoding
- `FieldDescriptor` for self-describing wire format

**Step 3: Binary, sorted, sorted-set pluggability**
- `BinaryFieldWriterFactory` / `BinaryFieldReaderFactory`
- `TermsDictWriterFactory` / `TermsDictReaderFactory`
- Extract binary compression and terms dict encoding from base class
- Enables future codecs to introduce SORTED/BINARY/SORTED_SET pipelines

**Step 4: Skip index, DISI, ordinal range pluggability**
- `SkipIndexWriterFactory` / `SkipIndexReaderFactory`
- `DISIWriterFactory` / `DISIReaderFactory`
- `OrdinalRangeWriterFactory` / `OrdinalRangeReaderFactory`
- Enables format-specific skip index structures and DISI encodings
- Unlocks merge optimizations (raw block copy, skip index bulk merge)

## 6. Benefits and Opportunities

**Codec evolution without base class changes.** New codec versions are introduced
by wiring factory implementations in a format class. The base classes are stable
infrastructure that rarely changes.

**Per-field adaptive compression.** With pluggable writer factories, a pipeline
resolver could select a different encoding strategy per field based on data
profiling: delta + bit-pack for monotonic timestamps, ALP for low-precision
floats, FPC/Chimp/Gorilla for high-entropy doubles. The factory creates a writer
with the selected pipeline, persisted in the field descriptor.

**Pluggable compression for every section.** Dictionary-based binary compression,
columnar binary encoding, custom skip index structures (storing sum/count per
interval for pre-aggregation), tiered DISI encoding (roaring bitmaps, run-length
encoding, or skipping DISI for known-dense fields) all become possible by
providing a custom factory for the relevant section.

**Merge-time optimizations.** Clean section boundaries enable raw block copy during
same-codec merges and skip index bulk merge. A dedicated merge strategy operating
above the writer/reader level can drive per-segment raw copy for compatible
sections.

**Independent testability.** Each section can be unit tested in isolation by
testing the writer/reader created by a factory, without constructing full segments.

**Backward compatibility by design.** The read path resolves codecs via SPI using
the codec name stored in segment metadata. Adding a new codec does not affect
reading of older segments. Version gating lives in the format class at injection
time, not scattered through the base class.

**Format experimentation without risk.** Behind a feature flag, a new codec version
can experiment with a different encoding for one section while reusing default
factories for everything else. The blast radius is limited to the factory being
changed.
