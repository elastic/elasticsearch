## Summary

Introduces the `ES94` TSDB doc values format as a drop-in replacement for `ES819`. The only difference is that numeric encoding uses a composable pipeline of stages (`delta>offset>gcd>bitpack`) via the pipeline framework from [#143589](https://github.com/elastic/elasticsearch/pull/143589) and [#143934](https://github.com/elastic/elasticsearch/pull/143934), instead of the monolithic `TSDBDocValuesEncoder`. All non-numeric field types (binary, sorted, sorted-set) are handled identically to ES819 by the shared abstract base classes from [#144324](https://github.com/elastic/elasticsearch/pull/144324).

The ES94 codec is not wired into `PerFieldFormatSupplier` yet — no index will use it until the wiring and gating PR lands. This PR focuses on the format implementation, entry factory refactoring, and test infrastructure.

### What's included

**ES94 format**: `ES94TSDBDocValuesFormat`, `ES94TSDBDocValuesConsumer`, `ES94TSDBDocValuesProducer` with the same configuration surface as ES819 (optimized merge, binary compression, block sizes, prefix partitions). Each numeric field writes a self-describing `FieldDescriptor` so decoders reconstruct themselves from segment metadata.

**Pipeline encode/decode**: `NumericEncodePipeline`, `NumericDecodePipeline`, `NumericBlockEncoder`/`NumericBlockDecoder`, `NumericCodecFactory`, `StageFactory`, `TransformEncoder`/`TransformDecoder` — the runtime that connects the pipeline framework to the doc values consumer/producer.

**Entry factory**: `EntryFactory` interface with default methods for all entry types. Each codec provides its own factory returning codec-specific entry subclasses. `PrefixPartitionedEntry` now encapsulates its own partition reading and access, eliminating `instanceof` checks from the base class.

**Test restructuring**: Shared TSDB tests extracted into `AbstractTSDBDocValuesFormatTestCase` extending Lucene's `BaseDocValuesFormatTestCase` directly. Both `ES819TSDBDocValuesFormatTests` and `ES94TSDBDocValuesFormatTests` extend this base independently — no codec test depends on another. Adding a new codec version (e.g. ES95) requires only implementing `getCodec()` and `createDocValuesFormat()` to inherit ~158 tests covering all doc value types, merge paths, block loaders, prefix partitions, and cross-format compatibility.

### Testing

```
./gradlew :server:test --tests "*ES94TSDBDocValuesFormatTests*"
./gradlew :server:test --tests "*ES819TSDBDocValuesFormatTests*"
./gradlew :server:test --tests "*ES87TSDBDocValuesFormatTests*"
./gradlew :server:test --tests "*PrefixPartitionTests*"
```
