## fix(mapper): handle VOID entries in _ignored_source with FLS

### Problem

When Field Level Security (FLS) is active on an index using synthetic source, any field with a `copy_to` target causes search requests to fail with:

```
[-1:12] Unexpected Break (0xFF) token in definite length (-1) Object at [Source: (byte[])[12 bytes]; byte offset: #12]
```

The failure path is:

1. During indexing, `copy_to` target fields are recorded in `_ignored_source` as **VOID entries** (`XContentDataHelper.voidValue()`): markers indicating the field exists but its data is stored elsewhere (in the `copy_to` source field).
2. When a query runs with FLS enabled, `FieldSubsetReader` iterates over `_ignored_source` stored fields and calls `IgnoredSourceFieldMapper.decodeAsMap` for each entry to decide which fields to keep or strip based on FLS access permissions.
3. `decodeAsMap` builds a CBOR object containing the field name but writes no value for VOID entries (`XContentDataHelper.decodeAndWrite` is a no-op). When `XContentHelper.convertToMap` tries to parse this malformed CBOR, the Jackson CBOR parser encounters an unexpected Break token (`0xFF`) and throws.

### Fix

The core fix is in `nameValueToMapped()`, which both encoding paths share. It now checks `XContentDataHelper.isDataPresent(nameValue.value())` before attempting to parse, returning `null` for VOID entries instead of building malformed CBOR.

Both encoding paths call `nameValueToMapped()` through their respective `decodeAsMap` methods, so both are protected by the same check. The only difference is how the `null` propagates:

1. **Coalesced path**: `CoalescedIgnoredSourceEncoding.decodeAsMap` iterates over entries and skips `null` results, filtering VOID entries out of the returned list.

2. **Legacy path**: `LegacyIgnoredSourceEncoding.decodeAsMap` returns `null` directly, and `filterValue` propagates the `null` to `FieldSubsetReader`, which drops the entry.

In both cases, VOID entries carry no field data, so omitting them is correct.

### Tests

- **`testCoalescedDecodeAsMapReturnsNullForVoidEntry`**: unit test verifying `CoalescedIgnoredSourceEncoding.decodeAsMap` filters out VOID entries, returning an empty list.
- **`testLegacyDecodeAsMapReturnsNullForVoidEntry`**: unit test verifying `LegacyIgnoredSourceEncoding.decodeAsMap` returns `null` for a VOID entry.
- **`testSyntheticSourceWithCopyToAndFLSCoalesced`**: integration test exercising `FieldSubsetReader` with `copy_to`, synthetic source, and FLS using the coalesced encoding format.
- **`testSyntheticSourceWithCopyToAndFLSLegacy`**: same integration test but using the legacy encoding format (pre-`IGNORED_SOURCE_COALESCED_ENTRIES_WITH_FF`).
- **YAML REST tests**: two end-to-end scenarios: one with API key FLS + synthetic source + `copy_to`, and one covering the `skip_ignored_source_read` workaround.

```
./gradlew :server:test --tests "org.elasticsearch.index.mapper.IgnoredSourceFieldMapperTests.testCoalescedDecodeAsMapReturnsNullForVoidEntry"
./gradlew :server:test --tests "org.elasticsearch.index.mapper.IgnoredSourceFieldMapperTests.testLegacyDecodeAsMapReturnsNullForVoidEntry"
./gradlew :x-pack:plugin:core:test --tests "org.elasticsearch.xpack.core.security.authz.accesscontrol.FieldSubsetReaderTests.testSyntheticSourceWithCopyToAndFLS*"
./gradlew :x-pack:plugin:yamlRestTest -Dtests.method="test {p0=security/authz_api_keys/30_field_level_security_synthetic_source/*copy_to*}"
```
