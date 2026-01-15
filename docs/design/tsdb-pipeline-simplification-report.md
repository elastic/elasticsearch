# TSDB Pipeline Code Simplification Report

Generated: 2026-01-15

## Overview

This report contains potential simplifications identified across all 4 TSDB pipeline branches. Each suggestion has a unique code for easy reference.

---

## Branch 1: feature/tsdb-pipeline-metadata-buffer

### MB-01: Remove SINGLE_BYTE constant
**Priority:** Medium | **File:** `MetadataBuffer.java:37`

**Current:**
```java
private static final int SINGLE_BYTE = Byte.BYTES;
```

**Suggested:** Remove constant, use `1` directly in `writeByte()`:
```java
grow(1);
```

**Rationale:** `SINGLE_BYTE` is redundant since `Byte.BYTES` already conveys the meaning (equals 1). Used only once.

---

### MB-02: Remove redundant MAX_ZINT_BYTES constant
**Priority:** Medium | **File:** `MetadataBuffer.java:39`

**Current:**
```java
private static final int MAX_ZINT_BYTES = Integer.BYTES + 1;
```

**Suggested:** Remove `MAX_ZINT_BYTES` and use `MAX_VINT_BYTES` instead.

**Rationale:** Zig-zag encoding for integers produces values that fit in 32 bits, so max byte length equals VInt (5 bytes). Two constants with same value.

---

### MB-03: Review MAX_ZLONG_BYTES value
**Priority:** Low | **File:** `MetadataBuffer.java:41`

**Current:**
```java
private static final int MAX_ZLONG_BYTES = Long.BYTES + 2;  // = 10
```

**Suggested:** Consider using `MAX_VLONG_BYTES` (9 bytes) instead.

**Rationale:** Zig-zag encoded long is unsigned 64-bit, max encoded size is 9 bytes same as VLong. Value 10 is overly conservative.

---

### MB-04: Remove redundant size initialization
**Priority:** Low | **File:** `MetadataBuffer.java:51`

**Current:**
```java
public MetadataBuffer() {
    this.buffer = new byte[DEFAULT_CAPACITY_BYTES];
    this.size = 0;
}
```

**Suggested:**
```java
public MetadataBuffer() {
    this.buffer = new byte[DEFAULT_CAPACITY_BYTES];
}
```

**Rationale:** `int` fields auto-initialize to 0 in Java.

---

### MB-05: Extract ensureCapacity() helper method
**Priority:** Low | **File:** `MetadataBuffer.java:61-63,79-81,97-99,110-112,123-125`

**Current (repeated 5 times):**
```java
if (size + MAX_VINT_BYTES > buffer.length) {
    grow(MAX_VINT_BYTES);
}
```

**Suggested:**
```java
private void ensureCapacity(int additional) {
    if (size + additional > buffer.length) {
        grow(additional);
    }
}
```

**Rationale:** Pattern repeated 5 times. Helper method reduces duplication.

---

### MB-06: Inconsistent `final` on parameters
**Priority:** Low | **File:** `MetadataBuffer.java`

**Current:** Some method parameters have `final`, others don't.

**Suggested:** Either add `final` to all parameters or remove from all for consistency.

**Rationale:** Style inconsistency. Most Java codebases omit `final` on parameters.

---

### MB-07: Remove GIVEN/WHEN/THEN comments from tests
**Priority:** Low | **File:** `MetadataBufferTests.java` (throughout)

**Current:**
```java
public void testWriteRandomBytes() {
    // GIVEN
    final MetadataBuffer buffer = new MetadataBuffer();
    // WHEN
    ...
    // THEN
    assertEquals(...);
}
```

**Suggested:** Remove section comments. Test names and structure are self-documenting.

**Rationale:** Comments add visual noise (~25 occurrences). Pattern is clear without them.

---

### MB-08: Use assertArrayEquals instead of loops
**Priority:** Low | **File:** `MetadataBufferTests.java:34-36,51-54,305-307`

**Current:**
```java
for (int i = 0; i < numBytes; i++) {
    assertEquals(expected[i], bytes[i]);
}
```

**Suggested:**
```java
assertArrayEquals(expected, buffer.toByteArray());
```

**Rationale:** JUnit's `assertArrayEquals` is more concise with better error messages.

---

## Branch 2: feature/tsdb-encode-decode-contexts

### EC-01: Remove redundant null/zero initializations in EncodingContext
**Priority:** High | **File:** `EncodingContext.java:37-38`

**Current:**
```java
public EncodingContext() {
    this.buffer = new MetadataBuffer();
    this.stageCount = 0;
}
```

**Suggested:**
```java
public EncodingContext() {
    this.buffer = new MetadataBuffer();
}
```

**Rationale:** `int` auto-initializes to 0.

---

### EC-02: Remove redundant null/zero initializations in DecodingContext
**Priority:** High | **File:** `DecodingContext.java:39-42`

**Current:**
```java
public DecodingContext() {
    this.input = null;
    this.stageCount = 0;
}
```

**Suggested:**
```java
public DecodingContext() {
    // No initialization needed - reset() must be called before use
}
```

**Rationale:** Both fields auto-initialize to default values.

---

### EC-03: Verbose stageApplied() Javadoc
**Priority:** Low | **File:** `EncodingContext.java:86-97`

**Current:** 10-line Javadoc explaining atomicity and design rationale.

**Suggested:**
```java
/**
 * Records that a stage has been applied by writing its ID and incrementing the stage count.
 *
 * @param stageId the stage identifier byte
 */
```

**Rationale:** Implementation is simple enough to be self-documenting.

---

### EC-04: Remove unnecessary clear() calls in tests
**Priority:** Medium | **File:** `EncodingDecodingContextIntegrationTests.java:40,71,100,123`

**Current:**
```java
encodeCtx.clear();
encodeCtx.stageApplied(DELTA);
```

**Suggested:**
```java
encodeCtx.stageApplied(DELTA);
```

**Rationale:** Newly created `EncodingContext` is already cleared.

---

### EC-05: Consolidate duplicated size computation methods
**Priority:** Medium | **File:** `EncodingContextTests.java:302-322`

**Current:** Separate `computeVIntSize()` and `computeZLongSize()` with similar logic.

**Suggested:**
```java
private static int computeVarLongSize(long value) {
    int size = 1;
    while ((value & ~0x7FL) != 0) {
        size++;
        value >>>= 7;
    }
    return size;
}
```

**Rationale:** Core computation is duplicated. Extract shared helper.

---

### EC-06: Remove GIVEN/WHEN/THEN comments from tests
**Priority:** Low | **Files:** `EncodingContextTests.java`, `DecodingContextTests.java`, `EncodingDecodingContextIntegrationTests.java`

**Suggested:** Same as MB-07. Remove section comments from simple tests.

---

### EC-07: Simplify single-line tests
**Priority:** Low | **File:** `DecodingContextTests.java:19-28`

**Current:**
```java
public void testNewContextStartsWithZeroStages() {
    // GIVEN
    final DecodingContext context = new DecodingContext();
    // WHEN
    int stageCount = context.stageCount();
    // THEN
    assertEquals(0, stageCount);
}
```

**Suggested:**
```java
public void testNewContextStartsWithZeroStages() {
    assertEquals(0, new DecodingContext().stageCount());
}
```

---

## Branch 3: feature/tsdb-numeric-stages

### NS-01: Remove redundant comment in NumericCodecStage
**Priority:** High | **File:** `NumericCodecStage.java:25-27`

**Current:**
```java
public interface NumericCodecStage extends NumericEncoder, NumericDecoder {
    // Combined interface - inherits all methods from NumericEncoder and NumericDecoder
}
```

**Suggested:**
```java
public interface NumericCodecStage extends NumericEncoder, NumericDecoder {
}
```

**Rationale:** Comment restates what `extends` clause already shows.

---

### NS-02: Rename cryptic variables in DeltaCodecStage
**Priority:** Medium | **File:** `DeltaCodecStage.java:69-70`

**Current:**
```java
int gts = 0;
int lts = 0;
```

**Suggested:**
```java
int increasingCount = 0;
int decreasingCount = 0;
```

**Rationale:** `gts` (greater than) and `lts` (less than) are cryptic.

---

### NS-03: Simplify GCD skip condition
**Priority:** Low | **File:** `GcdCodecStage.java:77-79`

**Current:**
```java
if (Long.compareUnsigned(gcd, 1) <= 0) {
    return;
}
```

**Suggested:**
```java
if (gcd <= 1) {
    return;
}
```

**Rationale:** `MathUtil.gcd()` returns non-negative values. Unsigned comparison unnecessary for non-negative inputs.

---

### NS-04: Use static import for Matchers.containsString
**Priority:** Low | **Files:** `StageIdTests.java:69,78,87`, `NumericStagesTests.java:72,81,90`

**Current:**
```java
assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("Unknown stage ID"));
```

**Suggested:**
```java
import static org.hamcrest.Matchers.containsString;
// ...
assertThat(e.getMessage(), containsString("Unknown stage ID"));
```

---

### NS-05: Use Set for valid IDs in random test
**Priority:** Low | **File:** `StageIdTests.java:115-125`

**Current:**
```java
if (randomId == 0x01 || randomId == 0x02 || randomId == 0x03 || randomId == 0x20) {
    continue;
}
```

**Suggested:**
```java
Set<Byte> validIds = Set.of((byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x20);
if (validIds.contains(randomId)) {
    continue;
}
```

**Rationale:** Set makes valid IDs clearer and easier to maintain.

---

### NS-06: Extract common round-trip test pattern
**Priority:** Low | **Files:** `DeltaCodecStageTests.java`, `GcdCodecStageTests.java`, `OffsetCodecStageTests.java`

**Current:** Each test file has 3-4 nearly identical round-trip test methods.

**Suggested:** Extract `assertRoundTrip(long[] original)` helper method.

**Rationale:** Reduces duplication, makes test intent clearer.

---

## Branch 4: feature/tsdb-wire-format

### WF-01: Remove unnecessary comment in WireFormatStage.decode()
**Priority:** Low | **File:** `WireFormatStage.java:94`

**Current:**
```java
@Override
public void decode(long[] values, int valueCount, DataInput in, DecodingContext context) throws IOException {
    // No assertion needed - values parameter is unused (documented as "ignored")
    int numStages = in.readVInt();
```

**Suggested:** Remove the comment. Javadoc already documents that `values` is ignored.

---

### WF-02: Remove tautological assertSame in LongArrayPayloadWriterTests
**Priority:** Low | **File:** `LongArrayPayloadWriterTests.java:20-23`

**Current:**
```java
public void testSingletonInstance() {
    assertNotNull(LongArrayPayloadWriter.INSTANCE);
    assertSame(LongArrayPayloadWriter.INSTANCE, LongArrayPayloadWriter.INSTANCE);
}
```

**Suggested:**
```java
public void testSingletonInstance() {
    assertNotNull(LongArrayPayloadWriter.INSTANCE);
}
```

**Rationale:** `assertSame(X, X)` is tautologically true.

---

### WF-03: Remove tautological assertSame in WireFormatStageTests
**Priority:** Low | **File:** `WireFormatStageTests.java:32-35`

**Current:**
```java
public void testSingletonInstance() {
    assertNotNull(WireFormatStage.INSTANCE);
    assertSame(WireFormatStage.INSTANCE, WireFormatStage.INSTANCE);
}
```

**Suggested:** Same as WF-02.

---

### WF-04: Extract decoding loop helper in WireFormatStageTests
**Priority:** Medium | **File:** `WireFormatStageTests.java:150-158,273-281,311-318`

**Current:** Same decoding loop with switch statement appears 3 times.

**Suggested:**
```java
private void decodeAllStages(
    long[] decoded,
    ByteArrayDataInput in,
    DecodingContext decContext,
    BitPackCodecStage bitPackStage
) throws IOException {
    for (int s = 0; s < decContext.stageCount(); s++) {
        byte stageId = decContext.readByte();
        switch (StageId.fromId(stageId)) {
            case BIT_PACK -> bitPackStage.decode(decoded, decoded.length, in, decContext);
            case GCD -> GcdCodecStage.INSTANCE.decode(decoded, decoded.length, in, decContext);
            case OFFSET -> OffsetCodecStage.INSTANCE.decode(decoded, decoded.length, in, decContext);
            case DELTA -> DeltaCodecStage.INSTANCE.decode(decoded, decoded.length, in, decContext);
        }
    }
}
```

---

## Summary by Priority

### High Priority
| Code | Description |
|------|-------------|
| EC-01 | Remove redundant initialization in EncodingContext |
| EC-02 | Remove redundant initialization in DecodingContext |
| NS-01 | Remove redundant comment in NumericCodecStage |

### Medium Priority
| Code | Description |
|------|-------------|
| MB-01 | Remove SINGLE_BYTE constant |
| MB-02 | Remove redundant MAX_ZINT_BYTES constant |
| EC-04 | Remove unnecessary clear() calls in tests |
| EC-05 | Consolidate duplicated size computation methods |
| NS-02 | Rename gts/lts to increasingCount/decreasingCount |
| WF-04 | Extract decoding loop helper |

### Low Priority
| Code | Description |
|------|-------------|
| MB-03 | Review MAX_ZLONG_BYTES value (10 vs 9) |
| MB-04 | Remove redundant size initialization |
| MB-05 | Extract ensureCapacity() helper |
| MB-06 | Inconsistent final on parameters |
| MB-07 | Remove GIVEN/WHEN/THEN comments |
| MB-08 | Use assertArrayEquals |
| EC-03 | Verbose stageApplied() Javadoc |
| EC-06 | Remove GIVEN/WHEN/THEN from context tests |
| EC-07 | Simplify single-line tests |
| NS-03 | Simplify GCD skip condition |
| NS-04 | Use static import for Matchers |
| NS-05 | Use Set for valid IDs |
| NS-06 | Extract round-trip test pattern |
| WF-01 | Remove comment in WireFormatStage |
| WF-02 | Remove tautological assertSame (PayloadWriter) |
| WF-03 | Remove tautological assertSame (WireFormat) |

---

## Quick Reference

To apply a simplification, tell me the code (e.g., "Apply MB-01" or "Apply EC-01, EC-02, NS-01").
