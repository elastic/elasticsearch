/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.escf.EscfColumnBuilder.CollisionPolicy;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link EscfColumnBuilder}, covering both {@link CollisionPolicy}s: the
 * {@link CollisionPolicy#SPLIT} append surface (kind selection, the lazily-allocated validity/absent
 * bitset, scalar&rarr;union promotion), the {@link CollisionPolicy#MERGE} positional surface
 * (scalar&harr;array promotion), the columnar&rarr;union rewrite, and hints. Scalar/union behavior that
 * involves no scalar&harr;array collision is identical under either policy and runs under both.
 */
public class EscfColumnBuilderTests extends ESTestCase {

    public void testLongKindSelectionAndValues() {
        EscfColumnBuilder b = builder();
        b.addLong(1);
        b.addLong(2);
        b.addLong(3);
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.LONG, data.kind());
        assertNull("dense column has no validity bitset", data.validity());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(1L, col.getLongValue(0));
        assertEquals(2L, col.getLongValue(1));
        assertEquals(3L, col.getLongValue(2));
        assertEquals(SourceValueType.LONG, col.getTypeByte(0));
    }

    public void testValidityBitsetOnlyWhenAbsent() {
        EscfColumnBuilder b = builder();
        b.addLong(10);
        b.addAbsent();
        b.addLong(30);
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.LONG, data.kind());
        assertNotNull("a column with an absent row carries a validity bitset", data.validity());
        EscfColumn col = EscfColumn.from(data);
        assertFalse(col.isAbsent(0));
        assertTrue(col.isAbsent(1));
        assertFalse(col.isAbsent(2));
        assertEquals(10L, col.getLongValue(0));
        assertEquals(30L, col.getLongValue(2));
    }

    public void testStringKind() {
        EscfColumnBuilder b = builder();
        b.addString(utf8("alpha"));
        b.addString(utf8("gamma"));
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.STRING, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals("alpha", col.getStringValue(0).string());
        assertEquals("gamma", col.getStringValue(1).string());
    }

    public void testBoolKind() {
        EscfColumnBuilder b = builder();
        b.addBoolean(true);
        b.addBoolean(false);
        b.addBoolean(true);
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.BOOL, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertTrue(col.getBooleanValue(0));
        assertFalse(col.getBooleanValue(1));
        assertTrue(col.getBooleanValue(2));
        assertEquals(SourceValueType.TRUE, col.getTypeByte(0));
        assertEquals(SourceValueType.FALSE, col.getTypeByte(1));
    }

    public void testPromoteOnTypeConflictPreservesValues() {
        EscfColumnBuilder b = builder();
        b.addLong(7);
        b.addString(utf8("text"));
        b.addDouble(2.5);
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.LONG, col.getTypeByte(0));
        assertEquals(7L, col.getLongValue(0));
        assertEquals(SourceValueType.STRING, col.getTypeByte(1));
        assertEquals("text", col.getStringValue(1).string());
        assertEquals(SourceValueType.DOUBLE, col.getTypeByte(2));
        assertEquals(2.5, col.getDoubleValue(2), 0.0);
    }

    public void testExplicitNullPromotesToUnion() {
        EscfColumnBuilder b = builder();
        b.addLong(1);
        b.addNull();
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(1L, col.getLongValue(0));
        assertTrue(col.isNull(1));
        assertEquals(SourceValueType.NULL, col.getTypeByte(1));
    }

    public void testAllAbsentFinishesAsLong() {
        EscfColumnBuilder b = builder();
        b.addAbsent();
        b.addAbsent();
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.LONG, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertTrue(col.isAbsent(0));
        assertTrue(col.isAbsent(1));
        assertEquals(SourceValueType.ABSENT, col.getTypeByte(0));
    }

    public void testDenseColumnKeepsNullValidity() {
        EscfColumnBuilder b = builder();
        b.addLong(1);
        b.addLong(2);
        b.addLong(3);
        EscfColumnData data = b.finish(3);
        assertNull("dense column must have null validity (all-present shortcut)", data.validity());

        EscfColumn col = EscfColumn.from(data);
        assertNull("after from(), dense column still has null validity", col.validity);

        // Slice a dense column: the window must also be null.
        EscfColumn slice = col.sliceInternal(1, 2);
        assertNull("slicing a dense column produces a null validity", slice.validity);

        // Round-trip through codec; the validity stays null.
        EscfColumnData sliceData = slice.toColumnData();
        assertNull("dense column data after toColumnData() has null validity", sliceData.validity());
        EscfColumn reparsed = EscfColumn.from(sliceData);
        assertNull("reparsed dense column has null validity", reparsed.validity);
    }

    public void testValidityBitsetBackfillOnFirstAbsent() {
        // Pattern: 3 present, 1 absent, 1 present.
        EscfColumnBuilder b = builder();
        b.addLong(10);
        b.addLong(20);
        b.addLong(30);
        b.addAbsent();
        b.addLong(50);
        EscfColumnData data = b.finish(5);

        assertNotNull("validity must be set once an absent appears", data.validity());
        // Under Arrow validity, bit set = present.
        assertTrue("doc 0 (present before absent) must have its bit set", data.validity().get(0));
        assertTrue("doc 1 (present before absent) must have its bit set", data.validity().get(1));
        assertTrue("doc 2 (present before absent) must have its bit set", data.validity().get(2));
        assertFalse("doc 3 (absent) must have its bit clear", data.validity().get(3));
        assertTrue("doc 4 (present after absent) must have its bit set", data.validity().get(4));

        // The column view must agree with the validity bitset.
        EscfColumn col = EscfColumn.from(data);
        assertFalse(col.isAbsent(0));
        assertFalse(col.isAbsent(1));
        assertFalse(col.isAbsent(2));
        assertTrue(col.isAbsent(3));
        assertFalse(col.isAbsent(4));
    }

    public void testTrailingAbsentRoundTrip() {
        EscfColumnBuilder b = builder();
        b.addLong(100);
        b.addLong(200);
        b.addAbsent(); // last doc
        EscfColumnData data = b.finish(3);

        assertNotNull(data.validity());
        assertTrue(data.validity().get(0));
        assertTrue(data.validity().get(1));
        assertFalse(data.validity().get(2));

        // Round-trip via EscfColumn.from (which calls windowValidity) and back to EscfColumnData.
        EscfColumn col = EscfColumn.from(data);
        assertFalse(col.isAbsent(0));
        assertFalse(col.isAbsent(1));
        assertTrue(col.isAbsent(2));

        EscfColumnData roundTripped = col.toColumnData();
        assertNotNull("trailing-absent column must keep a validity bitset", roundTripped.validity());
        EscfColumn reparsed = EscfColumn.from(roundTripped);
        assertFalse(reparsed.isAbsent(0));
        assertFalse(reparsed.isAbsent(1));
        assertTrue(reparsed.isAbsent(2));
    }

    public void testLongThenBooleanPromotesToUnion() {
        for (CollisionPolicy policy : CollisionPolicy.values()) {
            EscfColumnBuilder b = new EscfColumnBuilder(policy);
            b.addLong(9);      // doc 0: "field": 9
            b.addBoolean(true); // doc 1: "field": true
            EscfColumnData data = b.finish(2);
            assertEquals("policy " + policy, EscfColumnKind.UNION, data.kind());
            EscfColumn col = EscfColumn.from(data);
            assertEquals(SourceValueType.LONG, col.getTypeByte(0));
            assertEquals(9L, col.getLongValue(0));
            assertEquals(SourceValueType.TRUE, col.getTypeByte(1));
            assertTrue(col.getBooleanValue(1));
        }
    }

    public void testBooleanThenLongPromotesToUnion() {
        for (CollisionPolicy policy : CollisionPolicy.values()) {
            EscfColumnBuilder b = new EscfColumnBuilder(policy);
            b.addBoolean(false); // doc 0: "field": false
            b.addLong(9);        // doc 1: "field": 9
            EscfColumnData data = b.finish(2);
            assertEquals("policy " + policy, EscfColumnKind.UNION, data.kind());
            EscfColumn col = EscfColumn.from(data);
            assertEquals(SourceValueType.FALSE, col.getTypeByte(0));
            assertFalse(col.getBooleanValue(0));
            assertEquals(SourceValueType.LONG, col.getTypeByte(1));
            assertEquals(9L, col.getLongValue(1));
        }
    }

    // ── MERGE policy: positional surface, scalar↔array promotion ──

    public void testMergeStringsDenseAllPresent() {
        EscfColumnBuilder b = mergeBuilder();
        b.setString(0, bytesRef("a"));
        b.setString(1, bytesRef("b"));
        b.setString(2, bytesRef("c"));
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.STRING, data.kind());
        assertNull("dense column should have null validity", data.validity());
        assertEquals("a", readScalarString(data, 0));
        assertEquals("b", readScalarString(data, 1));
        assertEquals("c", readScalarString(data, 2));
    }

    public void testMergeStringsLeadingAbsentRows() {
        EscfColumnBuilder b = mergeBuilder();
        b.setString(2, bytesRef("last"));
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.STRING, data.kind());
        assertTrue(EscfColumn.from(data).isAbsent(0));
        assertTrue(EscfColumn.from(data).isAbsent(1));
        assertFalse(EscfColumn.from(data).isAbsent(2));
        assertEquals("last", readScalarString(data, 2));
    }

    public void testMergeStringsTrailingAbsentRows() {
        EscfColumnBuilder b = mergeBuilder();
        b.setString(0, bytesRef("foo"));
        b.setString(2, bytesRef("bar"));
        EscfColumnData data = b.finish(5);
        assertEquals(EscfColumnKind.STRING, data.kind());
        assertFalse(EscfColumn.from(data).isAbsent(0));
        assertEquals("foo", readScalarString(data, 0));
        assertTrue(EscfColumn.from(data).isAbsent(1));
        assertEquals("bar", readScalarString(data, 2));
        assertTrue(EscfColumn.from(data).isAbsent(3));
        assertTrue(EscfColumn.from(data).isAbsent(4));
    }

    public void testMergeStringsMultiValuePromotesToArray() {
        EscfColumnBuilder b = mergeBuilder();
        b.setString(0, bytesRef("x"));
        b.setString(0, bytesRef("y")); // second element for doc 0 → ARRAY
        b.setString(1, bytesRef("z"));
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals("x", readArrayElem(data, 0, 0));
        assertEquals("y", readArrayElem(data, 0, 1));
        assertEquals(1, elemCount(data, 1));
        assertEquals("z", readArrayElem(data, 1, 0));
    }

    public void testMergeStringArrayElementLayout() {
        EscfColumnBuilder b = mergeBuilder();
        b.setString(0, bytesRef("a"));
        b.setString(1, bytesRef("b"));
        b.setString(1, bytesRef("c")); // second element → ARRAY
        b.setString(2, bytesRef("d"));
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(1, elemCount(data, 0));
        assertEquals("a", readArrayElem(data, 0, 0));
        assertEquals(2, elemCount(data, 1));
        assertEquals("b", readArrayElem(data, 1, 0));
        assertEquals("c", readArrayElem(data, 1, 1));
        assertEquals(1, elemCount(data, 2));
        assertEquals("d", readArrayElem(data, 2, 0));
    }

    public void testMergeLongSingleValueDense() {
        EscfColumnBuilder b = mergeBuilder();
        b.setLong(0, 42L);
        b.setLong(1, -1L);
        b.setLong(2, Long.MAX_VALUE);
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.LONG, data.kind());
        assertNull("dense", data.validity());
        assertEquals(42L, readScalarLong(data, 0));
        assertEquals(-1L, readScalarLong(data, 1));
        assertEquals(Long.MAX_VALUE, readScalarLong(data, 2));
    }

    public void testMergeLongWithAbsentDocs() {
        EscfColumnBuilder b = mergeBuilder();
        b.setLong(1, 99L);
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.LONG, data.kind());
        assertNotNull("should have validity for absent docs", data.validity());
        assertTrue(EscfColumn.from(data).isAbsent(0));
        assertFalse(EscfColumn.from(data).isAbsent(1));
        assertEquals(99L, readScalarLong(data, 1));
        assertTrue(EscfColumn.from(data).isAbsent(2));
    }

    public void testMergeLongMultiValuePromotesToArray() {
        EscfColumnBuilder b = mergeBuilder();
        b.setLong(0, 1L);
        b.setLong(0, 2L);
        EscfColumnData data = b.finish(1);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(EscfColumnKind.LONG, data.child().kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals(1L, readArrayLong(data, 0, 0));
        assertEquals(2L, readArrayLong(data, 0, 1));
    }

    /** ARRAY[LONG] across docs, with an absent doc after the promotion and a second multi-value row. */
    public void testMergeLongMultiDocArrayValues() {
        EscfColumnBuilder b = mergeBuilder();
        b.setLong(0, 10L);
        b.setLong(0, 20L); // triggers promotion (dense zero-copy at this point)
        b.setLong(1, 30L);
        // doc 2 absent (skipped)
        b.setLong(3, 40L);
        b.setLong(3, 50L);
        EscfColumnData data = b.finish(4);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals(10L, readArrayLong(data, 0, 0));
        assertEquals(20L, readArrayLong(data, 0, 1));
        assertEquals(1, elemCount(data, 1));
        assertEquals(30L, readArrayLong(data, 1, 0));
        assertEquals(0, elemCount(data, 2)); // absent
        assertEquals(2, elemCount(data, 3));
        assertEquals(40L, readArrayLong(data, 3, 0));
        assertEquals(50L, readArrayLong(data, 3, 1));
    }

    /** A scalar column with an interior absent, then a same-row second value: exercises the sparse compacting swap. */
    public void testMergeLongSparsePromotionCompacts() {
        EscfColumnBuilder b = mergeBuilder();
        b.setLong(0, 10L);
        // doc 1 absent
        b.setLong(2, 30L);
        b.setLong(2, 31L); // second element for doc 2 → promote (sparse: doc 1 absent must compact out)
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(1, elemCount(data, 0));
        assertEquals(10L, readArrayLong(data, 0, 0));
        assertEquals(0, elemCount(data, 1)); // absent → empty range
        assertEquals(2, elemCount(data, 2));
        assertEquals(30L, readArrayLong(data, 2, 0));
        assertEquals(31L, readArrayLong(data, 2, 1));
    }

    public void testMergeDoubleMultiValuePromotesToArray() {
        EscfColumnBuilder b = mergeBuilder();
        b.setDouble(0, 1.0);
        b.setDouble(0, 2.0);
        EscfColumnData data = b.finish(1);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(EscfColumnKind.DOUBLE, data.child().kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals(1.0, readArrayDouble(data, 0, 0), 0.0);
        assertEquals(2.0, readArrayDouble(data, 0, 1), 0.0);
    }

    public void testMergeBinaryMultiValuePromotesToArray() {
        EscfColumnBuilder b = mergeBuilder();
        b.setBinary(0, new BytesRef(new byte[] { 0xA }));
        b.setBinary(0, new BytesRef(new byte[] { 0xB }));
        EscfColumnData data = b.finish(1);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(2, elemCount(data, 0));
    }

    public void testMergeNonDecreasingRowAssertion() {
        EscfColumnBuilder b = mergeBuilder();
        b.setLong(2, 0L);
        expectThrows(AssertionError.class, () -> b.setLong(1, 0L));
    }

    // ── Element-append array surface ──

    public void testElementAppendLongArray() {
        EscfColumnBuilder b = mergeBuilder();
        b.beginArray(0);
        b.appendLong(1L);
        b.appendLong(2L);
        b.appendLong(3L);
        b.endArray();
        b.beginArray(1);
        b.appendLong(4L);
        b.endArray();
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(EscfColumnKind.LONG, data.child().kind());
        assertEquals(3, elemCount(data, 0));
        assertEquals(1L, readArrayLong(data, 0, 0));
        assertEquals(3L, readArrayLong(data, 0, 2));
        assertEquals(1, elemCount(data, 1));
        assertEquals(4L, readArrayLong(data, 1, 0));
    }

    public void testElementAppendStringArrayWithGap() {
        EscfColumnBuilder b = mergeBuilder();
        b.beginArray(0);
        b.appendString(bytesRef("a"));
        b.appendString(bytesRef("b"));
        b.endArray();
        // row 1 absent
        b.beginArray(2);
        b.appendString(bytesRef("c"));
        b.endArray();
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals("a", readArrayElem(data, 0, 0));
        assertEquals("b", readArrayElem(data, 0, 1));
        assertEquals(0, elemCount(data, 1));
        assertEquals(1, elemCount(data, 2));
        assertEquals("c", readArrayElem(data, 2, 0));
    }

    // ── Phase 3: columnar → UNION rewrite ──

    /** SPLIT: an array row then a scalar row → UNION, the array row rewritten to an inline FIXED_ARRAY. */
    public void testSplitArrayThenScalarRewritesToUnion() {
        EscfColumnBuilder b = builder();
        b.beginArray(0);
        b.appendLong(1);
        b.appendLong(2);
        b.endArray();
        b.addLong(9); // row 1 scalar → rewrite
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.FIXED_ARRAY, col.getTypeByte(0));
        assertEquals(List.of(1L, 2L), unionArrayLongs(col, 0));
        assertEquals(SourceValueType.LONG, col.getTypeByte(1));
        assertEquals(9L, col.getLongValue(1));
    }

    /** SPLIT: a scalar row then an array row → UNION, the scalar preserved as a scalar slot. */
    public void testSplitScalarThenArrayPromotesToUnion() {
        EscfColumnBuilder b = builder();
        b.addLong(9); // row 0 scalar
        b.beginArray(1);
        b.appendLong(1);
        b.appendLong(2);
        b.endArray();
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.LONG, col.getTypeByte(0));
        assertEquals(9L, col.getLongValue(0));
        assertEquals(SourceValueType.UNION_ARRAY, col.getTypeByte(1));
        assertEquals(List.of(1L, 2L), unionArrayLongs(col, 1));
    }

    /** An array-of-long row then an array-of-double row (child-kind change) → UNION. */
    public void testArrayChildKindChangeRewritesToUnion() {
        for (CollisionPolicy policy : CollisionPolicy.values()) {
            EscfColumnBuilder b = new EscfColumnBuilder(policy);
            b.beginArray(0);
            b.appendLong(1);
            b.appendLong(2);
            b.endArray();
            b.beginArray(1);
            b.appendDouble(1.5);
            b.endArray();
            EscfColumnData data = b.finish(2);
            assertEquals("policy " + policy, EscfColumnKind.UNION, data.kind());
            EscfColumn col = EscfColumn.from(data);
            assertEquals(SourceValueType.FIXED_ARRAY, col.getTypeByte(0));
            assertEquals(List.of(1L, 2L), unionArrayLongs(col, 0));
            ArrayReader r = col.getArrayValue(1);
            assertTrue(r.next());
            assertEquals(1.5, r.doubleValue(), 0.0);
            assertFalse(r.next());
        }
    }

    /** An array row then an explicit null → UNION. */
    public void testArrayThenNullRewritesToUnion() {
        EscfColumnBuilder b = builder();
        b.beginArray(0);
        b.appendLong(1);
        b.endArray();
        b.addNull();
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.FIXED_ARRAY, col.getTypeByte(0));
        assertEquals(List.of(1L), unionArrayLongs(col, 0));
        assertTrue(col.isNull(1));
    }

    /** An array row then a key-value → UNION. */
    public void testArrayThenKeyValueRewritesToUnion() {
        EscfColumnBuilder b = builder();
        b.beginArray(0);
        b.appendLong(1);
        b.endArray();
        b.addKeyValue(new byte[0]); // empty object
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.FIXED_ARRAY, col.getTypeByte(0));
        assertEquals(SourceValueType.KEY_VALUE, col.getTypeByte(1));
    }

    /** A string array is rewritten with per-element length framing. */
    public void testStringArrayRewriteToUnion() {
        EscfColumnBuilder b = builder();
        b.beginArray(0);
        b.appendString(bytesRef("alpha"));
        b.appendString(bytesRef("beta"));
        b.endArray();
        b.addNull();
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.FIXED_ARRAY, col.getTypeByte(0));
        ArrayReader r = col.getArrayValue(0);
        assertTrue(r.next());
        assertEquals("alpha", r.textValue().string());
        assertTrue(r.next());
        assertEquals("beta", r.textValue().string());
        assertFalse(r.next());
    }

    /** Mid-array heterogeneity via element-append: appendLong then appendString in one cell → UNION_ARRAY row. */
    public void testMidArrayHeterogeneityBecomesUnionArray() {
        EscfColumnBuilder b = builder();
        b.beginArray(0);
        b.appendLong(1);
        b.appendString(bytesRef("x"));
        b.endArray();
        EscfColumnData data = b.finish(1);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.UNION_ARRAY, col.getTypeByte(0));
        ArrayReader r = col.getArrayValue(0);
        assertTrue(r.next());
        assertEquals(SourceValueType.LONG, r.type());
        assertEquals(1L, r.longValue());
        assertTrue(r.next());
        assertEquals(SourceValueType.STRING, r.type());
        assertEquals("x", r.textValue().string());
        assertFalse(r.next());
    }

    /** Mid-array heterogeneity with a prior committed row: the earlier row is rewritten, the open row inlined. */
    public void testMidArrayHeterogeneityWithPriorRow() {
        EscfColumnBuilder b = builder();
        b.beginArray(0);
        b.appendLong(10);
        b.appendLong(20);
        b.endArray();
        b.beginArray(1);
        b.appendLong(1);
        b.appendString(bytesRef("x"));
        b.endArray();
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.FIXED_ARRAY, col.getTypeByte(0));
        assertEquals(List.of(10L, 20L), unionArrayLongs(col, 0));
        assertEquals(SourceValueType.UNION_ARRAY, col.getTypeByte(1));
        ArrayReader r = col.getArrayValue(1);
        assertTrue(r.next());
        assertEquals(1L, r.longValue());
        assertTrue(r.next());
        assertEquals("x", r.textValue().string());
        assertFalse(r.next());
    }

    /** MERGE: an array row then a compatible scalar stays a typed ARRAY (scalar = 1-element row). */
    public void testMergeArrayThenScalarStaysArray() {
        EscfColumnBuilder b = mergeBuilder();
        b.beginArray(0);
        b.appendLong(1);
        b.appendLong(2);
        b.endArray();
        b.addLong(9); // row 1 scalar, same child kind → stays ARRAY
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals(1, elemCount(data, 1));
        assertEquals(9L, readArrayLong(data, 1, 0));
    }

    /** A column of only empty arrays and absents finishes as UNION. */
    public void testOnlyEmptyArraysFinishesAsUnion() {
        EscfColumnBuilder b = builder();
        b.beginArray(0);
        b.endArray(); // []
        b.addAbsent();
        b.beginArray(2);
        b.endArray(); // []
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.UNION_ARRAY, col.getTypeByte(0));
        assertFalse(col.getArrayValue(0).next()); // empty
        assertTrue(col.isAbsent(1));
        assertEquals(SourceValueType.UNION_ARRAY, col.getTypeByte(2));
    }

    /** An empty array followed by a non-empty array resolves the child kind → typed ARRAY. */
    public void testEmptyArrayThenNonEmptyStaysArray() {
        EscfColumnBuilder b = builder();
        b.beginArray(0);
        b.endArray(); // []
        b.beginArray(1);
        b.appendLong(1);
        b.appendLong(2);
        b.endArray();
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(EscfColumnKind.LONG, data.child().kind());
        assertEquals(0, elemCount(data, 0)); // empty array (present)
        assertEquals(2, elemCount(data, 1));
        assertEquals(1L, readArrayLong(data, 1, 0));
    }

    // ── Phase 4: hints ──

    public void testHintScalarThenValue() {
        EscfColumnBuilder b = builder();
        b.hintScalar(EscfColumnKind.LONG);
        assertTrue("a hint does not count as a value", b.isEmpty());
        b.addLong(5);
        b.addLong(6);
        assertFalse(b.isEmpty());
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.LONG, data.kind());
        assertEquals(5L, EscfColumn.from(data).getLongValue(0));
    }

    public void testHintScalarWrongKindFallsBackToUnion() {
        EscfColumnBuilder b = builder();
        b.hintScalar(EscfColumnKind.LONG);
        b.addLong(5);
        b.addString(utf8("x")); // diverges from the hint → normal promotion still fires
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(5L, col.getLongValue(0));
        assertEquals("x", col.getStringValue(1).string());
    }

    public void testHintArrayForcesArrayColumn() {
        // A single value under hintArray still finishes as ARRAY (one-element row).
        EscfColumnBuilder b = mergeBuilder();
        b.hintArray(EscfColumnKind.LONG);
        b.setLong(0, 42);
        EscfColumnData data = b.finish(1);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(1, elemCount(data, 0));
        assertEquals(42L, readArrayLong(data, 0, 0));
    }

    public void testHintUnionStartsUnion() {
        EscfColumnBuilder b = builder();
        b.hintUnion();
        b.addLong(5);
        b.addLong(6);
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(5L, col.getLongValue(0));
        assertEquals(6L, col.getLongValue(1));
    }

    public void testUnusedHintIsEmpty() {
        EscfColumnBuilder b = builder();
        b.hintScalar(EscfColumnKind.STRING);
        assertTrue(b.isEmpty());
        EscfColumnData data = b.finish(3); // all absent
        assertEquals(3, data.docCount());
        assertTrue(EscfColumn.from(data).isAbsent(0));
        b.discard();
    }

    /**
     * Writes a single value at a row with a large leading gap and a large interior gap, then verifies the
     * resulting validity bitset and values across SPLIT and MERGE policies and across builder kinds (LONG,
     * STRING, BOOL). This exercises the {@code fillGapTo} bulk-absent path that was previously a per-row
     * loop and is now a single {@code addAbsents(gap)} call.
     */
    public void testFillGapToUsesBulkAbsent() {
        for (CollisionPolicy policy : CollisionPolicy.values()) {
            // LONG builder: gap of 100 absent rows, then one present row, then 50 absent rows, then one more.
            {
                EscfColumnBuilder b = new EscfColumnBuilder(policy);
                b.setLong(100, 42L); // rows 0–99 absent, row 100 present
                b.setLong(151, 99L); // rows 101–150 absent, row 151 present
                EscfColumnData data = b.finish(152);
                assertEquals(EscfColumnKind.LONG, data.kind());
                EscfColumn col = EscfColumn.from(data);
                for (int r = 0; r < 100; r++) {
                    assertTrue("row " + r + " should be absent [" + policy + "]", col.isAbsent(r));
                }
                assertFalse("row 100 should be present [" + policy + "]", col.isAbsent(100));
                assertEquals(42L, col.getLongValue(100));
                for (int r = 101; r < 151; r++) {
                    assertTrue("row " + r + " should be absent [" + policy + "]", col.isAbsent(r));
                }
                assertFalse("row 151 should be present [" + policy + "]", col.isAbsent(151));
                assertEquals(99L, col.getLongValue(151));
            }
            // STRING builder: same gap structure.
            {
                EscfColumnBuilder b = new EscfColumnBuilder(policy);
                b.setString(100, utf8("hello")); // rows 0–99 absent
                b.setString(151, utf8("world")); // rows 101–150 absent
                EscfColumnData data = b.finish(152);
                assertEquals(EscfColumnKind.STRING, data.kind());
                EscfColumn col = EscfColumn.from(data);
                for (int r = 0; r < 100; r++) {
                    assertTrue("string row " + r + " should be absent [" + policy + "]", col.isAbsent(r));
                }
                assertFalse(col.isAbsent(100));
                assertEquals("hello", col.getBinaryValue(100).utf8ToString());
                assertFalse(col.isAbsent(151));
                assertEquals("world", col.getBinaryValue(151).utf8ToString());
            }
            // BOOL builder: gap of 50 absent rows then a true value.
            {
                EscfColumnBuilder b = new EscfColumnBuilder(policy);
                b.setBoolean(50, true);
                EscfColumnData data = b.finish(51);
                assertEquals(EscfColumnKind.BOOL, data.kind());
                EscfColumn col = EscfColumn.from(data);
                for (int r = 0; r < 50; r++) {
                    assertTrue("bool row " + r + " should be absent [" + policy + "]", col.isAbsent(r));
                }
                assertFalse(col.isAbsent(50));
                assertTrue(col.getBooleanValue(50));
            }
        }
    }

    /**
     * The direct columnar path ({@link EscfColumnBuilder#addLongArray}) must produce the same
     * {@link EscfColumnKind#ARRAY}-of-{@code LONG} layout as the packed {@code addArray} path, honoring
     * the {@code size} argument (trailing buffer slots beyond {@code size} are ignored) and representing
     * an absent row as an empty element range.
     */
    public void testAddLongArrayColumnarLayout() {
        EscfColumnBuilder b = builder(); // SPLIT
        b.addLongArray(new long[] { 10, 20, 30 }, 3);
        b.addAbsent();
        b.addLongArray(new long[] { 7, 8, 9, 99 /* beyond size, ignored */ }, 3);
        EscfColumnData data = b.finish(3);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(EscfColumnKind.LONG, data.child().kind());
        assertEquals(3, elemCount(data, 0));
        assertEquals(10L, readArrayLong(data, 0, 0));
        assertEquals(30L, readArrayLong(data, 0, 2));
        assertEquals(0, elemCount(data, 1)); // absent row
        assertEquals(3, elemCount(data, 2));
        assertEquals(7L, readArrayLong(data, 2, 0));
        assertEquals(9L, readArrayLong(data, 2, 2));
    }

    /** {@link EscfColumnBuilder#addDoubleArray} builds an ARRAY-of-{@code DOUBLE}, preserving raw bits. */
    public void testAddDoubleArrayColumnarLayout() {
        EscfColumnBuilder b = builder();
        long[] bits = { Double.doubleToRawLongBits(1.5), Double.doubleToRawLongBits(-2.5) };
        b.addDoubleArray(bits, 2);
        EscfColumnData data = b.finish(1);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(EscfColumnKind.DOUBLE, data.child().kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals(1.5, readArrayDouble(data, 0, 0), 0.0);
        assertEquals(-2.5, readArrayDouble(data, 0, 1), 0.0);
    }

    /**
     * Under SPLIT, a scalar row followed by an array row promotes the column to a union; the direct
     * columnar path must stream the array inline as a FIXED_ARRAY slot readable by {@code getArrayValue}.
     */
    public void testAddLongArrayAfterScalarInlinesOnUnion() {
        EscfColumnBuilder b = builder(); // SPLIT
        b.addLong(42); // row 0 scalar
        b.addLongArray(new long[] { 1, 2, 3 }, 3); // row 1 array → union inline FIXED_ARRAY
        EscfColumnData data = b.finish(2);
        assertEquals(EscfColumnKind.UNION, data.kind());
        EscfColumn col = EscfColumn.from(data);
        assertEquals(SourceValueType.LONG, col.getTypeByte(0));
        assertEquals(42L, col.getLongValue(0));
        assertEquals(SourceValueType.FIXED_ARRAY, col.getTypeByte(1));
        assertEquals(List.of(1L, 2L, 3L), unionArrayLongs(col, 1));
    }

    // ── Helpers ──

    private static List<Long> unionArrayLongs(EscfColumn col, int row) {
        ArrayReader r = col.getArrayValue(row);
        List<Long> out = new ArrayList<>();
        while (r.next()) {
            out.add(r.type() == SourceValueType.INT ? (long) r.intValue() : r.longValue());
        }
        return out;
    }

    /** Builds in SPLIT mode; scalar/union cases behave identically under MERGE. */
    private static EscfColumnBuilder builder() {
        return new EscfColumnBuilder(CollisionPolicy.SPLIT);
    }

    private static EscfColumnBuilder mergeBuilder() {
        return new EscfColumnBuilder(CollisionPolicy.MERGE);
    }

    private static BytesRef bytesRef(String s) {
        return new BytesRef(s.getBytes(StandardCharsets.UTF_8));
    }

    private static int elemCount(EscfColumnData data, int row) {
        return data.offsets()[row + 1] - data.offsets()[row];
    }

    private static String readArrayElem(EscfColumnData data, int row, int elemPos) {
        int elemIdx = data.offsets()[row] + elemPos;
        return EscfColumn.from(data.child()).getBinaryValue(elemIdx).utf8ToString();
    }

    private static long readArrayLong(EscfColumnData data, int row, int elemPos) {
        int elemIdx = data.offsets()[row] + elemPos;
        return EscfColumn.from(data.child()).getLongValue(elemIdx);
    }

    private static double readArrayDouble(EscfColumnData data, int row, int elemPos) {
        int elemIdx = data.offsets()[row] + elemPos;
        return EscfColumn.from(data.child()).getDoubleValue(elemIdx);
    }

    private static String readScalarString(EscfColumnData data, int row) {
        return EscfColumn.from(data).getBinaryValue(row).utf8ToString();
    }

    private static long readScalarLong(EscfColumnData data, int row) {
        return EscfColumn.from(data).getLongValue(row);
    }

    private static XContentString.UTF8Bytes utf8(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new XContentString.UTF8Bytes(bytes, 0, bytes.length);
    }
}
