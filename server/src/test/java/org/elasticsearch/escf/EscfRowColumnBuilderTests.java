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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link EscfRowColumnBuilder}: typed factories, scalar/ARRAY promotion,
 * per-kind setters, absent-row handling, and output shape for all supported kinds.
 */
public class EscfRowColumnBuilderTests extends ESTestCase {

    private static BytesRef bytesRef(String s) {
        return new BytesRef(s.getBytes(StandardCharsets.UTF_8));
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

    private static int elemCount(EscfColumnData data, int row) {
        return data.offsets()[row + 1] - data.offsets()[row];
    }

    private static String readScalarString(EscfColumnData data, int row) {
        return EscfColumn.from(data).getBinaryValue(row).utf8ToString();
    }

    private static long readScalarLong(EscfColumnData data, int row) {
        return EscfColumn.from(data).getLongValue(row);
    }

    private static double readScalarDouble(EscfColumnData data, int row) {
        return EscfColumn.from(data).getDoubleValue(row);
    }

    public void testStringsEmptyBuilder() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        assertTrue(builder.isEmpty());
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.STRING, data.kind());
        assertEquals(3, data.docCount());
        // All absent: validity should be a zero-filled bitset (or null only if zero docs, not here).
        assertNotNull("should have validity for all-absent column", data.validity());
        for (int r = 0; r < 3; r++) {
            assertTrue("doc " + r + " should be absent", EscfColumn.from(data).isAbsent(r));
        }
    }

    /** docCount == 0 and no writes produces a valid empty STRING column. */
    public void testStringsZeroDocCount() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        EscfColumnData data = builder.finish(0);
        assertEquals(EscfColumnKind.STRING, data.kind());
        assertEquals(0, data.docCount());
    }

    /** A single write → scalar STRING, that doc is present, others absent. */
    public void testStringsSingleValueSingleDoc() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setString(1, bytesRef("alpha"));
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.STRING, data.kind());
        assertTrue("doc 0 absent", EscfColumn.from(data).isAbsent(0));
        assertFalse("doc 1 present", EscfColumn.from(data).isAbsent(1));
        assertEquals("alpha", readScalarString(data, 1));
        assertTrue("doc 2 absent", EscfColumn.from(data).isAbsent(2));
    }

    /** All docs present, single value each → dense scalar STRING with no validity. */
    public void testStringsDenseAllPresent() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setString(0, bytesRef("a"));
        builder.setString(1, bytesRef("b"));
        builder.setString(2, bytesRef("c"));
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.STRING, data.kind());
        assertNull("dense column should have null validity", data.validity());
        assertEquals("a", readScalarString(data, 0));
        assertEquals("b", readScalarString(data, 1));
        assertEquals("c", readScalarString(data, 2));
    }

    /** Two values for one doc → ARRAY[STRING], multi-valued doc has two elements. */
    public void testStringsMultiValuePromotesToArray() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setString(0, bytesRef("x"));
        builder.setString(0, bytesRef("y")); // second element for doc 0
        builder.setString(1, bytesRef("z"));
        EscfColumnData data = builder.finish(2);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals("x", readArrayElem(data, 0, 0));
        assertEquals("y", readArrayElem(data, 0, 1));
        assertEquals(1, elemCount(data, 1));
        assertEquals("z", readArrayElem(data, 1, 0));
    }

    /** Rows supplied out of order must trigger the non-decreasing assertion. */
    public void testStringsNonDecreasingRowAssertion() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setString(2, bytesRef("a"));
        expectThrows(AssertionError.class, () -> builder.setString(1, bytesRef("b")));
    }

    /** Leading absent rows are correctly reflected in the scalar output. */
    public void testStringsLeadingAbsentRows() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setString(2, bytesRef("last"));
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.STRING, data.kind());
        assertTrue(EscfColumn.from(data).isAbsent(0));
        assertTrue(EscfColumn.from(data).isAbsent(1));
        assertFalse(EscfColumn.from(data).isAbsent(2));
        assertEquals("last", readScalarString(data, 2));
    }

    /** Trailing absent rows are correctly reflected in the scalar output. */
    public void testStringsTrailingAbsentRows() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setString(0, bytesRef("foo"));
        builder.setString(2, bytesRef("bar"));
        EscfColumnData data = builder.finish(5);
        assertEquals(EscfColumnKind.STRING, data.kind());
        assertFalse(EscfColumn.from(data).isAbsent(0));
        assertEquals("foo", readScalarString(data, 0));
        assertTrue(EscfColumn.from(data).isAbsent(1));
        assertFalse(EscfColumn.from(data).isAbsent(2));
        assertEquals("bar", readScalarString(data, 2));
        assertTrue(EscfColumn.from(data).isAbsent(3));
        assertTrue(EscfColumn.from(data).isAbsent(4));
    }

    /** isEmpty() transitions from true to false on the first write. */
    public void testStringsIsEmpty() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        assertTrue(builder.isEmpty());
        builder.setString(0, bytesRef("hello"));
        assertFalse(builder.isEmpty());
    }

    /** setString on a non-STRING builder triggers an AssertionError. */
    public void testStringsWrongSetterAssertion() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.longs(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        expectThrows(AssertionError.class, () -> builder.setString(0, bytesRef("x")));
    }

    /**
     * Verifies the element layout in an ARRAY output matches expectations, corresponding to the
     * old {@code testOutputMatchesXContentBasedArrayBuilder} test.
     */
    public void testArrayStringElementLayout() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setString(0, bytesRef("a"));
        builder.setString(1, bytesRef("b"));
        builder.setString(1, bytesRef("c")); // second element → ARRAY
        builder.setString(2, bytesRef("d"));
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(1, elemCount(data, 0));
        assertEquals("a", readArrayElem(data, 0, 0));
        assertEquals(2, elemCount(data, 1));
        assertEquals("b", readArrayElem(data, 1, 0));
        assertEquals("c", readArrayElem(data, 1, 1));
        assertEquals(1, elemCount(data, 2));
        assertEquals("d", readArrayElem(data, 2, 0));
    }

    /** Single BINARY write → scalar BINARY. */
    public void testBinarySingleValue() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.binaries(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setBinary(0, new BytesRef(new byte[] { 1, 2, 3 }));
        EscfColumnData data = builder.finish(1);
        assertEquals(EscfColumnKind.BINARY, data.kind());
        assertNull("dense", data.validity());
        assertEquals(new BytesRef(new byte[] { 1, 2, 3 }), EscfColumn.from(data).getBinaryValue(0));
    }

    /** Multi-value BINARY → ARRAY[BINARY]. */
    public void testBinaryMultiValuePromotesToArray() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.binaries(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setBinary(0, new BytesRef(new byte[] { 0xA }));
        builder.setBinary(0, new BytesRef(new byte[] { 0xB }));
        EscfColumnData data = builder.finish(1);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(2, elemCount(data, 0));
    }

    /** setBinary on a non-BINARY builder triggers an AssertionError. */
    public void testBinaryWrongSetterAssertion() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        expectThrows(AssertionError.class, () -> builder.setBinary(0, new BytesRef(new byte[] { 1 })));
    }

    // -- LONG builder --

    /** Single long write → dense scalar LONG column. */
    public void testLongSingleValueDense() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.longs(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setLong(0, 42L);
        builder.setLong(1, -1L);
        builder.setLong(2, Long.MAX_VALUE);
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.LONG, data.kind());
        assertNull("dense", data.validity());
        assertEquals(42L, readScalarLong(data, 0));
        assertEquals(-1L, readScalarLong(data, 1));
        assertEquals(Long.MAX_VALUE, readScalarLong(data, 2));
    }

    /** Absent LONG docs carry validity; present docs return correct values. */
    public void testLongWithAbsentDocs() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.longs(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setLong(1, 99L);
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.LONG, data.kind());
        assertNotNull("should have validity for absent docs", data.validity());
        assertTrue("doc 0 absent", EscfColumn.from(data).isAbsent(0));
        assertFalse("doc 1 present", EscfColumn.from(data).isAbsent(1));
        assertEquals(99L, readScalarLong(data, 1));
        assertTrue("doc 2 absent", EscfColumn.from(data).isAbsent(2));
    }

    /** Multi-value LONG → ARRAY[LONG] with correct element values after the compact-at-transition. */
    public void testLongMultiValuePromotesToArray() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.longs(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setLong(0, 1L);
        builder.setLong(0, 2L);
        EscfColumnData data = builder.finish(1);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(EscfColumnKind.LONG, data.child().kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals(1L, readArrayLong(data, 0, 0));
        assertEquals(2L, readArrayLong(data, 0, 1));
    }

    /**
     * ARRAY[LONG]: multiple docs, some with skip-rows before transition.
     * The first element for doc 0 is written positionally; the second element triggers the compact.
     * Doc 2 (absent) should have an empty range; doc 3 has two elements written after the compact.
     */
    public void testLongMultiDocArrayValues() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.longs(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setLong(0, 10L);
        builder.setLong(0, 20L); // triggers compact
        builder.setLong(1, 30L);
        // doc 2: absent (skip)
        builder.setLong(3, 40L);
        builder.setLong(3, 50L);
        EscfColumnData data = builder.finish(4);
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

    /** setLong on a non-LONG builder triggers an AssertionError. */
    public void testLongWrongSetterAssertion() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        expectThrows(AssertionError.class, () -> builder.setLong(0, 0L));
    }

    /** Non-decreasing assertion also applies to setLong. */
    public void testLongNonDecreasingRowAssertion() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.longs(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setLong(2, 0L);
        expectThrows(AssertionError.class, () -> builder.setLong(1, 0L));
    }

    /** Single double write → dense scalar DOUBLE column. */
    public void testDoubleSingleValueDense() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.doubles(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setDouble(0, 1.5);
        builder.setDouble(1, -0.0);
        builder.setDouble(2, Double.NaN);
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.DOUBLE, data.kind());
        assertNull("dense", data.validity());
        assertEquals(1.5, readScalarDouble(data, 0), 0.0);
        assertEquals(-0.0, readScalarDouble(data, 1), 0.0);
        // NaN equality via raw bits
        assertEquals(Double.doubleToRawLongBits(Double.NaN), Double.doubleToRawLongBits(readScalarDouble(data, 2)));
    }

    /** Absent DOUBLE docs carry validity; present docs return correct values. */
    public void testDoubleWithAbsentDocs() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.doubles(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setDouble(2, 3.14);
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.DOUBLE, data.kind());
        assertNotNull("should have validity for absent docs", data.validity());
        assertTrue("doc 0 absent", EscfColumn.from(data).isAbsent(0));
        assertTrue("doc 1 absent", EscfColumn.from(data).isAbsent(1));
        assertFalse("doc 2 present", EscfColumn.from(data).isAbsent(2));
        assertEquals(3.14, readScalarDouble(data, 2), 1e-9);
    }

    /** Multi-value DOUBLE → ARRAY[DOUBLE] with correct element values after the compact-at-transition. */
    public void testDoubleMultiValuePromotesToArray() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.doubles(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setDouble(0, 1.0);
        builder.setDouble(0, 2.0);
        EscfColumnData data = builder.finish(1);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(EscfColumnKind.DOUBLE, data.child().kind());
        assertEquals(2, elemCount(data, 0));
        assertEquals(1.0, readArrayDouble(data, 0, 0), 0.0);
        assertEquals(2.0, readArrayDouble(data, 0, 1), 0.0);
    }

    /** setDouble on a non-DOUBLE builder triggers an AssertionError. */
    public void testDoubleWrongSetterAssertion() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        expectThrows(AssertionError.class, () -> builder.setDouble(0, 0.0));
    }

    /** Non-decreasing assertion also applies to setDouble. */
    public void testDoubleNonDecreasingRowAssertion() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.doubles(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setDouble(3, 0.0);
        expectThrows(AssertionError.class, () -> builder.setDouble(2, 0.0));
    }
}
