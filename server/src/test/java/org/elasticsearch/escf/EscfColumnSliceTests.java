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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;

public class EscfColumnSliceTests extends ESTestCase {

    public void testLongSliceReadsAndRoundTrip() {
        // 5 docs: [10, absent, 30, 40, 50]; slice [1, 4) → [absent, 30, 40]
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(10);
        b.addAbsent();
        b.addLong(30);
        b.addLong(40);
        b.addLong(50);
        EscfColumnData data = b.finish(5);
        EscfColumn col = EscfColumn.from(data);

        EscfColumn slice = col.sliceInternal(1, 3);
        assertEquals(3, slice.docCount);

        assertTrue("doc 0 of slice must be absent", slice.isAbsent(0));
        assertFalse("doc 1 of slice must be present", slice.isAbsent(1));
        assertFalse("doc 2 of slice must be present", slice.isAbsent(2));
        assertEquals(30L, slice.getLongValue(1));
        assertEquals(40L, slice.getLongValue(2));

        // toColumnData round-trip
        EscfColumnData sliceData = slice.toColumnData();
        assertEquals(EscfColumnKind.LONG, sliceData.kind());
        assertEquals(3, sliceData.docCount());
        EscfColumn reparsed = EscfColumn.from(sliceData);
        assertTrue(reparsed.isAbsent(0));
        assertEquals(30L, reparsed.getLongValue(1));
        assertEquals(40L, reparsed.getLongValue(2));
    }

    public void testLongDenseSlice() {
        // 4 docs: [100, 200, 300, 400]; slice [2, 4) → [300, 400]
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(100);
        b.addLong(200);
        b.addLong(300);
        b.addLong(400);
        EscfColumnData data = b.finish(4);
        EscfColumn col = EscfColumn.from(data);
        EscfColumn slice = col.sliceInternal(2, 2);

        assertNull("dense column slice has no validity bitset", sliceData(slice).validity());
        assertEquals(300L, slice.getLongValue(0));
        assertEquals(400L, slice.getLongValue(1));
    }

    public void testDoubleSliceReadsAndRoundTrip() {
        // 4 docs: [1.1, 2.2, 3.3, 4.4]; slice [1, 3) → [2.2, 3.3]
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addDouble(1.1);
        b.addDouble(2.2);
        b.addDouble(3.3);
        b.addDouble(4.4);
        EscfColumnData data = b.finish(4);
        EscfColumn col = EscfColumn.from(data);
        EscfColumn slice = col.sliceInternal(1, 2);

        assertEquals(2, slice.docCount);
        assertEquals(2.2, slice.getDoubleValue(0), 0.0);
        assertEquals(3.3, slice.getDoubleValue(1), 0.0);

        EscfColumnData sliceData = slice.toColumnData();
        assertEquals(EscfColumnKind.DOUBLE, sliceData.kind());
        assertEquals(2, sliceData.docCount());
        EscfColumn reparsed = EscfColumn.from(sliceData);
        assertEquals(2.2, reparsed.getDoubleValue(0), 0.0);
        assertEquals(3.3, reparsed.getDoubleValue(1), 0.0);
    }

    public void testStringSliceReadsAndRoundTrip() {
        // 4 docs: ["hello", "world", absent, "bar"]; slice [1, 4) → ["world", absent, "bar"]
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addString(utf8("hello"));
        b.addString(utf8("world"));
        b.addAbsent();
        b.addString(utf8("bar"));
        EscfColumnData data = b.finish(4);
        EscfColumn col = EscfColumn.from(data);
        EscfColumn slice = col.sliceInternal(1, 3);

        assertEquals(3, slice.docCount);
        assertEquals("world", slice.getStringValue(0).string());
        assertTrue(slice.isAbsent(1));
        assertEquals("bar", slice.getStringValue(2).string());

        EscfColumnData sliceData = slice.toColumnData();
        assertEquals(EscfColumnKind.STRING, sliceData.kind());
        assertEquals(3, sliceData.docCount());
        // offsets must be rebased to 0
        assertEquals(0, sliceData.offsets()[0]);
        EscfColumn reparsed = EscfColumn.from(sliceData);
        assertEquals("world", reparsed.getStringValue(0).string());
        assertTrue(reparsed.isAbsent(1));
        assertEquals("bar", reparsed.getStringValue(2).string());
    }

    public void testBinarySliceReadsAndRoundTrip() {
        // Build a BINARY column directly: 3 docs with byte payloads [0x01 0x02], [0x03 0x04 0x05], [0x06]
        // slice [1, 3) → [[0x03 0x04 0x05], [0x06]]
        byte[] rawData = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 };
        int[] offsets = { 0, 2, 5, 6 };
        BytesReference data = new BytesArray(rawData);
        EscfColumnData colData = EscfColumnData.ofVarWidth(EscfColumnKind.BINARY, 3, null, offsets, data);

        EscfColumn col = EscfColumn.from(colData);
        EscfColumn slice = col.sliceInternal(1, 2);

        assertEquals(2, slice.docCount);
        assertBinaryEquals(new byte[] { 0x03, 0x04, 0x05 }, slice.getBinaryValue(0));
        assertBinaryEquals(new byte[] { 0x06 }, slice.getBinaryValue(1));

        EscfColumnData sliceData = slice.toColumnData();
        assertEquals(EscfColumnKind.BINARY, sliceData.kind());
        assertEquals(2, sliceData.docCount());
        assertEquals(0, sliceData.offsets()[0]);
        EscfColumn reparsed = EscfColumn.from(sliceData);
        assertBinaryEquals(new byte[] { 0x03, 0x04, 0x05 }, reparsed.getBinaryValue(0));
        assertBinaryEquals(new byte[] { 0x06 }, reparsed.getBinaryValue(1));
    }

    public void testBoolSliceReadsAndRoundTrip() {
        // 5 docs: [T, absent, F, T, T]; slice [1, 4) → [absent, F, T]
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addBoolean(true);
        b.addAbsent();
        b.addBoolean(false);
        b.addBoolean(true);
        b.addBoolean(true);
        EscfColumnData data = b.finish(5);
        EscfColumn col = EscfColumn.from(data);
        EscfColumn slice = col.sliceInternal(1, 3);

        assertEquals(3, slice.docCount);
        assertTrue(slice.isAbsent(0));
        assertFalse(slice.getBooleanValue(1));
        assertEquals(SourceValueType.FALSE, slice.getTypeByte(1));
        assertTrue(slice.getBooleanValue(2));
        assertEquals(SourceValueType.TRUE, slice.getTypeByte(2));

        EscfColumnData sliceData = slice.toColumnData();
        assertEquals(EscfColumnKind.BOOL, sliceData.kind());
        assertEquals(3, sliceData.docCount());
        EscfColumn reparsed = EscfColumn.from(sliceData);
        assertTrue(reparsed.isAbsent(0));
        assertFalse(reparsed.getBooleanValue(1));
        assertTrue(reparsed.getBooleanValue(2));
    }

    public void testBoolAllFalseSlice() {
        // When all values are false the values bitset is null; slicing must keep it null.
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addBoolean(false);
        b.addBoolean(false);
        b.addBoolean(false);
        EscfColumnData data = b.finish(3);
        EscfColumn col = EscfColumn.from(data);
        EscfColumn slice = col.sliceInternal(0, 2);

        assertFalse(slice.getBooleanValue(0));
        assertFalse(slice.getBooleanValue(1));
        assertNull("all-false slice must have null values bitset", sliceData(slice).values());
    }

    public void testArraySliceReadsAndRoundTrip() {
        // Build an ARRAY column directly:
        // 4 rows: [[1,2], [3], [], [4,5,6]]; child = LONG[1,2,3,4,5,6]
        // rowOffsets = [0, 2, 3, 3, 6]
        // slice [1, 4) → [[3], [], [4,5,6]]
        int[] rowOffsets = { 0, 2, 3, 3, 6 };
        byte[] childBytes = new byte[6 * 8];
        long[] childLongs = { 1L, 2L, 3L, 4L, 5L, 6L };
        for (int i = 0; i < childLongs.length; i++) {
            ByteUtils.writeLongLE(childLongs[i], childBytes, i * 8);
        }
        EscfColumnData childData = EscfColumnData.ofFixed64(EscfColumnKind.LONG, 6, null, new BytesArray(childBytes));
        EscfColumnData colData = EscfColumnData.ofArray(4, null, rowOffsets, childData);

        EscfColumn col = EscfColumn.from(colData);
        EscfColumn slice = col.sliceInternal(1, 3);

        assertEquals(3, slice.docCount);

        // Row 0 of slice = [3]
        var arr0 = slice.getArrayValue(0);
        assertTrue(arr0.next());
        assertEquals(3L, arr0.longValue());
        assertFalse(arr0.next());

        // Row 1 of slice = []
        var arr1 = slice.getArrayValue(1);
        assertFalse(arr1.next());

        // Row 2 of slice = [4, 5, 6]
        var arr2 = slice.getArrayValue(2);
        assertTrue(arr2.next());
        assertEquals(4L, arr2.longValue());
        assertTrue(arr2.next());
        assertEquals(5L, arr2.longValue());
        assertTrue(arr2.next());
        assertEquals(6L, arr2.longValue());
        assertFalse(arr2.next());

        // round-trip via toColumnData
        EscfColumnData sliceData = slice.toColumnData();
        assertEquals(EscfColumnKind.ARRAY, sliceData.kind());
        assertEquals(3, sliceData.docCount());
        assertEquals(0, sliceData.offsets()[0]);
        EscfColumn reparsed = EscfColumn.from(sliceData);
        var rep0 = reparsed.getArrayValue(0);
        assertTrue(rep0.next());
        assertEquals(3L, rep0.longValue());
        assertFalse(rep0.next());
    }

    public void testUnionSliceReadsAndRoundTrip() {
        // 4 docs: [long(7), string("abc"), double(3.14), null]; slice [1, 4) → [string("abc"), double(3.14), null]
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(7);
        b.addString(utf8("abc"));
        b.addDouble(3.14);
        b.addNull();
        EscfColumnData data = b.finish(4);
        EscfColumn col = EscfColumn.from(data);
        assertEquals(EscfColumnKind.UNION, data.kind());

        EscfColumn slice = col.sliceInternal(1, 3);
        assertEquals(3, slice.docCount);

        assertEquals(SourceValueType.STRING, slice.getTypeByte(0));
        assertEquals("abc", slice.getStringValue(0).string());
        assertEquals(SourceValueType.DOUBLE, slice.getTypeByte(1));
        assertEquals(3.14, slice.getDoubleValue(1), 1e-10);
        assertTrue(slice.isNull(2));
        assertEquals(SourceValueType.NULL, slice.getTypeByte(2));

        EscfColumnData sliceData = slice.toColumnData();
        assertEquals(EscfColumnKind.UNION, sliceData.kind());
        assertEquals(3, sliceData.docCount());
        assertEquals(0, sliceData.offsets()[0]);
        EscfColumn reparsed = EscfColumn.from(sliceData);
        assertEquals("abc", reparsed.getStringValue(0).string());
        assertEquals(3.14, reparsed.getDoubleValue(1), 1e-10);
        assertTrue(reparsed.isNull(2));
    }

    public void testAbsentBitsetNormalizationOnFrom() {
        // Build: 4 docs, absent at position 3 (last).
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(1);
        b.addLong(2);
        b.addLong(3);
        b.addAbsent();
        EscfColumnData data = b.finish(4);
        EscfColumn col = EscfColumn.from(data);
        assertFalse(col.isAbsent(0));
        assertFalse(col.isAbsent(2));
        assertTrue(col.isAbsent(3));

        // Slice [2, 4) → [3, absent]
        EscfColumn slice = col.sliceInternal(2, 2);
        assertFalse(slice.isAbsent(0));
        assertTrue(slice.isAbsent(1));
        assertEquals(3L, slice.getLongValue(0));
    }

    /**
     * Returns the {@link EscfColumnData} for the given column (materialized via {@link EscfColumn#toColumnData}).
     * Package-private method, so this helper is in the same package.
     */
    private static EscfColumnData sliceData(EscfColumn col) {
        return col.toColumnData();
    }

    private static XContentString.UTF8Bytes utf8(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new XContentString.UTF8Bytes(bytes, 0, bytes.length);
    }

    /** Asserts that a {@link BytesRef}'s effective bytes (respecting offset and length) match {@code expected}. */
    private static void assertBinaryEquals(byte[] expected, BytesRef ref) {
        assertEquals("binary length mismatch", expected.length, ref.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals("byte[" + i + "]", expected[i], ref.bytes[ref.offset + i]);
        }
    }
}
