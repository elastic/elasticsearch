/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class EscfCursorsTests extends ESTestCase {

    public void testLongTupleCursorDense() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(10);
        b.addLong(20);
        b.addLong(30);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(3));

        List<long[]> tuples = drainLongTuples(col.longCursor());
        assertEquals(3, tuples.size());
        assertLongTuple(0, 10, tuples.get(0));
        assertLongTuple(1, 20, tuples.get(1));
        assertLongTuple(2, 30, tuples.get(2));
    }

    public void testLongTupleCursorSparse() {
        // Docs: [10, absent, 30, absent, 50]
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(10);
        b.addAbsent();
        b.addLong(30);
        b.addAbsent();
        b.addLong(50);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(5));

        List<long[]> tuples = drainLongTuples(col.longCursor());
        assertEquals(3, tuples.size());
        assertLongTuple(0, 10, tuples.get(0));
        assertLongTuple(2, 30, tuples.get(1));
        assertLongTuple(4, 50, tuples.get(2));
    }

    public void testLongTupleCursorAllAbsent() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addAbsent();
        b.addAbsent();
        b.addAbsent();
        EscfColumn col = EscfColumn.from(b.finish(3));

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, col.longCursor().nextDoc());
    }

    public void testLongTupleCursorSingleRow() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(42);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(1));

        List<long[]> tuples = drainLongTuples(col.longCursor());
        assertEquals(1, tuples.size());
        assertLongTuple(0, 42, tuples.get(0));
    }

    public void testLongValuesCursorDenseNextLong() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(100);
        b.addLong(200);
        b.addLong(300);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(3));

        LongValuesCursor cursor = col.longValuesCursor();
        assertEquals(3, cursor.size());
        assertEquals(100, cursor.nextLong());
        assertEquals(200, cursor.nextLong());
        assertEquals(300, cursor.nextLong());
    }

    public void testLongValuesCursorDenseFillDocValues() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(7);
        b.addLong(8);
        b.addLong(9);
        b.addLong(10);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(4));

        LongValuesCursor cursor = col.longValuesCursor();
        long[] dst = new long[6];
        cursor.fillDocValues(dst, 1, 4);
        assertEquals(0, dst[0]);
        assertEquals(7, dst[1]);
        assertEquals(8, dst[2]);
        assertEquals(9, dst[3]);
        assertEquals(10, dst[4]);
        assertEquals(0, dst[5]);
    }

    public void testLongValuesCursorMixedNextLongAndFill() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(1);
        b.addLong(2);
        b.addLong(3);
        b.addLong(4);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(4));

        LongValuesCursor cursor = col.longValuesCursor();
        assertEquals(1, cursor.nextLong());
        long[] dst = new long[3];
        cursor.fillDocValues(dst, 0, 3);
        assertEquals(2, dst[0]);
        assertEquals(3, dst[1]);
        assertEquals(4, dst[2]);
    }

    public void testLongValuesCursorOverrunThrows() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addLong(1);
        b.addLong(2);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(2));

        LongValuesCursor cursor = col.longValuesCursor();
        cursor.nextLong();
        cursor.nextLong();
        expectThrows(IllegalStateException.class, cursor::nextLong);
    }

    public void testLongValuesCursorNextLongCrossesChunkBoundary() {
        // Chunk 1: [10], Chunk 2: [20, 30] — nextLong() triggers nextChunk() between values 1 and 2.
        BytesReference data = CompositeBytesReference.of(longChunk(10L), longChunk(20L, 30L));
        EscfLongColumn col = new EscfLongColumn(3, null, data);
        LongValuesCursor cursor = col.longValuesCursor();
        assertEquals(3, cursor.size());
        assertEquals(10L, cursor.nextLong());
        assertEquals(20L, cursor.nextLong()); // crosses chunk boundary
        assertEquals(30L, cursor.nextLong());
    }

    public void testLongValuesCursorFillDocValuesCrossesChunkBoundary() {
        // Chunk 1: [10, 20], Chunk 2: [30, 40] — fillDocValues inner loop must span both chunks.
        BytesReference data = CompositeBytesReference.of(longChunk(10L, 20L), longChunk(30L, 40L));
        EscfLongColumn col = new EscfLongColumn(4, null, data);
        LongValuesCursor cursor = col.longValuesCursor();
        long[] dst = new long[4];
        cursor.fillDocValues(dst, 0, 4);
        assertArrayEquals(new long[] { 10L, 20L, 30L, 40L }, dst);
    }

    public void testLongValuesCursorFillDocValuesOverrunThrows() {
        EscfLongColumn col = new EscfLongColumn(2, null, longChunk(1L, 2L));
        LongValuesCursor cursor = col.longValuesCursor();
        expectThrows(IllegalStateException.class, () -> cursor.fillDocValues(new long[3], 0, 3));
    }

    public void testLongTupleCursorConsecutiveAbsentRows() {
        // [10, absent, absent, 40] — toSkip accumulates to 2 before the single skip() call.
        FixedBitSet validity = new FixedBitSet(4);
        validity.set(0); // row 0 present
        validity.set(3); // row 3 present; rows 1 and 2 absent (bits clear)
        EscfLongColumn col = new EscfLongColumn(4, validity, longChunk(10L, 0L, 0L, 40L));
        List<long[]> tuples = drainLongTuples(col.longCursor());
        assertEquals(2, tuples.size());
        assertLongTuple(0, 10L, tuples.get(0));
        assertLongTuple(3, 40L, tuples.get(1));
    }

    public void testLongTupleCursorAbsentRowsSpanChunkBoundary() {
        // [10, absent, absent, 40], chunk split after row 1: [10, 0] | [0, 40].
        // skip(2) must call nextChunk() mid-skip (exhausts chunk 1 after skipping row 1,
        // then skips row 2 from chunk 2), and nextLong() reads row 3's value from chunk 2.
        FixedBitSet validity = new FixedBitSet(4);
        validity.set(0); // row 0 present
        validity.set(3); // row 3 present; rows 1 and 2 absent (bits clear)
        BytesReference data = CompositeBytesReference.of(longChunk(10L, 0L), longChunk(0L, 40L));
        EscfLongColumn col = new EscfLongColumn(4, validity, data);
        List<long[]> tuples = drainLongTuples(col.longCursor());
        assertEquals(2, tuples.size());
        assertLongTuple(0, 10L, tuples.get(0));
        assertLongTuple(3, 40L, tuples.get(1));
    }

    public void testLongArrayTupleCursorMultivalue() {
        // 4 rows: [[1, 2], [3], [], [4, 5, 6]]
        int[] rowOffsets = { 0, 2, 3, 3, 6 };
        long[] elements = { 1L, 2L, 3L, 4L, 5L, 6L };
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(longArrayChild(elements));
        EscfArrayColumn array = new EscfArrayColumn(4, null, col, intsRef(rowOffsets));

        List<long[]> tuples = drainLongTuples(array.longCursor());
        // Row 0: [1, 2] → (0,1), (0,2)
        // Row 1: [3] → (1,3)
        // Row 2: [] → skipped
        // Row 3: [4,5,6]→ (3,4), (3,5), (3,6)
        assertEquals(6, tuples.size());
        assertLongTuple(0, 1, tuples.get(0));
        assertLongTuple(0, 2, tuples.get(1));
        assertLongTuple(1, 3, tuples.get(2));
        assertLongTuple(3, 4, tuples.get(3));
        assertLongTuple(3, 5, tuples.get(4));
        assertLongTuple(3, 6, tuples.get(5));
    }

    public void testLongArrayTupleCursorAllEmpty() {
        // 3 rows all empty
        int[] rowOffsets = { 0, 0, 0, 0 };
        long[] elements = {};
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(longArrayChild(elements));
        EscfArrayColumn array = new EscfArrayColumn(3, null, col, intsRef(rowOffsets));

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, array.longCursor().nextDoc());
    }

    public void testLongArrayTupleCursorAbsentRowsSkipped() {
        // 3 rows: [[1], absent, [2, 3]]
        // absent row has empty element range (same as empty)
        int[] rowOffsets = { 0, 1, 1, 3 };
        long[] elements = { 1L, 2L, 3L };
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(longArrayChild(elements));
        // Mark row 1 absent in the rowOffsets — its element range is already 0-width (1..1)
        EscfArrayColumn array = new EscfArrayColumn(3, null, col, intsRef(rowOffsets));

        List<long[]> tuples = drainLongTuples(array.longCursor());
        assertEquals(3, tuples.size());
        assertLongTuple(0, 1, tuples.get(0));
        assertLongTuple(2, 2, tuples.get(1));
        assertLongTuple(2, 3, tuples.get(2));
    }

    public void testLongArrayTupleCursorWrongChildKindThrows() {
        // Build an ARRAY column with a STRING child — longCursor() should throw
        int[] rowOffsets = { 0, 1 };
        EscfColumnData strChild = EscfColumnData.ofVarWidth(
            EscfColumnKind.STRING,
            1,
            null,
            new int[] { 0, 3 },
            new BytesArray("foo".getBytes(StandardCharsets.UTF_8))
        );
        EscfColumn strColAsChild = EscfColumn.from(strChild);
        EscfArrayColumn array = new EscfArrayColumn(1, null, strColAsChild, intsRef(rowOffsets));

        expectThrows(UnsupportedOperationException.class, array::longCursor);
    }

    public void testStringTupleCursorDense() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addString(utf8("alpha"));
        b.addString(utf8("beta"));
        b.addString(utf8("gamma"));
        EscfColumn col = EscfColumn.from(b.finish(3));

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor());
        assertEquals(3, tuples.size());
        assertBytesRefTuple(0, "alpha", tuples.get(0));
        assertBytesRefTuple(1, "beta", tuples.get(1));
        assertBytesRefTuple(2, "gamma", tuples.get(2));
    }

    public void testStringTupleCursorSparse() {
        // Docs: ["a", absent, "c"]
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addString(utf8("a"));
        b.addAbsent();
        b.addString(utf8("c"));
        EscfColumn col = EscfColumn.from(b.finish(3));

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor());
        assertEquals(2, tuples.size());
        assertBytesRefTuple(0, "a", tuples.get(0));
        assertBytesRefTuple(2, "c", tuples.get(1));
    }

    public void testStringTupleCursorAllAbsent() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addAbsent();
        b.addAbsent();
        EscfColumn col = EscfColumn.from(b.finish(2));

        // All-absent column has kind LONG (the default), so we use a raw column
        // to get a string column with all absent. Build directly.
        EscfColumnData data = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 2, absentAll(2), new int[] { 0, 0, 0 }, BytesArray.EMPTY);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, EscfColumn.from(data).bytesRefCursor().nextDoc());
    }

    public void testStringValuesCursorDense() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addString(utf8("x"));
        b.addString(utf8("yz"));
        b.addString(utf8("abc"));
        AbstractVarColumn col = (AbstractVarColumn) EscfColumn.from(b.finish(3));

        BytesRefValuesCursor cursor = col.bytesRefValuesCursor();
        assertEquals(3, cursor.size());
        assertEquals(new BytesRef("x"), BytesRef.deepCopyOf(cursor.nextValue()));
        assertEquals(new BytesRef("yz"), BytesRef.deepCopyOf(cursor.nextValue()));
        assertEquals(new BytesRef("abc"), BytesRef.deepCopyOf(cursor.nextValue()));
    }

    public void testStringValuesCursorOverrunThrows() {
        EscfColumnBuilder b = new EscfColumnBuilder();
        b.addString(utf8("only"));
        AbstractVarColumn col = (AbstractVarColumn) EscfColumn.from(b.finish(1));

        BytesRefValuesCursor cursor = col.bytesRefValuesCursor();
        cursor.nextValue();
        expectThrows(IllegalStateException.class, cursor::nextValue);
    }

    public void testBinaryTupleCursorDense() {
        byte[] rawData = { 0x01, 0x02, 0x03, 0x04 };
        int[] offs = { 0, 2, 4 };
        EscfColumnData data = EscfColumnData.ofVarWidth(EscfColumnKind.BINARY, 2, null, offs, new BytesArray(rawData));
        EscfColumn col = EscfColumn.from(data);

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor());
        assertEquals(2, tuples.size());
        assertEquals(0, tuples.get(0)[0]);
        assertArrayEquals(new byte[] { 0x01, 0x02 }, bytesOf((BytesRef) tuples.get(0)[1]));
        assertEquals(1, tuples.get(1)[0]);
        assertArrayEquals(new byte[] { 0x03, 0x04 }, bytesOf((BytesRef) tuples.get(1)[1]));
    }

    public void testBinaryTupleCursorSparse() {
        // Row 0: [0x01], row 1: absent (same offsets), row 2: [0x02, 0x03]
        byte[] rawData = { 0x01, 0x02, 0x03 };
        int[] offs = { 0, 1, 1, 3 };
        FixedBitSet validity = new FixedBitSet(3);
        validity.set(0); // row 0 present
        validity.set(2); // row 2 present; row 1 absent (bit clear)
        EscfColumnData data = EscfColumnData.ofVarWidth(EscfColumnKind.BINARY, 3, validity, offs, new BytesArray(rawData));
        EscfColumn col = EscfColumn.from(data);

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor());
        assertEquals(2, tuples.size());
        assertEquals(0, tuples.get(0)[0]);
        assertArrayEquals(new byte[] { 0x01 }, bytesOf((BytesRef) tuples.get(0)[1]));
        assertEquals(2, tuples.get(1)[0]);
        assertArrayEquals(new byte[] { 0x02, 0x03 }, bytesOf((BytesRef) tuples.get(1)[1]));
    }

    public void testBinaryValuesCursorDense() {
        byte[] rawData = { 0x0A, 0x0B, 0x0C };
        int[] offs = { 0, 1, 2, 3 };
        EscfColumnData data = EscfColumnData.ofVarWidth(EscfColumnKind.BINARY, 3, null, offs, new BytesArray(rawData));
        AbstractVarColumn col = (AbstractVarColumn) EscfColumn.from(data);

        BytesRefValuesCursor cursor = col.bytesRefValuesCursor();
        assertEquals(3, cursor.size());
        assertArrayEquals(new byte[] { 0x0A }, bytesOf(cursor.nextValue()));
        assertArrayEquals(new byte[] { 0x0B }, bytesOf(cursor.nextValue()));
        assertArrayEquals(new byte[] { 0x0C }, bytesOf(cursor.nextValue()));
    }

    public void testStringArrayTupleCursorMultivalue() {
        // 3 rows: [["hello", "world"], ["!"], []]
        // Child STRING column: ["hello", "world", "!"]
        byte[] bytes = "helloworld!".getBytes(StandardCharsets.UTF_8);
        int[] childOffsets = { 0, 5, 10, 11 };
        EscfColumnData childData = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 3, null, childOffsets, new BytesArray(bytes));
        EscfColumn childCol = EscfColumn.from(childData);

        int[] rowOffsets = { 0, 2, 3, 3 };
        EscfArrayColumn array = new EscfArrayColumn(3, null, childCol, intsRef(rowOffsets));

        List<Object[]> tuples = drainBytesRefTuples(array.bytesRefCursor());
        // Row 0: ["hello", "world"] → (0,"hello"), (0,"world")
        // Row 1: ["!"] → (1,"!")
        // Row 2: [] → skipped
        assertEquals(3, tuples.size());
        assertBytesRefTuple(0, "hello", tuples.get(0));
        assertBytesRefTuple(0, "world", tuples.get(1));
        assertBytesRefTuple(1, "!", tuples.get(2));
    }

    public void testStringArrayTupleCursorAllEmpty() {
        // 2 rows, all empty
        int[] rowOffsets = { 0, 0, 0 };
        EscfColumnData childData = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 0, null, new int[] { 0 }, BytesArray.EMPTY);
        EscfArrayColumn array = new EscfArrayColumn(2, null, EscfColumn.from(childData), intsRef(rowOffsets));

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, array.bytesRefCursor().nextDoc());
    }

    public void testStringArrayTupleCursorWrongChildKindThrows() {
        // Build an ARRAY column with a LONG child — bytesRefCursor() should throw
        int[] rowOffsets = { 0, 1 };
        EscfLongColumn longChild = (EscfLongColumn) EscfColumn.from(
            EscfColumnData.ofFixed64(EscfColumnKind.LONG, 1, null, longBytes(new long[] { 99L }))
        );
        EscfArrayColumn array = new EscfArrayColumn(1, null, longChild, intsRef(rowOffsets));

        expectThrows(UnsupportedOperationException.class, array::bytesRefCursor);
    }

    public void testLongTupleCursorZeroDocs() {
        EscfLongColumn col = new EscfLongColumn(0, null, BytesArray.EMPTY);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, col.longCursor().nextDoc());
    }

    public void testVarTupleCursorZeroDocs() {
        EscfColumnData data = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 0, null, new int[] { 0 }, BytesArray.EMPTY);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, EscfColumn.from(data).bytesRefCursor().nextDoc());
    }

    public void testArrayLongCursorZeroDocs() {
        EscfLongColumn child = new EscfLongColumn(0, null, BytesArray.EMPTY);
        EscfArrayColumn array = new EscfArrayColumn(0, null, child, intsRef(new int[] { 0 }));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, array.longCursor().nextDoc());
    }

    private static List<long[]> drainLongTuples(LongTupleCursor cursor) {
        List<long[]> result = new ArrayList<>();
        int docId;
        while ((docId = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            result.add(new long[] { docId, cursor.longValue() });
        }
        return result;
    }

    private static List<Object[]> drainBytesRefTuples(ObjectTupleCursor<BytesRef> cursor) {
        List<Object[]> result = new ArrayList<>();
        int docId;
        while ((docId = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            result.add(new Object[] { docId, BytesRef.deepCopyOf(cursor.value()) });
        }
        return result;
    }

    private static void assertLongTuple(int expectedDoc, long expectedValue, long[] tuple) {
        assertEquals("docId", expectedDoc, tuple[0]);
        assertEquals("longValue", expectedValue, tuple[1]);
    }

    private static void assertBytesRefTuple(int expectedDoc, String expectedUtf8, Object[] tuple) {
        assertEquals("docId", expectedDoc, tuple[0]);
        assertEquals(new BytesRef(expectedUtf8), tuple[1]);
    }

    private static byte[] bytesOf(BytesRef ref) {
        byte[] out = new byte[ref.length];
        System.arraycopy(ref.bytes, ref.offset, out, 0, ref.length);
        return out;
    }

    private static XContentString.UTF8Bytes utf8(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new XContentString.UTF8Bytes(bytes, 0, bytes.length);
    }

    /** Wraps an int array into an {@link IntsRef} starting at offset 0. */
    private static IntsRef intsRef(int[] ints) {
        return new IntsRef(ints, 0, ints.length);
    }

    /** Builds a fixed-64 {@link EscfColumnData} for a long array (all dense, no absent). */
    private static EscfColumnData longArrayChild(long[] values) {
        BytesReference data = longBytes(values);
        return EscfColumnData.ofFixed64(EscfColumnKind.LONG, values.length, null, data);
    }

    /** Packs a varargs long array into a single {@link BytesArray} chunk of little-endian 8-byte slots. */
    private static BytesArray longChunk(long... values) {
        byte[] bytes = new byte[values.length * 8];
        for (int i = 0; i < values.length; i++) {
            ByteUtils.writeLongLE(values[i], bytes, i * 8);
        }
        return new BytesArray(bytes);
    }

    /** Packs a long array into little-endian bytes suitable for {@link EscfColumnData#ofFixed64}. */
    private static BytesReference longBytes(long[] values) {
        byte[] bytes = new byte[values.length * 8];
        for (int i = 0; i < values.length; i++) {
            ByteUtils.writeLongLE(values[i], bytes, i * 8);
        }
        return new BytesArray(bytes);
    }

    /** Builds a validity {@link FixedBitSet} with no bits set, meaning all docs are absent. */
    private static FixedBitSet absentAll(int count) {
        return new FixedBitSet(count); // all bits clear = all absent in validity semantics
    }
}
