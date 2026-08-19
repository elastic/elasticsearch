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
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
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
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
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
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addAbsent();
        b.addAbsent();
        b.addAbsent();
        EscfColumn col = EscfColumn.from(b.finish(3));

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, col.longCursor().nextDoc());
    }

    public void testLongTupleCursorSingleRow() {
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addLong(42);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(1));

        List<long[]> tuples = drainLongTuples(col.longCursor());
        assertEquals(1, tuples.size());
        assertLongTuple(0, 42, tuples.get(0));
    }

    public void testLongValuesCursorDenseNextLong() {
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
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
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
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
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
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
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
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
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addString(utf8("alpha"));
        b.addString(utf8("beta"));
        b.addString(utf8("gamma"));
        EscfColumn col = EscfColumn.from(b.finish(3));

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor(randomBoolean()));
        assertEquals(3, tuples.size());
        assertBytesRefTuple(0, "alpha", tuples.get(0));
        assertBytesRefTuple(1, "beta", tuples.get(1));
        assertBytesRefTuple(2, "gamma", tuples.get(2));
    }

    public void testStringTupleCursorSparse() {
        // Docs: ["a", absent, "c"]
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addString(utf8("a"));
        b.addAbsent();
        b.addString(utf8("c"));
        EscfColumn col = EscfColumn.from(b.finish(3));

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor(randomBoolean()));
        assertEquals(2, tuples.size());
        assertBytesRefTuple(0, "a", tuples.get(0));
        assertBytesRefTuple(2, "c", tuples.get(1));
    }

    public void testStringTupleCursorAllAbsent() {
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addAbsent();
        b.addAbsent();
        EscfColumn col = EscfColumn.from(b.finish(2));

        // All-absent column has kind LONG (the default), so we use a raw column
        // to get a string column with all absent. Build directly.
        EscfColumnData data = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 2, absentAll(2), new int[] { 0, 0, 0 }, BytesArray.EMPTY);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, EscfColumn.from(data).bytesRefCursor(randomBoolean()).nextDoc());
    }

    public void testStringValuesCursorDense() {
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addString(utf8("x"));
        b.addString(utf8("yz"));
        b.addString(utf8("abc"));
        AbstractVarColumn col = (AbstractVarColumn) EscfColumn.from(b.finish(3));

        BytesRefValuesCursor cursor = col.bytesRefValuesCursor(randomBoolean());
        assertEquals(3, cursor.size());
        assertEquals(new BytesRef("x"), BytesRef.deepCopyOf(cursor.nextValue()));
        assertEquals(new BytesRef("yz"), BytesRef.deepCopyOf(cursor.nextValue()));
        assertEquals(new BytesRef("abc"), BytesRef.deepCopyOf(cursor.nextValue()));
    }

    public void testStringValuesCursorOverrunThrows() {
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addString(utf8("only"));
        AbstractVarColumn col = (AbstractVarColumn) EscfColumn.from(b.finish(1));

        BytesRefValuesCursor cursor = col.bytesRefValuesCursor(randomBoolean());
        cursor.nextValue();
        expectThrows(IllegalStateException.class, cursor::nextValue);
    }

    public void testBinaryTupleCursorDense() {
        byte[] rawData = { 0x01, 0x02, 0x03, 0x04 };
        int[] offs = { 0, 2, 4 };
        EscfColumnData data = EscfColumnData.ofVarWidth(EscfColumnKind.BINARY, 2, null, offs, new BytesArray(rawData));
        EscfColumn col = EscfColumn.from(data);

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor(randomBoolean()));
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

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor(randomBoolean()));
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

        BytesRefValuesCursor cursor = col.bytesRefValuesCursor(randomBoolean());
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

        List<Object[]> tuples = drainBytesRefTuples(array.bytesRefCursor(randomBoolean()));
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

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, array.bytesRefCursor(randomBoolean()).nextDoc());
    }

    public void testStringArrayTupleCursorWrongChildKindThrows() {
        // Build an ARRAY column with a LONG child — bytesRefCursor() should throw
        int[] rowOffsets = { 0, 1 };
        EscfLongColumn longChild = (EscfLongColumn) EscfColumn.from(
            EscfColumnData.ofFixed64(EscfColumnKind.LONG, 1, null, longBytes(new long[] { 99L }))
        );
        EscfArrayColumn array = new EscfArrayColumn(1, null, longChild, intsRef(rowOffsets));

        expectThrows(UnsupportedOperationException.class, () -> array.bytesRefCursor(randomBoolean()));
    }

    public void testLongTupleCursorZeroDocs() {
        EscfLongColumn col = new EscfLongColumn(0, null, BytesArray.EMPTY);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, col.longCursor().nextDoc());
    }

    public void testVarTupleCursorZeroDocs() {
        EscfColumnData data = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 0, null, new int[] { 0 }, BytesArray.EMPTY);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, EscfColumn.from(data).bytesRefCursor(randomBoolean()).nextDoc());
    }

    public void testArrayLongCursorZeroDocs() {
        EscfLongColumn child = new EscfLongColumn(0, null, BytesArray.EMPTY);
        EscfArrayColumn array = new EscfArrayColumn(0, null, child, intsRef(new int[] { 0 }));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, array.longCursor().nextDoc());
    }

    public void testVarTupleCursorLeadingAbsentRows() {
        // Rows: absent, absent, "hello", "world"
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addAbsent();
        b.addAbsent();
        b.addString(utf8("hello"));
        b.addString(utf8("world"));
        EscfColumn col = EscfColumn.from(b.finish(4));

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor(randomBoolean()));
        assertEquals(2, tuples.size());
        assertBytesRefTuple(2, "hello", tuples.get(0));
        assertBytesRefTuple(3, "world", tuples.get(1));
    }

    public void testVarTupleCursorTrailingAbsentRows() {
        // Rows: "a", "b", absent, absent
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addString(utf8("a"));
        b.addString(utf8("b"));
        b.addAbsent();
        b.addAbsent();
        EscfColumn col = EscfColumn.from(b.finish(4));

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor(randomBoolean()));
        assertEquals(2, tuples.size());
        assertBytesRefTuple(0, "a", tuples.get(0));
        assertBytesRefTuple(1, "b", tuples.get(1));
    }

    public void testVarTupleCursorAbsentRowsSpanChunkBoundary() {
        // Rows: [0x01], absent, absent, [0x02, 0x03, 0x04]
        // offsets: {0, 1, 1, 1, 4} — absent rows are zero-width
        // Data split: first chunk [0x01], second chunk [0x02, 0x03, 0x04].
        // skip(2) must walk past the two absent (zero-width) rows, then nextValue() crosses into the second chunk.
        byte[] chunk1 = { 0x01 };
        byte[] chunk2 = { 0x02, 0x03, 0x04 };
        BytesReference data = CompositeBytesReference.of(new BytesArray(chunk1), new BytesArray(chunk2));
        int[] offs = { 0, 1, 1, 1, 4 };
        FixedBitSet validity = new FixedBitSet(4);
        validity.set(0); // row 0 present
        validity.set(3); // row 3 present; rows 1 and 2 absent
        EscfColumnData colData = EscfColumnData.ofVarWidth(EscfColumnKind.BINARY, 4, validity, offs, data);
        EscfColumn col = EscfColumn.from(colData);

        List<Object[]> tuples = drainBytesRefTuples(col.bytesRefCursor(randomBoolean()));
        assertEquals(2, tuples.size());
        assertEquals(0, tuples.get(0)[0]);
        assertArrayEquals(chunk1, bytesOf((BytesRef) tuples.get(0)[1]));
        assertEquals(3, tuples.get(1)[0]);
        assertArrayEquals(chunk2, bytesOf((BytesRef) tuples.get(1)[1]));
    }

    public void testTupleCursorsMatchRandomValidity() {
        // Randomized consistency check: for a random mix of present and absent rows, both
        // the long cursor and the string cursor must emit exactly the same (doc, value) pairs
        // as a reference walk that uses isPresent() + getLongValue() / getBinaryValue().
        int docCount = between(0, 50);
        var longBuilder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        var strBuilder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        long[] longVals = new long[docCount];
        String[] strVals = new String[docCount];
        boolean[] present = new boolean[docCount];
        for (int i = 0; i < docCount; i++) {
            present[i] = randomBoolean();
            if (present[i]) {
                longVals[i] = randomLong();
                strVals[i] = randomAlphaOfLengthBetween(0, 8);
                longBuilder.addLong(longVals[i]);
                strBuilder.addString(utf8(strVals[i]));
            } else {
                longBuilder.addAbsent();
                strBuilder.addAbsent();
            }
        }
        EscfColumn longCol = EscfColumn.from(longBuilder.finish(docCount));
        EscfColumnData strData = strBuilder.finish(docCount);
        EscfColumn strCol = EscfColumn.from(strData);

        // Build reference lists
        List<long[]> expectedLong = new ArrayList<>();
        List<Object[]> expectedStr = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            if (present[i]) {
                expectedLong.add(new long[] { i, longVals[i] });
                expectedStr.add(new Object[] { i, new BytesRef(strVals[i]) });
            }
        }

        List<long[]> actualLong = drainLongTuples(longCol.longCursor());
        assertEquals("long cursor doc count", expectedLong.size(), actualLong.size());
        for (int i = 0; i < expectedLong.size(); i++) {
            assertLongTuple((int) expectedLong.get(i)[0], expectedLong.get(i)[1], actualLong.get(i));
        }

        // A zero-doc or all-absent column falls back to the builder's default LONG kind and has no
        // BytesRef cursor, so there is nothing left to compare (same guard as
        // testArrayCursorsMatchRandomShape).
        if (strData.kind() != EscfColumnKind.STRING) {
            return;
        }

        List<Object[]> actualStr = drainBytesRefTuples(strCol.bytesRefCursor(randomBoolean()));
        assertEquals("str cursor doc count", expectedStr.size(), actualStr.size());
        for (int i = 0; i < expectedStr.size(); i++) {
            assertEquals("doc id at " + i, expectedStr.get(i)[0], actualStr.get(i)[0]);
            assertEquals("value at " + i, expectedStr.get(i)[1], actualStr.get(i)[1]);
        }
    }

    public void testArrayCursorsOnSlicedColumn() {
        // 4 rows: [[1, 2], [3], [], [4, 5, 6]] — then slice rows 1..3
        int[] rowOffsets = { 0, 2, 3, 3, 6 };
        long[] elements = { 1L, 2L, 3L, 4L, 5L, 6L };
        EscfLongColumn longCol = (EscfLongColumn) EscfColumn.from(longArrayChild(elements));
        EscfArrayColumn longArray = new EscfArrayColumn(4, null, longCol, intsRef(rowOffsets));

        // Build an equivalent string child
        byte[] strBytes = "abcdefghijklmnop".getBytes(StandardCharsets.UTF_8); // 6 elements, each ~2-3 bytes
        // Simple single-char elements for easy assertion
        byte[] charBytes = "ABCDEF".getBytes(StandardCharsets.UTF_8);
        int[] childOff = { 0, 1, 2, 3, 4, 5, 6 };
        EscfColumnData strChildData = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 6, null, childOff, new BytesArray(charBytes));
        EscfColumn strChild = EscfColumn.from(strChildData);
        EscfArrayColumn strArray = new EscfArrayColumn(4, null, strChild, intsRef(rowOffsets));

        // Slice rows 1..3 (i.e. from=1, count=3): expects [[3], [], [4,5,6]] for long; [["C"], [], ["D","E","F"]] for str
        EscfArrayColumn longSlice = (EscfArrayColumn) longArray.sliceInternal(1, 3);
        EscfArrayColumn strSlice = (EscfArrayColumn) strArray.sliceInternal(1, 3);

        // Long cursor on slice: row 0→3, row 1→(empty/skipped), row 2→4,5,6
        List<long[]> longTuples = drainLongTuples(longSlice.longCursor());
        assertEquals(4, longTuples.size());
        assertLongTuple(0, 3L, longTuples.get(0));
        assertLongTuple(2, 4L, longTuples.get(1));
        assertLongTuple(2, 5L, longTuples.get(2));
        assertLongTuple(2, 6L, longTuples.get(3));

        // String cursor on slice: row 0→"C", row 1→(empty/skipped), row 2→"D","E","F"
        List<Object[]> strTuples = drainBytesRefTuples(strSlice.bytesRefCursor(randomBoolean()));
        assertEquals(4, strTuples.size());
        assertBytesRefTuple(0, "C", strTuples.get(0));
        assertBytesRefTuple(2, "D", strTuples.get(1));
        assertBytesRefTuple(2, "E", strTuples.get(2));
        assertBytesRefTuple(2, "F", strTuples.get(3));
    }

    public void testArrayCursorsSpanChunkBoundary() {
        // Long: 3 elements split at a chunk boundary mid-array
        // row 0: [1, 2], row 1: [3] — chunk1 = [1, 2], chunk2 = [3]
        BytesArray longChunk1 = longChunk(1L, 2L);
        BytesArray longChunk2 = longChunk(3L);
        BytesReference longData = CompositeBytesReference.of(longChunk1, longChunk2);
        EscfColumnData longChildData = EscfColumnData.ofFixed64(EscfColumnKind.LONG, 3, null, longData);
        EscfLongColumn longChild = (EscfLongColumn) EscfColumn.from(longChildData);
        EscfArrayColumn longArray = new EscfArrayColumn(2, null, longChild, intsRef(new int[] { 0, 2, 3 }));

        List<long[]> longTuples = drainLongTuples(longArray.longCursor());
        assertEquals(3, longTuples.size());
        assertLongTuple(0, 1L, longTuples.get(0));
        assertLongTuple(0, 2L, longTuples.get(1));
        assertLongTuple(1, 3L, longTuples.get(2));

        // String: two rows, second element straddles the chunk boundary (split inside a single value's bytes)
        // row 0: ["ab"] — child element 0 = bytes {0x61, 0x62}
        // row 1: ["cd"] — child element 1 = bytes {0x63, 0x64}
        // Split so chunk1 = [0x61], chunk2 = [0x62, 0x63, 0x64] — element 0 straddles
        byte[] strChunk1 = { 0x61 };                    // 'a' (first byte of "ab")
        byte[] strChunk2 = { 0x62, 0x63, 0x64 };        // 'b' + "cd"
        BytesReference strData = CompositeBytesReference.of(new BytesArray(strChunk1), new BytesArray(strChunk2));
        int[] strChildOff = { 0, 2, 4 };
        EscfColumnData strChildData = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 2, null, strChildOff, strData);
        EscfColumn strChild = EscfColumn.from(strChildData);
        EscfArrayColumn strArray = new EscfArrayColumn(2, null, strChild, intsRef(new int[] { 0, 1, 2 }));

        // Both lifetimes must read the straddling value correctly: retaining copies out of scratch, while
        // non-retaining returns the scratch-backed ref that drainBytesRefTuples reads before advancing.
        for (boolean retainValues : new boolean[] { true, false }) {
            List<Object[]> strTuples = drainBytesRefTuples(strArray.bytesRefCursor(retainValues));
            assertEquals("retainValues=" + retainValues, 2, strTuples.size());
            assertBytesRefTuple(0, "ab", strTuples.get(0));
            assertBytesRefTuple(1, "cd", strTuples.get(1));
        }
    }

    public void testArrayCursorsMatchRandomShape() {
        // Build an ARRAY column via EscfColumnBuilder so the child is a real composite reference.
        int docCount = between(0, 20);
        var longBuilder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        var strBuilder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);

        // Per-row element counts and values; 0 elements = empty array row.
        int[][] elemCounts = new int[docCount][];
        long[][] longVals = new long[docCount][];
        String[][] strVals = new String[docCount][];
        for (int i = 0; i < docCount; i++) {
            int n = between(0, 4);
            elemCounts[i] = new int[] { n };
            longVals[i] = new long[n];
            strVals[i] = new String[n];
            if (n == 0) {
                longBuilder.addAbsent();
                strBuilder.addAbsent();
            } else {
                longBuilder.beginArray(i);
                strBuilder.beginArray(i);
                for (int j = 0; j < n; j++) {
                    longVals[i][j] = randomLong();
                    strVals[i][j] = randomAlphaOfLengthBetween(0, 8);
                    longBuilder.appendLong(longVals[i][j]);
                    strBuilder.appendString(new BytesRef(strVals[i][j]));
                }
                longBuilder.endArray();
                strBuilder.endArray();
            }
        }
        EscfColumnData longData = longBuilder.finish(docCount);
        EscfColumnData strData = strBuilder.finish(docCount);

        // Only proceed if the builder produced ARRAY columns (empty docCount or all-absent yields a simpler kind).
        if (longData.kind() != EscfColumnKind.ARRAY) {
            return;
        }
        EscfArrayColumn longArray = (EscfArrayColumn) EscfColumn.from(longData);
        EscfArrayColumn strArray = (EscfArrayColumn) EscfColumn.from(strData);

        // Build reference lists by direct element access
        List<long[]> expectedLong = new ArrayList<>();
        List<Object[]> expectedStr = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            for (int j = 0; j < elemCounts[i][0]; j++) {
                expectedLong.add(new long[] { i, longVals[i][j] });
                expectedStr.add(new Object[] { i, new BytesRef(strVals[i][j]) });
            }
        }

        List<long[]> actualLong = drainLongTuples(longArray.longCursor());
        assertEquals("long cursor element count", expectedLong.size(), actualLong.size());
        for (int i = 0; i < expectedLong.size(); i++) {
            assertLongTuple((int) expectedLong.get(i)[0], expectedLong.get(i)[1], actualLong.get(i));
        }

        List<Object[]> actualStr = drainBytesRefTuples(strArray.bytesRefCursor(randomBoolean()));
        assertEquals("str cursor element count", expectedStr.size(), actualStr.size());
        for (int i = 0; i < expectedStr.size(); i++) {
            assertEquals("doc id at " + i, expectedStr.get(i)[0], actualStr.get(i)[0]);
            assertEquals("value at " + i, expectedStr.get(i)[1], actualStr.get(i)[1]);
        }
    }

    public void testArrayBytesRefValuesAreRetained() {
        EscfArrayColumn array = helloWorldBangArray();

        // Collect without copying — each BytesRef must stay valid after the cursor advances.
        List<BytesRef> retained = new ArrayList<>();
        ObjectTupleCursor<BytesRef> cursor = array.bytesRefCursor(true);
        while (cursor.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            retained.add(cursor.value()); // intentionally no deepCopyOf
        }

        assertEquals(3, retained.size());
        assertEquals(new BytesRef("hello"), retained.get(0));
        assertEquals(new BytesRef("world"), retained.get(1));
        assertEquals(new BytesRef("!"), retained.get(2));
    }

    public void testArrayBytesRefValuesAreReusedWhenNotRetained() {
        EscfArrayColumn array = helloWorldBangArray();

        ObjectTupleCursor<BytesRef> cursor = array.bytesRefCursor(false);
        List<String> values = new ArrayList<>();
        BytesRef first = null;
        while (cursor.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            BytesRef value = cursor.value();
            if (first == null) {
                first = value;
            } else {
                assertSame("non-retaining cursor must reuse one BytesRef instance", first, value);
            }
            values.add(value.utf8ToString()); // read before advancing, as the contract requires
        }

        assertEquals(List.of("hello", "world", "!"), values);
    }

    /** 3 rows over a dense STRING child: {@code [["hello", "world"], ["!"], []]}. */
    private static EscfArrayColumn helloWorldBangArray() {
        byte[] bytes = "helloworld!".getBytes(StandardCharsets.UTF_8);
        int[] childOffsets = { 0, 5, 10, 11 };
        EscfColumnData childData = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 3, null, childOffsets, new BytesArray(bytes));
        return new EscfArrayColumn(3, null, EscfColumn.from(childData), intsRef(new int[] { 0, 2, 3, 3 }));
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

    public void testLongValueAtDense() {
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addLong(10);
        b.addLong(20);
        b.addLong(30);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(3));

        assertNull("dense column has no validity bitset", col.toColumnData().validity());
        assertEquals(10L, col.longValueAt(0));
        assertEquals(20L, col.longValueAt(1));
        assertEquals(30L, col.longValueAt(2));
    }

    public void testLongValueAtSparse() {
        // Docs: [10, absent, 30]
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addLong(10);
        b.addAbsent();
        b.addLong(30);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(3));

        assertTrue(col.isPresent(0));
        assertFalse(col.isPresent(1));
        assertTrue(col.isPresent(2));
        assertEquals(10L, col.longValueAt(0));
        assertEquals(30L, col.longValueAt(2));
    }

    public void testLongValueAtBoundaryValues() {
        var b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        b.addLong(Long.MIN_VALUE);
        b.addLong(-1L);
        b.addLong(0L);
        b.addLong(1L);
        b.addLong(Long.MAX_VALUE);
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(b.finish(5));

        assertEquals(Long.MIN_VALUE, col.longValueAt(0));
        assertEquals(-1L, col.longValueAt(1));
        assertEquals(0L, col.longValueAt(2));
        assertEquals(1L, col.longValueAt(3));
        assertEquals(Long.MAX_VALUE, col.longValueAt(4));
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
