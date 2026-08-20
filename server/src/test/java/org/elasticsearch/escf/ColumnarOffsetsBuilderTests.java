/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;
import java.util.Arrays;

public class ColumnarOffsetsBuilderTests extends ESTestCase {

    private static final String OFFSETS_FIELD = "f.offsets";

    /**
     * The whole contract in one property: for every document with enough slots to record, mapping the
     * decoded ordinals back through the document's sorted distinct values must reproduce the source
     * array exactly — order and duplicates included — and every other document must record nothing.
     * Decoding goes through {@link FieldArrayContext#parseOffsetArray}, the production reader.
     */
    public void testRoundTrip() throws IOException {
        final int docCount = between(1, 50);
        final long[][] docs = new long[docCount][];
        // A small pool makes duplicates frequent; the extremes catch signed-ordering slips in sort/dedup.
        final long[] pool = { 0L, 1L, -1L, 7L, 3L, Long.MIN_VALUE, Long.MAX_VALUE, randomLong() };
        for (int doc = 0; doc < docCount; doc++) {
            if (rarely()) {
                continue; // absent document
            }
            final int slotCount = rarely() ? between(20, 40) : between(0, 8);
            docs[doc] = new long[slotCount];
            for (int slot = 0; slot < slotCount; slot++) {
                docs[doc][slot] = pool[randomInt(pool.length - 1)];
            }
        }
        // Guarantee at least one recording document so the assertions below are never vacuous; the
        // all-single-valued case is covered by testAllSingleValuedReturnsNull.
        docs[randomInt(docCount - 1)] = randomLongArray(between(ColumnarOffsetsBuilder.MIN_RECORDED_SLOTS, 8), pool);

        final LuceneBinaryColumn column = ColumnarOffsetsBuilder.build(
            arrayColumn(docs),
            OFFSETS_FIELD,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        final String message = "input " + Arrays.deepToString(docs);
        assertNotNull(message, column);
        assertEquals(message, OFFSETS_FIELD, column.name());
        assertSame(message, SortedDocValuesField.TYPE, column.fieldType());

        final int[][] decoded = readOffsets(column, docCount);
        for (int doc = 0; doc < docCount; doc++) {
            final long[] slotValues = docs[doc];
            final int slotCount = slotValues == null ? 0 : slotValues.length;
            final String docMessage = message + ", doc " + doc;
            if (slotCount < ColumnarOffsetsBuilder.MIN_RECORDED_SLOTS) {
                assertNull(docMessage, decoded[doc]);
                continue;
            }
            final int[] ordinals = decoded[doc];
            assertNotNull(docMessage, ordinals);
            assertEquals(docMessage, slotCount, ordinals.length);

            final long[] distinctValues = sortedDistinct(slotValues);
            final long[] rebuilt = new long[slotCount];
            for (int slot = 0; slot < slotCount; slot++) {
                rebuilt[slot] = distinctValues[ordinals[slot]];
            }
            assertArrayEquals(docMessage, slotValues, rebuilt);
        }
    }

    /** No document has enough slots to record, so no column may be added at all. */
    public void testAllSingleValuedReturnsNull() {
        final long[][] docs = { new long[] { 42L }, null, new long[0], new long[] { -1L } };
        assertNull(ColumnarOffsetsBuilder.build(arrayColumn(docs), OFFSETS_FIELD, BytesRefRecycler.NON_RECYCLING_INSTANCE));
    }

    /** Repeated values share one ordinal, which is what makes the sidecar smaller than the source it replaces. */
    public void testDuplicatesShareOrdinal() throws IOException {
        final long[][] docs = { { 7L, 3L, 7L, 3L, 7L } };
        final LuceneBinaryColumn column = ColumnarOffsetsBuilder.build(
            arrayColumn(docs),
            OFFSETS_FIELD,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        // The sorted distinct values are [3, 7], so every 3 maps to ordinal 0 and every 7 to ordinal 1.
        assertArrayEquals(new int[] { 1, 0, 1, 0, 1 }, readOffsets(column, docs.length)[0]);
    }

    private long[] randomLongArray(int slotCount, long[] pool) {
        final long[] values = new long[slotCount];
        for (int slot = 0; slot < slotCount; slot++) {
            values[slot] = pool[randomInt(pool.length - 1)];
        }
        return values;
    }

    /**
     * Builds an ARRAY-of-LONG column: each non-null entry in {@code docs} is an array (possibly empty),
     * each null entry an absent document. {@code hintArray} pins the child kind so that a batch of only
     * empty arrays still finishes as ARRAY rather than being rewritten to a UNION.
     */
    private static EscfColumn arrayColumn(long[][] docs) {
        final EscfColumnBuilder builder = new EscfColumnBuilder(
            EscfColumnBuilder.CollisionPolicy.MERGE,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        builder.hintArray(EscfColumnKind.LONG);
        for (int doc = 0; doc < docs.length; doc++) {
            if (docs[doc] != null) {
                builder.beginArray(doc);
                for (long value : docs[doc]) {
                    builder.appendLong(value);
                }
                builder.endArray();
            }
        }
        return EscfColumn.from(builder.finish(docs.length));
    }

    /** Decodes each recorded document's ordinals; documents that recorded nothing stay {@code null}. */
    private static int[][] readOffsets(LuceneBinaryColumn column, int docCount) throws IOException {
        final int[][] ordinalsPerDoc = new int[docCount][];
        final ObjectTupleCursor<BytesRef> cursor = column.tuples();
        final ByteArrayStreamInput in = new ByteArrayStreamInput();
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            final BytesRef encoded = cursor.value();
            in.reset(encoded.bytes, encoded.offset, encoded.length);
            ordinalsPerDoc[doc] = FieldArrayContext.parseOffsetArray(in);
        }
        return ordinalsPerDoc;
    }

    /** Independent of the production dedup: the value list the read side reconstructs from doc values. */
    private static long[] sortedDistinct(long[] values) {
        return Arrays.stream(values).sorted().distinct().toArray();
    }
}
