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
     * decoded ordinals back through the document's sorted distinct values must reproduce the source array
     * exactly — order and duplicates included — and every other document must record nothing. Decoding goes
     * through {@link FieldArrayContext#parseOffsetArray}, the production reader.
     */
    public void testLongRoundTrip() throws IOException {
        final int docCount = between(1, 50);
        final long[][] docs = new long[docCount][];
        final long[] pool = longPool();
        for (int doc = 0; doc < docCount; doc++) {
            if (rarely()) {
                continue; // absent document
            }
            docs[doc] = randomLongSlots(rarely() ? between(20, 40) : between(0, 8), pool);
        }
        // Guarantee at least one recording document so the assertions below are never vacuous.
        docs[randomInt(docCount - 1)] = randomLongSlots(between(ColumnarOffsetsBuilder.MIN_RECORDED_SLOTS, 8), pool);

        final LuceneBinaryColumn column = ColumnarOffsetsBuilder.build(
            longArrayColumn(docs),
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

            final long[] distinctValues = sortedDistinctLongs(slotValues);
            final long[] rebuilt = new long[slotCount];
            for (int slot = 0; slot < slotCount; slot++) {
                rebuilt[slot] = distinctValues[ordinals[slot]];
            }
            assertArrayEquals(docMessage, slotValues, rebuilt);
        }
    }

    /** No document has enough slots to record, so no column may be added at all. */
    public void testLongAllSingleValuedReturnsNull() {
        final long[][] docs = { new long[] { 42L }, null, new long[0], new long[] { -1L } };
        assertNull(ColumnarOffsetsBuilder.build(longArrayColumn(docs), OFFSETS_FIELD, BytesRefRecycler.NON_RECYCLING_INSTANCE));
    }

    /** Repeated values share one ordinal, which is what makes the sidecar smaller than the source it replaces. */
    public void testLongDuplicatesShareOrdinal() throws IOException {
        final long[][] docs = { { 7L, 3L, 7L, 3L, 7L } };
        final LuceneBinaryColumn column = ColumnarOffsetsBuilder.build(
            longArrayColumn(docs),
            OFFSETS_FIELD,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        // The sorted distinct values are [3, 7], so every 3 maps to ordinal 0 and every 7 to ordinal 1.
        assertArrayEquals(new int[] { 1, 0, 1, 0, 1 }, readOffsets(column, docs.length)[0]);
    }

    /** Same property as {@link #testLongRoundTrip}, over byte-string elements. */
    public void testBytesRoundTrip() throws IOException {
        final int docCount = between(1, 50);
        final BytesRef[][] docs = new BytesRef[docCount][];
        final BytesRef[] pool = bytesPool();
        for (int doc = 0; doc < docCount; doc++) {
            if (rarely()) {
                continue; // absent document
            }
            docs[doc] = randomBytesSlots(rarely() ? between(20, 40) : between(0, 8), pool);
        }
        // Guarantee at least one recording document so the assertions below are never vacuous.
        docs[randomInt(docCount - 1)] = randomBytesSlots(between(ColumnarOffsetsBuilder.MIN_RECORDED_SLOTS, 8), pool);

        final LuceneBinaryColumn column = ColumnarOffsetsBuilder.build(
            bytesArrayColumn(docs, randomBoolean()),
            OFFSETS_FIELD,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        final String message = "input " + Arrays.deepToString(docs);
        assertNotNull(message, column);
        assertEquals(message, OFFSETS_FIELD, column.name());
        assertSame(message, SortedDocValuesField.TYPE, column.fieldType());

        final int[][] decoded = readOffsets(column, docCount);
        for (int doc = 0; doc < docCount; doc++) {
            final BytesRef[] slotValues = docs[doc];
            final int slotCount = slotValues == null ? 0 : slotValues.length;
            final String docMessage = message + ", doc " + doc;
            if (slotCount < ColumnarOffsetsBuilder.MIN_RECORDED_SLOTS) {
                assertNull(docMessage, decoded[doc]);
                continue;
            }
            final int[] ordinals = decoded[doc];
            assertNotNull(docMessage, ordinals);
            assertEquals(docMessage, slotCount, ordinals.length);

            final BytesRef[] distinctValues = sortedDistinctBytes(slotValues);
            final BytesRef[] rebuilt = new BytesRef[slotCount];
            for (int slot = 0; slot < slotCount; slot++) {
                rebuilt[slot] = distinctValues[ordinals[slot]];
            }
            assertArrayEquals(docMessage, slotValues, rebuilt);
        }
    }

    /** Pins the two byte-string orderings with no numeric analogue: a prefix sorts first, and so does the empty value. */
    public void testBytesPrefixAndEmptyOrdering() throws IOException {
        final BytesRef[][] docs = bytesDoc("", "a", "ab", "", "b", "a");
        final LuceneBinaryColumn column = ColumnarOffsetsBuilder.build(
            bytesArrayColumn(docs, randomBoolean()),
            OFFSETS_FIELD,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        // The sorted distinct values are ["", "a", "ab", "b"] — "ab" precedes "b" on the second byte.
        assertArrayEquals(new int[] { 0, 1, 2, 0, 3, 1 }, readOffsets(column, docs.length)[0]);
    }

    /** Every value empty gives a zero-length gather buffer, and one ordinal covers every slot. */
    public void testBytesAllEmptyValues() throws IOException {
        final BytesRef[][] docs = bytesDoc("", "", "");
        final LuceneBinaryColumn column = ColumnarOffsetsBuilder.build(
            bytesArrayColumn(docs, randomBoolean()),
            OFFSETS_FIELD,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        assertArrayEquals(new int[] { 0, 0, 0 }, readOffsets(column, docs.length)[0]);
    }

    /** A small pool makes duplicates frequent; the extremes catch signed-ordering slips. */
    private long[] longPool() {
        return new long[] { 0L, 1L, -1L, 7L, 3L, Long.MIN_VALUE, Long.MAX_VALUE, randomLong() };
    }

    private long[] randomLongSlots(int slotCount, long[] pool) {
        final long[] values = new long[slotCount];
        for (int slot = 0; slot < slotCount; slot++) {
            values[slot] = pool[randomInt(pool.length - 1)];
        }
        return values;
    }

    /**
     * {@code hintArray} pins the child kind, so a batch holding only empty arrays still finishes as ARRAY
     * instead of being rewritten to a UNION. A null entry in {@code docs} is an absent document.
     */
    private static EscfColumn longArrayColumn(long[][] docs) {
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

    /** Independent of the production dedup: the value list the read side reconstructs from doc values. */
    private static long[] sortedDistinctLongs(long[] values) {
        return Arrays.stream(values).sorted().distinct().toArray();
    }

    /**
     * Empty and prefix values, a multi-byte character, and {@code U+FFFF} against {@code U+10000} — a pair
     * whose UTF-16 order is the reverse of its UTF-8 order. The wide value forces the gather buffer to be
     * sized from the widest document.
     */
    private BytesRef[] bytesPool() {
        return new BytesRef[] {
            new BytesRef(""),
            new BytesRef("a"),
            new BytesRef("ab"),
            new BytesRef("abc"),
            new BytesRef("b"),
            new BytesRef("\u00e9"),
            new BytesRef("\uffff"),
            new BytesRef("\ud800\udc00"),
            new BytesRef(randomAlphaOfLength(between(50, 200))) };
    }

    private BytesRef[] randomBytesSlots(int slotCount, BytesRef[] pool) {
        final BytesRef[] values = new BytesRef[slotCount];
        for (int slot = 0; slot < slotCount; slot++) {
            values[slot] = pool[randomInt(pool.length - 1)];
        }
        return values;
    }

    /** A single document holding {@code values}, for the cases that pin an exact ordinal array. */
    private static BytesRef[][] bytesDoc(String... values) {
        final BytesRef[] refs = new BytesRef[values.length];
        for (int i = 0; i < values.length; i++) {
            refs[i] = new BytesRef(values[i]);
        }
        return new BytesRef[][] { refs };
    }

    /** STRING and BINARY elements take the same encoder, so tests randomize {@code binaryElements}. */
    private static EscfColumn bytesArrayColumn(BytesRef[][] docs, boolean binaryElements) {
        final EscfColumnBuilder builder = new EscfColumnBuilder(
            EscfColumnBuilder.CollisionPolicy.MERGE,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
        builder.hintArray(binaryElements ? EscfColumnKind.BINARY : EscfColumnKind.STRING);
        for (int doc = 0; doc < docs.length; doc++) {
            if (docs[doc] != null) {
                builder.beginArray(doc);
                for (BytesRef value : docs[doc]) {
                    if (binaryElements) {
                        builder.appendBinary(value);
                    } else {
                        builder.appendString(value);
                    }
                }
                builder.endArray();
            }
        }
        return EscfColumn.from(builder.finish(docs.length));
    }

    /**
     * Independent of the production dedup, and deliberately {@link BytesRef#compareTo} rather than
     * {@link String#compareTo}: the two disagree above the BMP, and only unsigned byte order matches what
     * {@code SortedSetDocValues} assigns ordinals in.
     */
    private static BytesRef[] sortedDistinctBytes(BytesRef[] values) {
        return Arrays.stream(values).sorted().distinct().toArray(BytesRef[]::new);
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
}
