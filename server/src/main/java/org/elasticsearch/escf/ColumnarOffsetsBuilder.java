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
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;

import java.util.Arrays;

/**
 * Builds the {@code <field>.offsets} sidecar column on the columnar indexing path — the counterpart of
 * the row path's {@code FieldArrayContext.Offsets}, and what lets synthetic {@code _source} recover a
 * leaf array's original order and duplicates.
 *
 * <p>Doc values keep each document's values sorted and deduplicated, which discards both the array's
 * order and its repeats. Rather than also store the array, this sidecar stores one ordinal per source
 * slot — an index into those sorted distinct values — and the reader reassembles the array from the two
 * halves. So the ordering work happens twice: once to write the values, once to describe how to undo it.
 * The win comes from the repeats, which means an array of all-distinct values pays for its ordinals and
 * saves nothing.
 *
 * <p>Each recorded document holds {@code [slot count][ordinal 0][ordinal 1]...}, every entry a zig-zag
 * encoded vint. The decoders are shared with the row path, so this has to stay byte-compatible with
 * {@code FieldArrayContext#encodeOffsetArray}.
 *
 * <p>{@code source} is an {@code ARRAY} column whose elements are already in the encoding the doc-values
 * writer will sort — for {@code LONG} elements, the sortable-long output of
 * {@link NumberColumnTransform}. Ordinals have to agree with the order the values are read back in, so a
 * column encoded any other way misorders the array silently. Byte-string elements are limited to
 * {@code IndexWriter.MAX_TERM_LENGTH}.
 *
 * <p>Elements must be {@code LONG}, {@code STRING} or {@code BINARY}, which is also why no
 * {@code NULL_ORD} slot is ever emitted: a null element makes the element child a {@code UNION}.
 */
public final class ColumnarOffsetsBuilder {

    // Matches the SortedDocValuesField the row path adds in FieldArrayContext#addToLuceneDocument.
    private static final IndexableFieldType OFFSETS_FIELD_TYPE = SortedDocValuesField.TYPE;

    /**
     * Documents with fewer slots than this record nothing: the values column already carries everything a
     * single value can express about order and shape. The row path applies the same skip in
     * {@code FieldArrayContext#addToLuceneDocument}.
     *
     * <p>An {@code ARRAY} column represents an absent row as a zero-width offset range, so this one check
     * also excludes absent rows and empty arrays.
     */
    static final int MIN_RECORDED_SLOTS = 2;

    private ColumnarOffsetsBuilder() {}

    /**
     * Builds the sidecar column for {@code source}, an {@code ARRAY} column of long, string or binary
     * elements already in the doc-values encoding described on the class javadoc.
     *
     * @return the sidecar column, or {@code null} when no document in the batch had enough slots to record
     */
    @Nullable
    public static LuceneBinaryColumn build(EscfColumn source, String offsetsFieldName, Recycler<BytesRef> recycler) {
        // ARRAY is EscfArrayColumn by construction; see EscfColumn#from, the only place columns are built.
        assert source instanceof EscfArrayColumn : "expected ARRAY, got " + EscfColumnKind.name(source.kind());
        final EscfArrayColumn arrayColumn = (EscfArrayColumn) source;

        final EscfColumnData data = arrayColumn.columnData();
        final int[] rowOffsets = data.offsets();
        final int docCount = data.docCount();
        final int maxSlotCount = findMaxSlotCount(rowOffsets, docCount);
        // Also the empty-batch test: if any document records at all, maxSlotCount reaches MIN_RECORDED_SLOTS.
        if (maxSlotCount < MIN_RECORDED_SLOTS) {
            return null;
        }
        final LongTupleCursor cursor = switch (arrayColumn.leafValueKind()) {
            case EscfColumnKind.LONG -> arrayColumn.longCursor();
            case EscfColumnKind.STRING, EscfColumnKind.BINARY -> batchRankCursor(arrayColumn, rowOffsets, docCount);
            default -> throw new AssertionError("unexpected element kind: " + EscfColumnKind.name(arrayColumn.leafValueKind()));
        };
        return encodeColumn(cursor, rowOffsets, docCount, maxSlotCount, offsetsFieldName, recycler);
    }

    /** Widest per-document slot range. */
    private static int findMaxSlotCount(int[] rowOffsets, int docCount) {
        int max = 0;
        for (int doc = 0; doc < docCount; doc++) {
            max = Math.max(max, rowOffsets[doc + 1] - rowOffsets[doc]);
        }
        return max;
    }

    private static LuceneBinaryColumn encodeColumn(
        LongTupleCursor cursor,
        int[] rowOffsets,
        int docCount,
        int maxSlotCount,
        String offsetsFieldName,
        Recycler<BytesRef> recycler
    ) {
        final EscfColumnBuilder columnBuilder = newBinaryBuilder(recycler);
        final long[] slotValues = new long[maxSlotCount];
        final long[] sortScratch = new long[maxSlotCount];
        final int[] slotOrdinals = new int[maxSlotCount];

        // One stream for the whole batch, rewound per document: bytes() is bounded by the stream position
        // and setBinary copies immediately, so rewriting over the previous document is safe. The stream
        // grows to the widest document's encoding and then stops allocating.
        try (RecyclerBytesStreamOutput encoded = new RecyclerBytesStreamOutput(recycler)) {
            for (int doc = 0; doc < docCount; doc++) {
                final int slotCount = rowOffsets[doc + 1] - rowOffsets[doc];
                // Drains skipped documents too: the cursor advances per element, so every document's slots
                // are consumed to keep it in step with rowOffsets.
                gatherSlots(cursor, doc, slotCount, slotValues);
                if (slotCount < MIN_RECORDED_SLOTS) {
                    continue;
                }
                copySorted(slotValues, slotCount, sortScratch);
                final int distinctCount = dedupSorted(sortScratch, slotCount);
                assignOrdinals(slotValues, slotCount, sortScratch, distinctCount, slotOrdinals);

                encoded.seek(0);
                writeSlotOrdinals(encoded, slotOrdinals, slotCount);
                columnBuilder.setBinary(doc, encoded.bytes().toBytesRef());
            }
        }
        return LuceneBinaryColumn.of(columnBuilder.finish(docCount), offsetsFieldName, OFFSETS_FIELD_TYPE);
    }

    private static void gatherSlots(LongTupleCursor cursor, int doc, int slotCount, long[] slotValues) {
        for (int slot = 0; slot < slotCount; slot++) {
            final int advanced = cursor.nextDoc();
            assert advanced == doc : "cursor desynchronized from row offsets: at doc " + advanced + ", expected " + doc;
            slotValues[slot] = cursor.longValue();
        }
    }

    private static void copySorted(long[] slotValues, int slotCount, long[] sorted) {
        System.arraycopy(slotValues, 0, sorted, 0, slotCount);
        Arrays.sort(sorted, 0, slotCount);
    }

    /**
     * Compacts runs of equal values in the {@code slotCount}-long prefix of {@code sorted}, in place.
     *
     * @return the number of distinct values, which is the length of the compacted prefix
     */
    private static int dedupSorted(long[] sorted, int slotCount) {
        assert slotCount >= MIN_RECORDED_SLOTS : "slotCount " + slotCount;
        int distinctCount = 1;
        for (int i = 1; i < slotCount; i++) {
            if (sorted[i] != sorted[distinctCount - 1]) {
                sorted[distinctCount++] = sorted[i];
            }
        }
        return distinctCount;
    }

    private static void assignOrdinals(long[] slotValues, int slotCount, long[] distinctValues, int distinctCount, int[] slotOrdinals) {
        for (int slot = 0; slot < slotCount; slot++) {
            final int ord = Arrays.binarySearch(distinctValues, 0, distinctCount, slotValues[slot]);
            assert ord >= 0 : "slot value " + slotValues[slot] + " missing from its own distinct value set";
            slotOrdinals[slot] = ord;
        }
    }

    private static void writeSlotOrdinals(RecyclerBytesStreamOutput out, int[] slotOrdinals, int slotCount) {
        out.writeVInt(BitUtil.zigZagEncode(slotCount));
        for (int slot = 0; slot < slotCount; slot++) {
            out.writeVInt(BitUtil.zigZagEncode(slotOrdinals[slot]));
        }
    }

    private static EscfColumnBuilder newBinaryBuilder(Recycler<BytesRef> recycler) {
        final EscfColumnBuilder builder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE, recycler);
        builder.lockScalar(EscfColumnKind.BINARY);
        return builder;
    }

    /**
     * Returns each element's rank among the batch's distinct values in element order (unsigned byte /
     * {@code BytesRef} order). Elements from documents with fewer than {@link #MIN_RECORDED_SLOTS} slots
     * are drained without hashing.
     */
    private static int[] batchRanksPerSlot(EscfArrayColumn source, int[] rowOffsets, int docCount) {
        final int totalSlots = rowOffsets[docCount];
        final BytesRefHash values = new BytesRefHash();
        final int[] termIds = new int[totalSlots];
        // retainValues=false: add() copies each value into the hash's pool before the cursor advances.
        final ObjectTupleCursor<BytesRef> cursor = source.bytesRefCursor(false);
        for (int doc = 0; doc < docCount; doc++) {
            final int slotStart = rowOffsets[doc];
            final int slotCount = rowOffsets[doc + 1] - slotStart;
            final boolean needsHash = slotCount >= MIN_RECORDED_SLOTS;
            for (int slot = 0; slot < slotCount; slot++) {
                final int advanced = cursor.nextDoc();
                assert advanced == doc : "cursor desynchronized from row offsets: at doc " + advanced + ", expected " + doc;
                if (needsHash) {
                    final int id = values.add(cursor.value());
                    // add() returns -(id)-1 for a value it has already interned.
                    termIds[slotStart + slot] = id < 0 ? -id - 1 : id;
                }
            }
        }

        // sort() returns ids in lexicographic order, so inverting it maps an id to its rank. It also
        // consumes the hash table, which is why nothing looks a value up after this point.
        final int[] sortedIds = values.sort();
        final int[] rankPerId = new int[values.size()];
        for (int rank = 0; rank < rankPerId.length; rank++) {
            rankPerId[sortedIds[rank]] = rank;
        }
        for (int i = 0; i < totalSlots; i++) {
            termIds[i] = rankPerId[termIds[i]];
        }
        return termIds;
    }

    /**
     * Exposes {@link #batchRanksPerSlot} as a cursor with the same shape {@link EscfArrayColumn#longCursor}
     * produces: one doc id per element, with empty and absent rows skipped by their zero-width offset range.
     */
    private static LongTupleCursor batchRankCursor(EscfArrayColumn source, int[] rowOffsets, int docCount) {
        final int[] slotRanks = batchRanksPerSlot(source, rowOffsets, docCount);
        return new LongTupleCursor() {
            private int currentDoc = -1;
            private int rowEnd = 0;
            private int remainingInRow = 0;
            private int nextSlot = 0;
            private long currentValue;

            @Override
            public int nextDoc() {
                while (remainingInRow == 0) {
                    if (currentDoc + 1 >= docCount) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    currentDoc++;
                    final int nextEnd = rowOffsets[currentDoc + 1];
                    remainingInRow = nextEnd - rowEnd;
                    rowEnd = nextEnd;
                }
                remainingInRow--;
                // longValue() may be called more than once per nextDoc(), so the value is read here.
                currentValue = slotRanks[nextSlot++];
                return currentDoc;
            }

            @Override
            public long longValue() {
                return currentValue;
            }
        };
    }
}
