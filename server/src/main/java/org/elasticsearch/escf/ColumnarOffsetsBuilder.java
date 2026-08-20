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
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;

import java.util.Arrays;

/**
 * Builds the {@code <field>.offsets} sidecar column for a multi-valued numeric field on the columnar
 * indexing path — the columnar counterpart of the row path's {@code FieldArrayContext.Offsets}.
 *
 * <p>Offsets exist to compact synthetic {@code _source}: rather than storing the source array, the
 * values column stores each document's sorted distinct values and this sidecar stores one ordinal per
 * source slot, which is what makes the original order and the duplicates recoverable. The saving comes
 * entirely from repetition — with every value distinct there is nothing to fold away and the ordinals
 * are pure overhead, so in the worst case this costs more than the source it replaces. What remains to
 * squeeze is the ordinal encoding: zig-zag varint per ordinal is what the shared decoder expects today,
 * but the sidecar carries ordering only, never values, so it could be packed denser or inlined into the
 * values column (as high-cardinality keyword already does) without touching the values themselves.
 *
 * <p>Each emitted document holds {@code [slot count][ordinal 0][ordinal 1]...}, every entry a zig-zag
 * encoded vint — byte-compatible with {@code FieldArrayContext#encodeOffsetArray} by obligation rather
 * than by shared code, since the decoders are shared with the row path. Ordinals index the sorted
 * distinct values, not the raw doc-values entries: {@code SortedNumericDocValues} retains duplicates
 * and the read side collapses them itself.
 *
 * <p>The input must be the sortable-long column produced by {@link NumberColumnTransform}, not the raw
 * source column. Sorting that encoding is order-preserving, so ordinal order matches the order the
 * values are read back in; and for {@code float} / {@code half_float} its precision loss has to
 * collapse two source values into a single ordinal exactly as the row path's {@code toSortableLong}
 * call does.
 *
 * <p>Producing the ordinals is the awkward part, because the source is not a {@code long[]}: values
 * arrive one element at a time from a cursor that knows nothing about document boundaries, while the
 * boundaries live in a separate row-offsets vector. Hence, the per-document pipeline — gather, sort,
 * dedup, ordinals, encode — with the column assembled from the encoded blobs at the end.
 *
 * <p>Only {@code ARRAY} columns with {@code LONG} elements are handled, so no {@code NULL_ORD} slot is
 * ever emitted: a null element or a mixed null/array batch produces a {@code UNION} column, which the
 * columnar numeric path rejects in favour of the row path.
 */
public final class ColumnarOffsetsBuilder {

    // Matches the SortedDocValuesField the row path adds in FieldArrayContext#addToLuceneDocument.
    private static final IndexableFieldType OFFSETS_FIELD_TYPE = SortedDocValuesField.TYPE;

    /**
     * Documents with fewer slots than this record nothing: a single value carries no ordering or shape
     * information the values column does not already have. The row path applies the same skip in
     * {@code FieldArrayContext#addToLuceneDocument}.
     */
    static final int MIN_RECORDED_SLOTS = 2;

    private ColumnarOffsetsBuilder() {}

    /**
     * Builds the sidecar column for {@code source}, which must be an {@code ARRAY} column of
     * sortable-long elements.
     *
     * @return the sidecar column, or {@code null} when no document in the batch has enough slots to
     *         record — in which case the caller must not add a column at all
     */
    @Nullable
    public static LuceneBinaryColumn build(EscfColumn source, String offsetsFieldName, Recycler<BytesRef> recycler) {
        assert source.kind() == EscfColumnKind.ARRAY : "expected ARRAY, got " + EscfColumnKind.name(source.kind());
        assert source.leafValueKind() == EscfColumnKind.LONG : "expected LONG elements, got " + EscfColumnKind.name(source.leafValueKind());

        final EscfColumnData data = source.columnData();
        final int maxSlotCount = findMaxSlotCount(data.offsets(), data.docCount());
        // Also the empty-batch test: if any document records at all, maxSlotCount reaches MIN_RECORDED_SLOTS.
        if (maxSlotCount < MIN_RECORDED_SLOTS) {
            return null;
        }
        return encodeColumn(source, data, maxSlotCount, offsetsFieldName, recycler);
    }

    /** Widest per-document slot range, which bounds the scratch arrays. */
    private static int findMaxSlotCount(int[] rowOffsets, int docCount) {
        int max = 0;
        for (int doc = 0; doc < docCount; doc++) {
            max = Math.max(max, rowOffsets[doc + 1] - rowOffsets[doc]);
        }
        return max;
    }

    private static LuceneBinaryColumn encodeColumn(
        EscfColumn source,
        EscfColumnData data,
        int maxSlotCount,
        String offsetsFieldName,
        Recycler<BytesRef> recycler
    ) {
        final int[] rowOffsets = data.offsets();
        final int docCount = data.docCount();
        final EscfColumnBuilder columnBuilder = newBinaryBuilder(recycler);
        final LongTupleCursor cursor = source.longCursor();

        final long[] slotValues = new long[maxSlotCount];
        final long[] sortScratch = new long[maxSlotCount];
        final int[] slotOrdinals = new int[maxSlotCount];

        // One stream for the whole batch, rewound per document: bytes() is bounded by the stream position
        // and setBinary copies immediately, so rewriting over the previous document is safe. The stream
        // grows to the widest document's encoding and then stops allocating.
        try (RecyclerBytesStreamOutput encoded = new RecyclerBytesStreamOutput(recycler)) {
            for (int doc = 0; doc < docCount; doc++) {
                final int slotCount = rowOffsets[doc + 1] - rowOffsets[doc];
                // Runs for skipped documents too: the cursor advances per element, so not draining a
                // document's slots would desynchronize it from rowOffsets.
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

    /** Copies before sorting, because ordinal assignment still needs {@code slotValues} in source order. */
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

    /** Zig-zag costs nothing for these non-negative values and is what lets the shared decoder read the row path's {@code -1}. */
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
}
