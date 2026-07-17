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
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;

import java.util.Arrays;

/**
 * A row-addressed, direct-write column builder for ESCF array columns. Unlike the append-only
 * {@link EscfColumnBuilder} (which is position-implicit and is the exclusive province of
 * {@link EscfEncoder}), this builder accepts values keyed by a non-decreasing batch-local row
 * number and materialises a native {@link EscfColumnData} of kind {@link EscfColumnKind#ARRAY} at
 * {@link #finish}.
 *
 * <p>Create via {@link #arrayOfString} for the {@code _field_names} use case. Row numbers must be
 * non-decreasing across calls; supplying the same row number multiple times appends another element
 * to that row's array (multi-value). Rows that are never supplied are treated as absent in the
 * output (empty range in the {@code rowOffsets} vector).
 *
 * <p>Value bytes are written directly to an in-memory stream the instant {@link #setString} is
 * called — there is no per-column buffering. Only the fence metadata ({@code rowOffsets},
 * {@code childOffsets}) is accumulated alongside the stream.
 *
 * <p>This class is not thread-safe.
 */
public final class EscfRowColumnBuilder {

    /** Element byte stream — written directly when setString is called. */
    private final RecyclerBytesStreamOutput childData;
    /**
     * {@code childOffsets[i]} is the byte offset in {@code childData} of the start of element
     * {@code i}. Grown on demand; sealed with the final length sentinel in {@link #finish}.
     */
    private int[] childOffsets;
    /**
     * {@code rowOffsets[r]} is the index of the first element belonging to row {@code r} in the
     * child column. Grown on demand; sealed with {@code elemCount} in {@link #finish}.
     */
    private int[] rowOffsets;
    /** Running count of elements appended so far. */
    private int elemCount;
    /** Running byte length of {@code childData}, tracked to avoid calling {@code size()} on the stream. */
    private int childDataLen;
    /**
     * The last row that was opened (i.e., {@code rowOffsets[currentRow]} set). Starts at
     * {@code -1} (before row 0) to indicate that no row has been opened yet.
     */
    private int currentRow;

    private EscfRowColumnBuilder(Recycler<BytesRef> recycler) {
        this.childData = new RecyclerBytesStreamOutput(recycler);
        this.childOffsets = new int[16];
        this.rowOffsets = new int[16];
        this.elemCount = 0;
        this.childDataLen = 0;
        this.currentRow = -1;
    }

    /**
     * Returns a new builder pre-configured for an array-of-string ({@link EscfColumnKind#ARRAY}
     * with a {@link EscfColumnKind#STRING} child) column. Every call to
     * {@link #setString(int, BytesRef)} adds one string element to the specified row; rows not
     * supplied yield an empty range (absent) in the output.
     *
     * @param recycler backing memory source for the element byte stream. Use
     *                 {@link org.elasticsearch.transport.BytesRefRecycler#NON_RECYCLING_INSTANCE}
     *                 when the column's backing bytes will not be explicitly released (e.g. the
     *                 batch-mapping columnar path, which has no per-column lifecycle plumbing).
     */
    public static EscfRowColumnBuilder arrayOfString(Recycler<BytesRef> recycler) {
        return new EscfRowColumnBuilder(recycler);
    }

    /**
     * Returns {@code true} if no elements have been contributed to this builder yet. A column built
     * from an empty builder contains all-absent rows; callers may skip {@link #finish} entirely in
     * that case.
     */
    public boolean isEmpty() {
        return elemCount == 0;
    }

    /**
     * Records {@code value} as the next string element for the array at {@code row}. The row
     * number must be non-decreasing across calls; supplying the same row number as the previous
     * call appends another element to that row's array (multi-value support).
     *
     * <p>Bytes are written immediately to the element stream — no intermediate copy is made.
     *
     * @param row   the batch-local document index (0-based), must be ≥ 0 and ≥ any previously
     *              supplied row
     * @param value the string element to record; its UTF-8 bytes are written directly
     */
    public void setString(int row, BytesRef value) {
        assert row >= 0;
        assert currentRow == -1 || row >= currentRow
            : "row " + row + " is less than the previous row " + currentRow + " (rows must be non-decreasing)";
        if (row > currentRow) {
            advanceTo(row);
        }
        // Append element: record start byte offset, then write bytes.
        childOffsets = ensureIntCapacity(childOffsets, elemCount + 1);
        childOffsets[elemCount] = childDataLen;
        childData.writeBytes(value.bytes, value.offset, value.length);
        childDataLen += value.length;
        elemCount++;
    }

    /**
     * Seals the builder, fills trailing absent rows for {@code (currentRow, docCount)}, and returns
     * the completed column data. The builder must not be used after this call.
     *
     * @param docCount total number of documents in the batch; must be &gt; {@code currentRow}
     * @return a native {@link EscfColumnData} of kind {@link EscfColumnKind#ARRAY} with a
     *         {@link EscfColumnKind#STRING} child; docs with no elements are empty ranges in the
     *         {@code rowOffsets} vector (absent = {@code null}, using the empty-range convention)
     */
    public EscfColumnData finish(int docCount) {
        // Fill trailing absent rows [currentRow+1, docCount-1] and set the final fence.
        // The loop includes docCount itself to record rowOffsets[docCount] = elemCount.
        for (int r = currentRow + 1; r <= docCount; r++) {
            rowOffsets = ensureIntCapacity(rowOffsets, r + 1);
            rowOffsets[r] = elemCount;
        }
        // Seal childOffsets with the final byte-length sentinel.
        childOffsets = ensureIntCapacity(childOffsets, elemCount + 1);
        childOffsets[elemCount] = childDataLen;

        EscfColumnData child = EscfColumnData.ofVarWidth(
            EscfColumnKind.STRING,
            elemCount,
            null, // child holds only real elements; no absent slots
            Arrays.copyOf(childOffsets, elemCount + 1),
            childData.moveToBytesReference()
        );
        return EscfColumnData.ofArray(
            docCount,
            null, // absent indicated by empty rowOffsets ranges, not a bitset
            Arrays.copyOf(rowOffsets, docCount + 1),
            child
        );
    }

    /**
     * Opens all rows in {@code (currentRow, targetRow]}, recording their starting element index
     * in {@code rowOffsets}. Intervening unvisited rows receive an empty range (start == end ==
     * {@code elemCount}), which marks them absent in the output.
     */
    private void advanceTo(int targetRow) {
        for (int r = currentRow + 1; r <= targetRow; r++) {
            rowOffsets = ensureIntCapacity(rowOffsets, r + 1);
            rowOffsets[r] = elemCount;
        }
        currentRow = targetRow;
    }

    private static int[] ensureIntCapacity(int[] array, int minSize) {
        return array.length >= minSize ? array : Arrays.copyOf(array, Math.max(minSize, array.length * 2));
    }
}
