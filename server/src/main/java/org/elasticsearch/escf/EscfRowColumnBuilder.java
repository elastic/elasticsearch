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
 * Row-addressed builder for ESCF {@link EscfColumnKind#ARRAY} columns. Accepts {@code (row, value)}
 * pairs with non-decreasing row numbers; rows never supplied are absent (empty range) in the output.
 * Create via {@link #arrayOfString}; seal with {@link #finish}. Not thread-safe.
 */
public final class EscfRowColumnBuilder {

    private final RecyclerBytesStreamOutput childData;
    /** {@code childOffsets[i]}: byte offset of element {@code i} in {@code childData}. */
    private int[] childOffsets;
    /** {@code rowOffsets[r]}: index of the first element for row {@code r}. */
    private int[] rowOffsets;
    private int elemCount;
    /** Tracked separately to avoid calling {@code size()} on the stream. */
    private int childDataLen;
    /** Last row opened; {@code -1} before any row is written. */
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
     * Returns a new builder for an {@code ARRAY[STRING]} column.
     * Use {@link org.elasticsearch.transport.BytesRefRecycler#NON_RECYCLING_INSTANCE} when the
     * backing bytes have no explicit lifecycle (e.g. the batch-mapping columnar path).
     */
    public static EscfRowColumnBuilder arrayOfString(Recycler<BytesRef> recycler) {
        return new EscfRowColumnBuilder(recycler);
    }

    /** Returns {@code true} if no elements have been recorded; callers may skip {@link #finish} in this case. */
    public boolean isEmpty() {
        return elemCount == 0;
    }

    /**
     * Appends {@code value} to the array at {@code row}. Row numbers must be non-decreasing;
     * repeating a row number adds another element (multi-value). Bytes are written immediately.
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
     * Seals the builder and returns the completed {@code ARRAY[STRING]} column. Fills trailing
     * absent rows up to {@code docCount}. The builder must not be used after this call.
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

    /** Advances to {@code targetRow}, filling skipped rows with empty ranges (absent). */
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
