/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

/**
 * Row-addressed accumulator for a single ESCF column of a fixed kind (STRING, BINARY, LONG, or DOUBLE).
 * Accepts {@code (row, value)} pairs with non-decreasing row numbers; rows never supplied are absent.
 * Starts scalar and promotes to {@link EscfColumnKind#ARRAY} automatically when any row receives
 * more than one value. Create via a typed factory; seal with {@link #finish}. Not thread-safe.
 */
public final class EscfRowColumnBuilder {

    private final byte kind;
    private final Recycler<BytesRef> recycler;
    /** Written positionally for fixed64 scalar mode; swapped to element-packed at the first multi-value. */
    private RecyclerBytesStreamOutput childData;
    /** {@code childOffsets[i]}: byte offset of element {@code i} in {@code childData}. */
    private int[] childOffsets;
    /** {@code rowOffsets[r]}: index of the first element for row {@code r}. */
    private int[] rowOffsets;
    private int elemCount;
    /** Tracked separately to avoid calling {@code size()} on the stream. */
    private int childDataLen;
    /** Last row opened; {@code -1} before any row is written. */
    private int currentRow;
    /** {@code true} once any row receives a second element, triggering ARRAY output in {@link #finish}. */
    private boolean multivalued;

    private EscfRowColumnBuilder(byte kind, Recycler<BytesRef> recycler) {
        assert kind == EscfColumnKind.STRING
            || kind == EscfColumnKind.BINARY
            || kind == EscfColumnKind.LONG
            || kind == EscfColumnKind.DOUBLE : "unsupported kind: " + EscfColumnKind.name(kind);
        this.kind = kind;
        this.recycler = recycler;
        this.childData = new RecyclerBytesStreamOutput(recycler);
        this.childOffsets = new int[16];
        this.rowOffsets = new int[16];
        this.elemCount = 0;
        this.childDataLen = 0;
        this.currentRow = -1;
        this.multivalued = false;
    }

    /** Returns a new builder for {@link EscfColumnKind#STRING} columns. */
    public static EscfRowColumnBuilder strings(Recycler<BytesRef> recycler) {
        return new EscfRowColumnBuilder(EscfColumnKind.STRING, recycler);
    }

    /** Returns a new builder for {@link EscfColumnKind#BINARY} columns. */
    public static EscfRowColumnBuilder binaries(Recycler<BytesRef> recycler) {
        return new EscfRowColumnBuilder(EscfColumnKind.BINARY, recycler);
    }

    /** Returns a new builder for {@link EscfColumnKind#LONG} columns. */
    public static EscfRowColumnBuilder longs(Recycler<BytesRef> recycler) {
        return new EscfRowColumnBuilder(EscfColumnKind.LONG, recycler);
    }

    /** Returns a new builder for {@link EscfColumnKind#DOUBLE} columns. */
    public static EscfRowColumnBuilder doubles(Recycler<BytesRef> recycler) {
        return new EscfRowColumnBuilder(EscfColumnKind.DOUBLE, recycler);
    }

    /** Returns {@code true} if no elements have been recorded; callers may skip {@link #finish} in this case. */
    public boolean isEmpty() {
        return elemCount == 0;
    }

    /** Appends a string value for {@code row}; builder must be {@link #strings}. */
    public void setString(int row, BytesRef value) {
        assertKind(EscfColumnKind.STRING);
        appendVarElement(row, value);
    }

    /** Appends a binary value for {@code row}; builder must be {@link #binaries}. */
    public void setBinary(int row, BytesRef value) {
        assertKind(EscfColumnKind.BINARY);
        appendVarElement(row, value);
    }

    /** Appends a long value for {@code row}; builder must be {@link #longs}. */
    public void setLong(int row, long value) {
        assertKind(EscfColumnKind.LONG);
        appendElement(row);
        writeLongLE(value);
        childDataLen += Long.BYTES;
    }

    /** Appends a double value for {@code row}; builder must be {@link #doubles}. */
    public void setDouble(int row, double value) {
        assertKind(EscfColumnKind.DOUBLE);
        appendElement(row);
        writeLongLE(Double.doubleToRawLongBits(value));
        childDataLen += Long.BYTES;
    }

    private void assertKind(byte expected) {
        assert kind == expected : EscfColumnKind.name(expected) + " setter called on " + EscfColumnKind.name(kind) + " builder";
    }

    /**
     * Seals the builder and returns the completed column. Emits a scalar column of the fixed kind
     * when every document received at most one value (absent docs tracked in a validity bitset), or
     * an {@link EscfColumnKind#ARRAY} column otherwise. Must not be called again after this.
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

        if (multivalued) {
            return finishArray(docCount);
        } else {
            return finishScalar(docCount);
        }
    }

    private void appendVarElement(int row, BytesRef value) {
        appendElement(row);
        childData.writeBytes(value.bytes, value.offset, value.length);
        childDataLen += value.length;
    }

    private void writeLongLE(long value) {
        try {
            childData.writeLongLE(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void appendElement(int row) {
        assert row >= 0;
        assert currentRow == -1 || row >= currentRow
            : "row " + row + " is less than the previous row " + currentRow + " (rows must be non-decreasing)";
        if (row > currentRow) {
            advanceTo(row);
        } else {
            // row == currentRow: second (or later) element for this doc → promote to ARRAY.
            if (!multivalued && (kind == EscfColumnKind.LONG || kind == EscfColumnKind.DOUBLE)) {
                // First transition for fixed64: take ownership of the positional pages, swap in a
                // fresh stream, then compact existing elements element-packed into it.
                BytesReference positional = childData.moveToBytesReference();
                childData = new RecyclerBytesStreamOutput(recycler);
                try {
                    for (int i = 0; i < elemCount; i++) {
                        positional.slice(childOffsets[i], Long.BYTES).writeTo(childData);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            multivalued = true;
        }
        childOffsets = ensureIntCapacity(childOffsets, elemCount + 1);
        childOffsets[elemCount] = childDataLen;
        elemCount++;
    }

    /** Advances to {@code targetRow}, filling skipped rows with empty element ranges (absent). */
    private void advanceTo(int targetRow) {
        for (int r = currentRow + 1; r <= targetRow; r++) {
            rowOffsets = ensureIntCapacity(rowOffsets, r + 1);
            rowOffsets[r] = elemCount;
        }
        if (!multivalued && (kind == EscfColumnKind.LONG || kind == EscfColumnKind.DOUBLE)) {
            // Positional scalar mode: write zeros for absent slots so childData is a docCount*8 buffer.
            int absentCount = targetRow - currentRow - 1;
            if (absentCount > 0) {
                skipBytes(absentCount * Long.BYTES);
            }
        }
        currentRow = targetRow;
    }

    private void skipBytes(int byteCount) {
        childData.skip(byteCount);
        childDataLen += byteCount;
    }

    /** Builds the ARRAY result when any document received two or more elements. */
    private EscfColumnData finishArray(int docCount) {
        EscfColumnData child = buildChildForArray();
        return EscfColumnData.ofArray(docCount, null, Arrays.copyOf(rowOffsets, docCount + 1), child);
    }

    /**
     * Builds the scalar result when every document received at most one element. Absent documents
     * (those never written) are marked in a validity bitset.
     */
    private EscfColumnData finishScalar(int docCount) {
        // Check for absent docs. docCount == 0 → no absent possible.
        boolean hasAbsent = false;
        for (int d = 0; d < docCount; d++) {
            if (rowOffsets[d] == rowOffsets[d + 1]) {
                hasAbsent = true;
                break;
            }
        }
        final FixedBitSet validity;
        if (hasAbsent) {
            validity = new FixedBitSet(docCount);
            for (int d = 0; d < docCount; d++) {
                if (rowOffsets[d] < rowOffsets[d + 1]) {
                    validity.set(d);
                }
            }
        } else {
            validity = null;
        }

        if (kind == EscfColumnKind.LONG || kind == EscfColumnKind.DOUBLE) {
            return finishScalarFixed64(docCount, validity);
        } else {
            return finishScalarVarWidth(docCount, validity);
        }
    }

    private EscfColumnData finishScalarFixed64(int docCount, FixedBitSet validity) {
        // Complete the positional buffer: absent trailing slots also need zeros.
        int trailingAbsent = docCount - currentRow - 1;
        if (trailingAbsent > 0) {
            skipBytes(trailingAbsent * Long.BYTES);
        }
        // childData is now a positional docCount*8 buffer; absent slots are zero.
        return EscfColumnData.ofFixed64(kind, docCount, validity, childData.bytes());
    }

    private EscfColumnData finishScalarVarWidth(int docCount, FixedBitSet validity) {
        int[] perDocOffsets = new int[docCount + 1];
        for (int d = 0; d <= docCount; d++) {
            perDocOffsets[d] = childOffsets[rowOffsets[d]];
        }
        return EscfColumnData.ofVarWidth(kind, docCount, validity, perDocOffsets, childData.bytes());
    }

    private EscfColumnData buildChildForArray() {
        if (kind == EscfColumnKind.LONG || kind == EscfColumnKind.DOUBLE) {
            // childData was swapped to element-packed at the first multi-value; hand it straight to ofFixed64.
            return EscfColumnData.ofFixed64(kind, elemCount, null, childData.moveToBytesReference());
        } else {
            // Var-width child: elemCount elements with per-element byte boundaries.
            return EscfColumnData.ofVarWidth(kind, elemCount, null, childOffsets, childData.moveToBytesReference());
        }
    }

    private static int[] ensureIntCapacity(int[] array, int minSize) {
        return array.length >= minSize ? array : Arrays.copyOf(array, ArrayUtil.oversize(minSize, Integer.BYTES));
    }
}
