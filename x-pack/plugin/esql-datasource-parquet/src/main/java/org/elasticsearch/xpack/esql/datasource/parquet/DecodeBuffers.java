/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import java.util.BitSet;

/**
 * Reusable scratch buffers for {@link PageColumnReader} batch decoding. Eliminates per-batch
 * allocation of primitive arrays by keeping warm arrays that are reused across batches within
 * a single column reader.
 *
 * <p>Buffers are pure scratch space: {@link PageColumnReader} decodes into them, then copies the
 * decoded slice into a freshly allocated, tightly sized array before handing it to
 * {@code BlockFactory}. The Block therefore never aliases anything inside this class, so the
 * reader is free to overwrite the scratch arrays on the next batch. See the {@code Block
 * construction helpers} section in {@link PageColumnReader} for the full ownership rationale.
 *
 * <p>Each buffer type is lazily allocated and grown as needed. Longs and doubles still rotate
 * between two slots (see {@link #takeLongs()} / {@link #takeDoubles()}); after the copy-on-emit
 * fix this is no longer required for correctness - one slot would be sufficient - and is kept
 * only to avoid changing the buffer-growth profile in the same patch. Collapsing to a single
 * slot is left as a follow-up.
 *
 * <p>Each {@link PageColumnReader} owns its own instance. Sharing across columns is unnecessary
 * and would only complicate ownership without buying anything.
 */
final class DecodeBuffers {

    private int[] intBuf;
    private long[] longBufA;
    private long[] longBufB;
    private boolean longSlotA = true;
    private double[] doubleBufA;
    private double[] doubleBufB;
    private boolean doubleSlotA = true;
    private boolean[] boolBuf;
    private BitSet nullsBuf;

    DecodeBuffers() {}

    int[] ints(int minSize) {
        if (intBuf == null || intBuf.length < minSize) {
            intBuf = new int[minSize];
        }
        return intBuf;
    }

    long[] longs(int minSize) {
        if (longSlotA) {
            if (longBufA == null || longBufA.length < minSize) {
                longBufA = new long[minSize];
            }
            return longBufA;
        } else {
            if (longBufB == null || longBufB.length < minSize) {
                longBufB = new long[minSize];
            }
            return longBufB;
        }
    }

    /**
     * Returns the current long scratch buffer and swaps to the alternate slot for the next
     * call to {@link #longs(int)}. The returned array is still owned by this {@code DecodeBuffers}
     * - callers must not retain it past the next {@code takeLongs()} or {@code longs(int)}
     * call. {@link PageColumnReader} reads the decoded slice and copies it into a fresh,
     * Block-owned array before emit, so this is safe by construction.
     *
     * <p>The slot flip is a historical artifact: with copy-on-emit a single slot would be
     * enough. Kept for now to leave allocation behavior unchanged in this patch.
     */
    long[] takeLongs() {
        if (longSlotA) {
            longSlotA = false;
            return longBufA;
        } else {
            longSlotA = true;
            return longBufB;
        }
    }

    double[] doubles(int minSize) {
        if (doubleSlotA) {
            if (doubleBufA == null || doubleBufA.length < minSize) {
                doubleBufA = new double[minSize];
            }
            return doubleBufA;
        } else {
            if (doubleBufB == null || doubleBufB.length < minSize) {
                doubleBufB = new double[minSize];
            }
            return doubleBufB;
        }
    }

    /**
     * Returns the current double scratch buffer and swaps to the alternate slot. Same ownership
     * rules as {@link #takeLongs()}: the array stays owned by this {@code DecodeBuffers} and
     * must not be retained past the next call. {@link PageColumnReader} copies the decoded
     * slice into a Block-owned array before emit.
     */
    double[] takeDoubles() {
        if (doubleSlotA) {
            doubleSlotA = false;
            return doubleBufA;
        } else {
            doubleSlotA = true;
            return doubleBufB;
        }
    }

    boolean[] booleans(int minSize) {
        if (boolBuf == null || boolBuf.length < minSize) {
            boolBuf = new boolean[minSize];
        }
        return boolBuf;
    }

    /**
     * Returns a reusable {@link BitSet} cleared to all-false.
     */
    BitSet nulls(int minSize) {
        if (nullsBuf == null) {
            nullsBuf = new BitSet(minSize);
        } else {
            nullsBuf.clear();
        }
        return nullsBuf;
    }

    private WordMask nullsMask;
    private WordMask valueSelMask;

    WordMask nullsMask(int numBits) {
        if (nullsMask == null) nullsMask = new WordMask();
        nullsMask.reset(numBits);
        return nullsMask;
    }

    WordMask valueSelection(int numBits) {
        if (valueSelMask == null) valueSelMask = new WordMask();
        valueSelMask.reset(numBits);
        return valueSelMask;
    }
}
