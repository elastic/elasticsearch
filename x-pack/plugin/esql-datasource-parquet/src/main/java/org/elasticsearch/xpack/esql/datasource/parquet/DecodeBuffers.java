/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import java.util.BitSet;

/**
 * Reusable decode buffers for {@link PageColumnReader} batch operations. Eliminates per-batch
 * allocation of primitive arrays by keeping warm arrays that are reused across batches.
 *
 * <p>Each buffer type is lazily allocated and grown as needed. {@code BlockFactory.newXxxArrayVector}
 * wraps arrays by reference (no copy), so each {@link PageColumnReader} must own its own
 * {@code DecodeBuffers} instance. Within a single column, the {@link #takeLongs()} and
 * {@link #takeDoubles()} methods alternate between two slots so the previous batch's array can
 * be handed off to a Block while the next batch decodes into the alternate slot.
 *
 * <p><b>Important:</b> each {@link PageColumnReader} must own its own {@code DecodeBuffers} instance.
 * Sharing across columns causes block corruption because {@code BlockFactory.newXxxArrayVector()}
 * wraps arrays by reference. When two columns share a buffer, the second column's decode overwrites
 * the first column's live block data. The double-buffering for longs/doubles (slot A/B via
 * {@link #takeLongs()}/{@link #takeDoubles()}) handles at most 2 consumers but fails with 3+.
 *
 * <p>TODO: when {@code ColumnBlockConversions} gains an {@code ownsArray=true} overload (zero-copy
 * handoff), revisit sharing: a single DecodeBuffers per row group would reduce object overhead if
 * blocks take ownership and the buffer is immediately recycled.
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
     * Returns the current long buffer and swaps to the alternate slot. The caller takes
     * exclusive ownership of the returned array (e.g., to hand off to a Block without copying).
     * The next call to {@link #longs(int)} will use the alternate slot, avoiding reallocation
     * as long as the previous buffer is released before the one after next is taken.
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
     * Returns the current double buffer and swaps to the alternate slot. The caller takes
     * exclusive ownership of the returned array (e.g., to hand off to a Block without copying).
     * The next call to {@link #doubles(int)} will use the alternate slot, avoiding reallocation
     * as long as the previous buffer is released before the one after next is taken.
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
