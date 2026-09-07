/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.ArrayUtil;

import java.util.Arrays;

/**
 * Surveys a block's raw byte data and produces a {@link FsstSymbolTable} tailored to it.
 *
 * <p>Also exposes the entropy estimate ({@link #distinctByteCount}) used by the caller to gate
 * whether FSST is worth attempting at all, so the block-level codec does not need to re-scan.
 *
 * <p>Instances are not thread-safe and are intended to be owned by a single
 * {@link FsstBlockCodec}. The heavy scratch arrays ({@code bigramFreq}, {@code keys},
 * {@code freqAndKey}) are allocated once per instance and reused across calls to {@link #build},
 * avoiding per-block heap allocation. {@code bigramFreq} is reset selectively — only the entries
 * that were incremented — keeping cleanup cost proportional to the number of distinct bigrams in
 * the block rather than a full {@code BIGRAM_TABLE_SIZE}-entry scan.
 *
 * <p>Configure once and call per block:
 *
 * <pre>{@code
 * FsstSymbolTableBuilder builder = new FsstSymbolTableBuilder().maxSymbolLength(6);
 * FsstSymbolTable table = builder.data(bytes, len).lengths(lengths, count).build();
 * }</pre>
 */
final class FsstSymbolTableBuilder {

    private static final int BIGRAM_TABLE_SIZE = 1 << 16; // 256 * 256 possible byte pairs
    private static final int BYTE_RANGE = 256;

    private int maxSymbolLength = FsstSymbolTable.MAX_SYMBOL_LENGTH;

    private byte[] blockData;
    private int totalLength;
    private int[] pendingLengths;
    private int count;

    private final int[] bigramFreq = new int[BIGRAM_TABLE_SIZE];
    private final int[] keys = new int[BIGRAM_TABLE_SIZE];
    private final long[] freqAndKey = new long[BIGRAM_TABLE_SIZE];
    private final int[] extFreq = new int[BYTE_RANGE];
    private int[] starts = new int[0];

    /**
     * Sets the maximum byte length of a symbol this builder may produce. Longer symbols compress
     * structured strings more aggressively at the cost of a more expensive training scan; shorter
     * symbols reduce training cost but leave more bytes uncovered per block. Must be between 1 and
     * {@link FsstSymbolTable#MAX_SYMBOL_LENGTH} inclusive.
     *
     * <p>Returns {@code this} for chaining.
     */
    FsstSymbolTableBuilder maxSymbolLength(int maxSymbolLength) {
        if (maxSymbolLength < 1 || maxSymbolLength > FsstSymbolTable.MAX_SYMBOL_LENGTH) {
            throw new IllegalArgumentException(
                "maxSymbolLength must be between 1 and " + FsstSymbolTable.MAX_SYMBOL_LENGTH + ", got " + maxSymbolLength
            );
        }
        this.maxSymbolLength = maxSymbolLength;
        return this;
    }

    /**
     * Sets the concatenated block bytes for the next {@link #build} call.
     * {@code blockData[0..totalLength)} holds all values for the block.
     *
     * <p>Returns {@code this} for chaining.
     */
    FsstSymbolTableBuilder data(byte[] blockData, int totalLength) {
        this.blockData = blockData;
        this.totalLength = totalLength;
        return this;
    }

    /**
     * Sets the per-value lengths for the next {@link #build} call.
     * {@code pendingLengths[0..count)} holds each value's byte length.
     *
     * <p>Returns {@code this} for chaining.
     */
    FsstSymbolTableBuilder lengths(int[] pendingLengths, int count) {
        this.pendingLengths = pendingLengths;
        this.count = count;
        return this;
    }

    /**
     * Counts the number of distinct byte values that appear anywhere in
     * {@code data[0..totalLength)}.
     */
    static int distinctByteCount(byte[] data, int totalLength) {
        final boolean[] seen = new boolean[BYTE_RANGE];
        for (int i = 0; i < totalLength; i++) {
            seen[data[i] & 0xFF] = true;
        }
        int count = 0;
        for (final boolean b : seen) {
            if (b) {
                count++;
            }
        }
        return count;
    }

    /**
     * Builds a symbol table from the block data set by {@link #data} and {@link #lengths}, or
     * returns {@code null} when the block's bigram distribution is too uniform to benefit from FSST
     * compression.
     *
     * <p>Counts 2-gram frequencies within each value (not across boundaries), selects the top
     * {@link FsstSymbolTable#MAX_SYMBOLS} bigrams by frequency, and greedily extends each toward
     * longer symbols when the extension saves more bytes than the current length.
     *
     * <p>Returns {@code null} when no bigram appears significantly more often than average.
     * Pseudo-random data — hex IDs, UUIDs, base64 blobs — produces a near-uniform bigram
     * distribution where every code from the small alphabet is equally likely after any other.
     * FSST builds a useless symbol table in that case, adds per-block overhead, and escapes almost
     * every byte. The caller should fall back to packed layout instead.
     */
    FsstSymbolTable build() {
        int at = 0;
        for (int v = 0; v < count; v++) {
            final int end = at + pendingLengths[v];
            for (int pos = at; pos + 1 < end; pos++) {
                bigramFreq[((blockData[pos] & 0xFF) << 8) | (blockData[pos + 1] & 0xFF)]++;
            }
            at += pendingLengths[v];
        }

        // keys[0..touched) tracks all incremented entries for cleanup; singletons must be reset
        // too or they contaminate the next block's training data.
        int touched = 0;
        int nonTrivial = 0;
        long totalBigramCount = 0;
        for (int i = 0; i < BIGRAM_TABLE_SIZE; i++) {
            final int freq = bigramFreq[i];
            if (freq > 0) {
                keys[touched++] = i;
                if (freq > 1) {
                    freqAndKey[nonTrivial] = ((long) freq << 16) | (i & 0xFFFF);
                    nonTrivial++;
                    totalBigramCount += freq;
                }
            }
        }
        Arrays.sort(freqAndKey, 0, nonTrivial);

        // Both conditions together target pseudo-random data (hex IDs, UUIDs) where no bigram
        // dominates. The 4x threshold holds because random hex data's max/avg ratio stays below 4,
        // while any structured field with nonTrivial > 255 has a dominant bigram above that ratio.
        if (nonTrivial > FsstSymbolTable.MAX_SYMBOLS) {
            final int topFreq = bigramFreq[(int) (freqAndKey[nonTrivial - 1] & 0xFFFF)];
            if ((long) topFreq * nonTrivial < 4L * totalBigramCount) {
                clearBigramFreq(touched);
                return null;
            }
        }

        // Zero candidates means escape-only encoding (2 bytes per input byte), always worse than PACKED.
        final int numCandidates = Math.min(nonTrivial, FsstSymbolTable.MAX_SYMBOLS);
        if (numCandidates == 0) {
            clearBigramFreq(touched);
            return null;
        }

        starts = ArrayUtil.growNoCopy(starts, count);
        int offset = 0;
        for (int v = 0; v < count; v++) {
            starts[v] = offset;
            offset += pendingLengths[v];
        }

        final byte[][] candidates = new byte[numCandidates][];
        for (int i = 0; i < numCandidates; i++) {
            final int key = (int) (freqAndKey[nonTrivial - 1 - i] & 0xFFFF);
            final byte b0 = (byte) (key >>> 8);
            final byte b1 = (byte) (key & 0xFF);
            candidates[i] = extend(new byte[] { b0, b1 }, bigramFreq[key]);
        }

        clearBigramFreq(touched);
        return new FsstSymbolTable(candidates);
    }

    private void clearBigramFreq(int nonZero) {
        for (int i = 0; i < nonZero; i++) {
            bigramFreq[keys[i]] = 0;
        }
    }

    /**
     * Tries to extend {@code current} by one byte at a time up to {@link #maxSymbolLength}, keeping
     * the extension when it saves more bytes in aggregate than the current length.
     */
    private byte[] extend(byte[] current, int curFreq) {
        while (current.length < maxSymbolLength) {
            Arrays.fill(extFreq, 0);
            for (int v = 0; v < count; v++) {
                final int vEnd = starts[v] + pendingLengths[v];
                outer: for (int pos = starts[v]; pos + current.length < vEnd; pos++) {
                    for (int k = 0; k < current.length; k++) {
                        if (blockData[pos + k] != current[k]) {
                            continue outer;
                        }
                    }
                    extFreq[blockData[pos + current.length] & 0xFF]++;
                }
            }
            int bestByte = -1;
            int bestFreq = 0;
            for (int b = 0; b < BYTE_RANGE; b++) {
                if (extFreq[b] > bestFreq) {
                    bestFreq = extFreq[b];
                    bestByte = b;
                }
            }
            if (bestByte >= 0 && (long) bestFreq * current.length > (long) curFreq * (current.length - 1)) {
                final byte[] extended = Arrays.copyOf(current, current.length + 1);
                extended[current.length] = (byte) bestByte;
                current = extended;
                curFreq = bestFreq;
            } else {
                break;
            }
        }
        return current;
    }
}
