/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Reusable fixed-width bit mask backed by a {@code long[]} word array. Unlike {@link BitSet},
 * this class supports explicit reset-and-reuse semantics with zero allocation after warmup,
 * making it suitable for per-batch null tracking in tight decode loops.
 */
final class WordMask {
    private long[] words;
    private int numBits;

    WordMask() {}

    void reset(int numBits) {
        this.numBits = numBits;
        int numWords = (numBits + 63) >>> 6;
        if (words == null || words.length < numWords) {
            words = new long[numWords];
        } else {
            Arrays.fill(words, 0, numWords, 0L);
        }
    }

    void set(int index) {
        words[index >>> 6] |= 1L << index;
    }

    boolean get(int index) {
        return (words[index >>> 6] & (1L << index)) != 0;
    }

    boolean isEmpty() {
        for (int i = 0; i < wordCount(); i++) {
            if (words[i] != 0) {
                return false;
            }
        }
        return true;
    }

    int wordCount() {
        return (numBits + 63) >>> 6;
    }

    void clear(int index) {
        words[index >>> 6] &= ~(1L << index);
    }

    /**
     * Resets the mask to the given size and sets all {@code numBits} bits to 1.
     * Trailing bits in the last word beyond {@code numBits} are left as 0.
     */
    void setAll(int numBits) {
        reset(numBits);
        int numWords = wordCount();
        if (numWords == 0) {
            return;
        }
        // fill all full words with all-ones
        Arrays.fill(words, 0, numWords, ~0L);
        // mask off trailing bits in the last word
        int trailing = numBits & 63;
        if (trailing != 0) {
            words[numWords - 1] = (1L << trailing) - 1;
        }
    }

    /**
     * Returns the number of set bits in this mask.
     */
    int popCount() {
        int count = 0;
        int numWords = wordCount();
        if (numWords == 0) {
            return 0;
        }
        for (int i = 0; i < numWords - 1; i++) {
            count += Long.bitCount(words[i]);
        }
        // mask the last word to only count bits within numBits
        int trailing = numBits & 63;
        long lastWord = words[numWords - 1];
        if (trailing != 0) {
            lastWord &= (1L << trailing) - 1;
        }
        count += Long.bitCount(lastWord);
        return count;
    }

    /**
     * Returns {@code true} if all {@code numBits} bits are set.
     */
    boolean isAll() {
        int numWords = wordCount();
        if (numWords == 0) {
            return true;
        }
        // check all full words are all-ones
        for (int i = 0; i < numWords - 1; i++) {
            if (words[i] != ~0L) {
                return false;
            }
        }
        // check the last word has the correct trailing bits
        int trailing = numBits & 63;
        long expectedLast = trailing == 0 ? ~0L : (1L << trailing) - 1;
        return words[numWords - 1] == expectedLast;
    }

    /**
     * Bitwise AND with another mask of the same size, mutating this mask.
     */
    void and(WordMask other) {
        if (this.numBits != other.numBits) {
            throw new IllegalArgumentException("numBits mismatch: " + this.numBits + " vs " + other.numBits);
        }
        int numWords = wordCount();
        for (int i = 0; i < numWords; i++) {
            words[i] &= other.words[i];
        }
        maskTrailingBits();
    }

    void or(WordMask other) {
        if (this.numBits != other.numBits) {
            throw new IllegalArgumentException("numBits mismatch: " + this.numBits + " vs " + other.numBits);
        }
        int numWords = wordCount();
        for (int i = 0; i < numWords; i++) {
            words[i] |= other.words[i];
        }
        maskTrailingBits();
    }

    /**
     * Flips all bits in this mask. Trailing bits beyond {@code numBits} are kept as 0.
     */
    void negate() {
        int numWords = wordCount();
        for (int i = 0; i < numWords; i++) {
            words[i] = ~words[i];
        }
        maskTrailingBits();
    }

    private void maskTrailingBits() {
        int numWords = wordCount();
        int trailing = numBits & 63;
        if (trailing != 0 && numWords > 0) {
            words[numWords - 1] &= (1L << trailing) - 1;
        }
    }

    /**
     * Returns an array of the indices of all set bits, in ascending order.
     * Uses {@link Long#numberOfTrailingZeros} to efficiently iterate set bits.
     */
    int[] survivingPositions() {
        int count = popCount();
        int[] positions = new int[count];
        int pos = 0;
        int numWords = wordCount();
        for (int w = 0; w < numWords; w++) {
            long word = words[w];
            // mask trailing bits on the last word
            int trailing = numBits & 63;
            if (w == numWords - 1 && trailing != 0) {
                word &= (1L << trailing) - 1;
            }
            int base = w << 6;
            while (word != 0) {
                int bit = Long.numberOfTrailingZeros(word);
                positions[pos++] = base + bit;
                word &= word - 1; // clear lowest set bit
            }
        }
        return positions;
    }

    BitSet toBitSet() {
        return BitSet.valueOf(Arrays.copyOf(words, wordCount()));
    }
}
