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

    BitSet toBitSet() {
        return BitSet.valueOf(Arrays.copyOf(words, wordCount()));
    }
}
