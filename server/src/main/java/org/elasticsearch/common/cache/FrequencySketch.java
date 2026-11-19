/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

/**
 * A small count-min sketch with periodic halving (as used by TinyLFU) that estimates how often a key fingerprint was seen
 * recently in bounded memory. The four counters of a key sit in one 16-counter block (one per row, each row picking one of four
 * cells), so counting and estimating a key touches a single 64-byte neighbourhood and costs one 64-bit mix. A counter is shared
 * with another key with probability 1/width, as in the classic layout. Counters need no saturation check: the sketch halves after
 * {@code sampleSize} increments (10 * width by default), so a counter cannot overflow while that is below 2^31. Not thread-safe;
 * guarded by the cache's LRU lock.
 */
final class FrequencySketch {
    private final int[] table;
    private final int blockMask;
    private final long sampleSize;
    private long additions;
    private boolean halved;

    /** @param width counters per row; a power of two, at least 4 */
    FrequencySketch(int width) {
        this(width, 10L * width);
    }

    /**
     * @param width      counters per row; a power of two, at least 4
     * @param sampleSize the number of counted accesses after which all counters are halved, i.e. how far back the sketch remembers
     */
    FrequencySketch(int width, long sampleSize) {
        this.blockMask = width / 4 - 1;
        this.table = new int[4 * width];
        this.sampleSize = sampleSize;
    }

    private static long mix(long fingerprint) {
        long h = fingerprint * 0x9E3779B97F4A7C15L;
        h ^= h >>> 32;
        h *= 0xBF58476D1CE4E5B9L;
        h ^= h >>> 29;
        return h;
    }

    int estimate(long fingerprint) {
        final long h = mix(fingerprint);
        final int base = ((int) h & blockMask) << 4;
        final int[] t = table;
        final int a = t[base + (int) (h >>> 32 & 3)];
        final int b = t[base + 4 + (int) (h >>> 34 & 3)];
        final int c = t[base + 8 + (int) (h >>> 36 & 3)];
        final int d = t[base + 12 + (int) (h >>> 38 & 3)];
        return Math.min(Math.min(a, b), Math.min(c, d));
    }

    /**
     * Counts one occurrence and returns the new estimate. {@link #wasHalved()} tells whether this call also halved the sketch (in which
     * case the returned estimate is already the halved one).
     */
    int incrementAndEstimate(long fingerprint) {
        final long h = mix(fingerprint);
        final int base = ((int) h & blockMask) << 4;
        final int[] t = table;
        final int i0 = base + (int) (h >>> 32 & 3);
        final int i1 = base + 4 + (int) (h >>> 34 & 3);
        final int i2 = base + 8 + (int) (h >>> 36 & 3);
        final int i3 = base + 12 + (int) (h >>> 38 & 3);
        t[i0]++;
        t[i1]++;
        t[i2]++;
        t[i3]++;
        halved = false;
        if (++additions >= sampleSize) {
            for (int i = 0; i < t.length; i++) {
                t[i] >>>= 1;
            }
            additions /= 2;
            halved = true;
        }
        return Math.min(Math.min(t[i0], t[i1]), Math.min(t[i2], t[i3]));
    }

    boolean wasHalved() {
        return halved;
    }

    /** Counts one occurrence; returns true if the sketch was halved as a result. */
    boolean increment(long fingerprint) {
        incrementAndEstimate(fingerprint);
        return halved;
    }
}
