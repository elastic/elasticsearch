/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.sketch;

/**
 * An immutable set of {@code long} values with open addressing, built for fast {@link #contains} probes on a small set.
 * It is small enough to stay in the L1 cache for the candidate sets that the sketch proof produces. Zero is handled apart,
 * because it marks a free table cell.
 */
final class LongSet {
    private static final long MIX = 0xC2B2AE3D27D4EB4FL;

    private final long[] cells;
    private final int shift;
    private final boolean hasZero;
    private final int size;

    LongSet(long[] values) {
        int capacity = Math.max(8, Integer.highestOneBit(Math.max(1, values.length * 4 - 1)) << 1);
        this.cells = new long[capacity];
        this.shift = 64 - Integer.numberOfTrailingZeros(capacity);
        boolean zero = false;
        int count = 0;
        for (long v : values) {
            if (v == 0) {
                if (zero == false) {
                    zero = true;
                    count++;
                }
            } else if (insert(v)) {
                count++;
            }
        }
        this.hasZero = zero;
        this.size = count;
    }

    int size() {
        return size;
    }

    boolean contains(long v) {
        if (v == 0) {
            return hasZero;
        }
        final long[] c = cells;
        final int mask = c.length - 1;
        int i = (int) ((v * MIX) >>> shift);
        while (true) {
            long x = c[i];
            if (x == v) {
                return true;
            }
            if (x == 0) {
                return false;
            }
            i = (i + 1) & mask;
        }
    }

    private boolean insert(long v) {
        final int mask = cells.length - 1;
        int i = (int) ((v * MIX) >>> shift);
        while (true) {
            long x = cells[i];
            if (x == v) {
                return false;
            }
            if (x == 0) {
                cells[i] = v;
                return true;
            }
            i = (i + 1) & mask;
        }
    }
}
