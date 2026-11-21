/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

import java.util.Arrays;

/**
 * An open-addressing map from a 32-bit key hash to a non-negative {@code int} slot number, with linear probing and backward-shift
 * deletion. Each table cell packs the hash and the slot into one {@code long}, so a probe touches one array and one cache line. It
 * exists so that the adaptive eviction hot path neither boxes keys nor allocates a node per entry. Not thread-safe; guarded by the
 * cache's LRU lock.
 */
final class FingerprintIndex {
    /** Returned by {@link #get} for a missing key. */
    static final int ABSENT = -1;

    private static final long FIBONACCI = 0x9E3779B97F4A7C15L;
    private static final long HASH_MASK = 0xFFFFFFFF00000000L;
    private static final int MIN_CAPACITY = 16;

    private long[] cells;   // 0 for an empty cell, otherwise (hash << 32) | (slot + 1)
    private int mask;
    private int shift;
    private int size;

    FingerprintIndex(int expectedSize) {
        int capacity = MIN_CAPACITY;
        while (capacity < 2L * expectedSize) {
            capacity <<= 1;
        }
        allocate(capacity);
    }

    private void allocate(int capacity) {
        cells = new long[capacity];
        mask = capacity - 1;
        shift = 64 - Integer.numberOfTrailingZeros(capacity);
    }

    private int home(long hashInHighBits) {
        return (int) (((hashInHighBits >> 32) * FIBONACCI) >>> shift);
    }

    int size() {
        return size;
    }

    /** The slot stored for {@code hash}, or {@link #ABSENT}. */
    int get(int hash) {
        final long[] c = cells;
        final long tag = (long) hash << 32;
        int i = home(tag);
        long cell;
        while ((cell = c[i]) != 0) {
            if ((cell & HASH_MASK) == tag) {
                return (int) cell - 1;
            }
            i = (i + 1) & mask;
        }
        return ABSENT;
    }

    /**
     * Returns the slot stored for {@code hash}; if there is none, stores {@code newSlot} for it and returns {@link #ABSENT}. One probe
     * sequence serves both the lookup and the insertion.
     */
    int getOrPut(int hash, int newSlot) {
        assert newSlot >= 0;
        if (2 * (size + 1) > cells.length) {
            grow();
        }
        final long[] c = cells;
        final long tag = (long) hash << 32;
        int i = home(tag);
        long cell;
        while ((cell = c[i]) != 0) {
            if ((cell & HASH_MASK) == tag) {
                return (int) cell - 1;
            }
            i = (i + 1) & mask;
        }
        c[i] = tag | (newSlot + 1L);
        size++;
        return ABSENT;
    }

    /** Stores {@code slot} for {@code hash}, replacing any previous slot of that key. */
    void put(int hash, int slot) {
        assert slot >= 0;
        if (2 * (size + 1) > cells.length) {
            grow();
        }
        final long tag = (long) hash << 32;
        int i = home(tag);
        long cell;
        while ((cell = cells[i]) != 0) {
            if ((cell & HASH_MASK) == tag) {
                cells[i] = tag | (slot + 1L);
                return;
            }
            i = (i + 1) & mask;
        }
        cells[i] = tag | (slot + 1L);
        size++;
    }

    /** Removes {@code hash} if present. */
    void remove(int hash) {
        final long[] c = cells;
        final long tag = (long) hash << 32;
        int hole = home(tag);
        long cell;
        while ((cell = c[hole]) != 0) {
            if ((cell & HASH_MASK) == tag) {
                break;
            }
            hole = (hole + 1) & mask;
        }
        if (cell == 0) {
            return;
        }
        // backward-shift deletion: pull later members of the probe chain into the hole unless they are already at or before their home
        int j = hole;
        while (true) {
            j = (j + 1) & mask;
            final long moved = c[j];
            if (moved == 0) {
                break;
            }
            final int home = home(moved);
            final boolean homeBetweenHoleAndJ = hole <= j ? (hole < home && home <= j) : (hole < home || home <= j);
            if (homeBetweenHoleAndJ) {
                continue;
            }
            c[hole] = moved;
            hole = j;
        }
        c[hole] = 0;
        size--;
    }

    void clear() {
        Arrays.fill(cells, 0);
        size = 0;
    }

    private void grow() {
        final long[] old = cells;
        allocate(old.length << 1);
        for (long cell : old) {
            if (cell != 0) {
                int j = home(cell);
                while (cells[j] != 0) {
                    j = (j + 1) & mask;
                }
                cells[j] = cell;
            }
        }
    }
}
