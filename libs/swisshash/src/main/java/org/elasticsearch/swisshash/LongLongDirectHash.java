/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.common.util.PartitionedHashTable;

import java.util.Arrays;

/**
 * A direct-mapped table for pairs of {@code long} keys. See {@link LongDirectHash} for the semantics: no collision
 * resolution, a pair that finds a different pair in its slot gets a fresh sequential id, and one pair can have several ids.
 */
public final class LongLongDirectHash implements LongLongHashTable, PartitionedHashTable {
    private static final long EMPTY = -1L;
    private static final int INITIAL_KEYS_CAPACITY = 1024;
    private static final String BREAKER_LABEL = "LongLongDirectHash";

    private final CircuitBreaker breaker;
    private final long[] slots;
    private final int mask;
    private long[] keys1;
    private long[] keys2;
    private int size;
    private boolean closed;

    /**
     * @param slots the number of slots, a power of two
     */
    public LongLongDirectHash(CircuitBreaker breaker, int slots) {
        if (slots <= 0 || Integer.bitCount(slots) != 1) {
            throw new IllegalArgumentException("slots must be a positive power of two; got " + slots);
        }
        this.breaker = breaker;
        this.mask = slots - 1;
        breaker.addEstimateBytesAndMaybeBreak(bytesFor(slots) + 2 * bytesFor(INITIAL_KEYS_CAPACITY), BREAKER_LABEL);
        this.slots = new long[slots];
        this.keys1 = new long[INITIAL_KEYS_CAPACITY];
        this.keys2 = new long[INITIAL_KEYS_CAPACITY];
        Arrays.fill(this.slots, EMPTY);
    }

    private static long bytesFor(int longs) {
        return (long) longs * Long.BYTES;
    }

    @Override
    public long add(long key1, long key2) {
        final long hash64 = LongLongSwissHash.hash(key1, key2);
        final int fingerprint = (int) (hash64 >>> 32);
        final int slot = (int) hash64 & mask;
        final long existing = slots[slot];
        if ((int) (existing >>> 32) == fingerprint && existing != EMPTY) {
            final int id = (int) existing;
            if (keys1[id] == key1 && keys2[id] == key2) {
                return -1L - id;
            }
        }
        final int id = size;
        if (id == keys1.length) {
            growKeys();
        }
        keys1[id] = key1;
        keys2[id] = key2;
        size = id + 1;
        slots[slot] = ((long) fingerprint << 32) | id;
        return id;
    }

    private void growKeys() {
        final int newLength = ArrayUtil.oversize(size + 1, Long.BYTES);
        breaker.addEstimateBytesAndMaybeBreak(2 * bytesFor(newLength - keys1.length), BREAKER_LABEL);
        keys1 = Arrays.copyOf(keys1, newLength);
        keys2 = Arrays.copyOf(keys2, newLength);
    }

    /**
     * Returns the id of the pair if it still holds its slot, or {@code -1}. A miss does not mean the pair was never added.
     */
    @Override
    public long find(long key1, long key2) {
        final long hash64 = LongLongSwissHash.hash(key1, key2);
        final long existing = slots[(int) hash64 & mask];
        if ((int) (existing >>> 32) == (int) (hash64 >>> 32) && existing != EMPTY) {
            final int id = (int) existing;
            if (keys1[id] == key1 && keys2[id] == key2) {
                return id;
            }
        }
        return -1;
    }

    @Override
    public long getKey1(long id) {
        assert id >= 0 && id < size : id + " is not in [0," + size + ")";
        return keys1[(int) id];
    }

    @Override
    public long getKey2(long id) {
        assert id >= 0 && id < size : id + " is not in [0," + size + ")";
        return keys2[(int) id];
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public void clear() {
        Arrays.fill(slots, EMPTY);
        size = 0;
    }

    @Override
    public PartitionedHashKeys splitPartition(CircuitBreaker breaker, PartitionSplitter partitionSplitter) {
        return LongLongSwissHash.splitPartition(breaker, keys1, keys2, size, partitionSplitter);
    }

    /**
     * Not supported. This table only produces partial results. Combining happens in a regular table.
     */
    @Override
    public boolean combinePartition(PartitionedHashKeys keys, int partitionIndex, int[] resultIds) {
        throw new UnsupportedOperationException("LongLongDirectHash can't combine partitions");
    }

    @Override
    public long ramBytesUsed() {
        return bytesFor(slots.length) + 2 * bytesFor(keys1.length);
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            breaker.addWithoutBreaking(-ramBytesUsed());
        }
    }
}
