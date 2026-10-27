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
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.common.util.PartitionedHashTable;

import java.util.Arrays;

/**
 * A direct-mapped table for {@code long} keys, for partial aggregations whose keys rarely repeat. Unlike a regular hash
 * table it has no collision resolution, so it stays small and cache-resident however many distinct keys it sees.
 * <p>
 * Every slot holds {@code (hash32 << 32) | id}, or {@link #EMPTY}. {@link #add} reads the slot of the key. If the slot holds
 * the same key, that id is returned. Otherwise the key gets the next sequential id and takes over the slot. The previous
 * occupant keeps its id and its key, so one key can have several ids. This is only correct if the consumer of the ids merges
 * rows with equal keys later, as the final stage of an aggregation does.
 * <p>
 * Keys are stored densely by id, so {@link #size()} counts ids, including those of evicted keys, and {@link #get} works for
 * every id. {@link #find} only finds keys that still hold their slot.
 */
public final class LongDirectHash implements LongHashTable, PartitionedHashTable {
    private static final long EMPTY = -1L;
    private static final int INITIAL_KEYS_CAPACITY = 1024;
    private static final String BREAKER_LABEL = "LongDirectHash";

    private final CircuitBreaker breaker;
    private final long[] slots;
    private final int mask;
    private long[] keys;
    private int size;
    private boolean closed;

    /**
     * @param slots the number of slots, a power of two
     */
    public LongDirectHash(CircuitBreaker breaker, int slots) {
        if (slots <= 0 || Integer.bitCount(slots) != 1) {
            throw new IllegalArgumentException("slots must be a positive power of two; got " + slots);
        }
        this.breaker = breaker;
        this.mask = slots - 1;
        breaker.addEstimateBytesAndMaybeBreak(bytesFor(slots) + bytesFor(INITIAL_KEYS_CAPACITY), BREAKER_LABEL);
        this.slots = new long[slots];
        this.keys = new long[INITIAL_KEYS_CAPACITY];
        Arrays.fill(this.slots, EMPTY);
    }

    private static long bytesFor(int longs) {
        return (long) longs * Long.BYTES;
    }

    @Override
    public long add(long key) {
        final int hash32 = LongSwissHash.hash(key);
        final int slot = hash32 & mask;
        final long existing = slots[slot];
        if ((int) (existing >>> 32) == hash32 && existing != EMPTY) {
            final int id = (int) existing;
            if (keys[id] == key) {
                return -1L - id;
            }
        }
        final int id = size;
        if (id == keys.length) {
            growKeys();
        }
        keys[id] = key;
        size = id + 1;
        slots[slot] = ((long) hash32 << 32) | id;
        return id;
    }

    private void growKeys() {
        final int newLength = ArrayUtil.oversize(size + 1, Long.BYTES);
        breaker.addEstimateBytesAndMaybeBreak(bytesFor(newLength - keys.length), BREAKER_LABEL);
        keys = Arrays.copyOf(keys, newLength);
    }

    /**
     * Returns the id of {@code key} if it still holds its slot, or {@code -1}. A miss does not mean the key was never added.
     */
    @Override
    public long find(long key) {
        final int hash32 = LongSwissHash.hash(key);
        final long existing = slots[hash32 & mask];
        if ((int) (existing >>> 32) == hash32 && existing != EMPTY) {
            final int id = (int) existing;
            if (keys[id] == key) {
                return id;
            }
        }
        return -1;
    }

    @Override
    public long get(long id) {
        assert id >= 0 && id < size : id + " is not in [0," + size + ")";
        return keys[(int) id];
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
        return LongSwissHash.splitPartition(breaker, keys, size, partitionSplitter);
    }

    /**
     * Not supported. This table only produces partial results. Combining happens in a regular table.
     */
    @Override
    public boolean combinePartition(PartitionedHashKeys keys, int partitionIndex, int[] resultIds) {
        throw new UnsupportedOperationException("LongDirectHash can't combine partitions");
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            breaker.addWithoutBreaking(-(bytesFor(slots.length) + bytesFor(keys.length)));
        }
    }
}
