/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.PartitionedHashTable;

import java.util.Arrays;

/**
 * A direct-mapped table for {@link BytesRef} keys. See {@link LongDirectHash} for the semantics: no collision resolution,
 * a key that finds a different key in its slot gets a fresh sequential id, and one key can have several ids.
 */
public final class BytesRefDirectHash implements BytesRefHashTable, PartitionedHashTable {
    private static final long EMPTY = -1L;
    private static final String BREAKER_LABEL = "BytesRefDirectHash";

    private final CircuitBreaker breaker;
    private final BigArrays bigArrays;
    private final long[] slots;
    private final int mask;
    private final BytesRefArray keys;
    private final PagedBytesCursor cursorScratch = new PagedBytesCursor();
    private int size;
    private boolean closed;

    /**
     * @param slots the number of slots, a power of two
     */
    public BytesRefDirectHash(CircuitBreaker breaker, BigArrays bigArrays, int slots) {
        if (slots <= 0 || Integer.bitCount(slots) != 1) {
            throw new IllegalArgumentException("slots must be a positive power of two; got " + slots);
        }
        this.breaker = breaker;
        this.bigArrays = bigArrays;
        this.mask = slots - 1;
        breaker.addEstimateBytesAndMaybeBreak((long) slots * Long.BYTES, BREAKER_LABEL);
        boolean success = false;
        try {
            this.slots = new long[slots];
            this.keys = new BytesRefArray(Math.min(slots, 1024), bigArrays);
            Arrays.fill(this.slots, EMPTY);
            success = true;
        } finally {
            if (success == false) {
                breaker.addWithoutBreaking(-(long) slots * Long.BYTES);
            }
        }
    }

    @Override
    public long add(BytesRef key) {
        final long hash64 = BytesRefSwissHash.hash64(key);
        final int fingerprint = (int) (hash64 >>> 32);
        final int slot = (int) hash64 & mask;
        final long existing = slots[slot];
        if ((int) (existing >>> 32) == fingerprint && existing != EMPTY) {
            final int id = (int) existing;
            if (keys.bytesEqual(id, key)) {
                return -1L - id;
            }
        }
        final int id = size;
        keys.append(key);
        size = id + 1;
        slots[slot] = ((long) fingerprint << 32) | id;
        return id;
    }

    @Override
    public long add(PagedBytesCursor key) {
        final long hash64 = BytesRefSwissHash.hash64(key);
        final int fingerprint = (int) (hash64 >>> 32);
        final int slot = (int) hash64 & mask;
        final long existing = slots[slot];
        if ((int) (existing >>> 32) == fingerprint && existing != EMPTY) {
            final int id = (int) existing;
            if (key.equals(keys.get(id, cursorScratch))) {
                return -1L - id;
            }
        }
        final int id = size;
        keys.append(key);
        size = id + 1;
        slots[slot] = ((long) fingerprint << 32) | id;
        return id;
    }

    /**
     * Returns the id of {@code key} if it still holds its slot, or {@code -1}. A miss does not mean the key was never added.
     */
    @Override
    public long find(BytesRef key) {
        final long hash64 = BytesRefSwissHash.hash64(key);
        final long existing = slots[(int) hash64 & mask];
        if ((int) (existing >>> 32) == (int) (hash64 >>> 32) && existing != EMPTY) {
            final int id = (int) existing;
            if (keys.bytesEqual(id, key)) {
                return id;
            }
        }
        return -1;
    }

    @Override
    public BytesRef get(long id, BytesRef dest) {
        return keys.get(id, dest);
    }

    @Override
    public BytesRefArray getBytesRefs() {
        return keys;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public void clear() {
        Arrays.fill(slots, EMPTY);
        keys.truncateTo(0);
        size = 0;
    }

    @Override
    public PartitionedHashKeys splitPartition(CircuitBreaker breaker, PartitionSplitter partitionSplitter) {
        return BytesRefSwissHash.splitPartition(breaker, bigArrays, keys, partitionSplitter);
    }

    /**
     * Not supported. This table only produces partial results. Combining happens in a regular table.
     */
    @Override
    public boolean combinePartition(PartitionedHashKeys keys, int partitionIndex, int[] resultIds) {
        throw new UnsupportedOperationException("BytesRefDirectHash can't combine partitions");
    }

    @Override
    public long ramBytesUsed() {
        return (long) slots.length * Long.BYTES + keys.ramBytesUsed();
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            keys.close();
            breaker.addWithoutBreaking(-(long) slots.length * Long.BYTES);
        }
    }
}
