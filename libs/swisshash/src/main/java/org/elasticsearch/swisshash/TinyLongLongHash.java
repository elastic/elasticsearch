/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasable;

import java.util.Arrays;

/**
 * A lightweight (long, long) -> int id hash table. Designed for use as a sub-table in a two-level
 * hash, where each sub-table is expected to fit in L2 cache. Uses simple linear-probe open
 * addressing with inline (k1, k2) storage. No control bytes, no SIMD probing, no prefetching --
 * the assumption is the table is cache-resident so those tricks are pure overhead.
 *
 * <p>Layout:
 * <ul>
 *   <li>{@code keys}: paired {@code [k1, k2, k1, k2, ...]} at slot indices {@code 2i, 2i+1}</li>
 *   <li>{@code ids}: id assigned for each slot; {@code -1} means empty</li>
 * </ul>
 *
 * <p>IDs are dense {@code 0, 1, ..., size()-1} in insertion order.
 */
public final class TinyLongLongHash implements Releasable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TinyLongLongHash.class);
    private static final float LOAD_FACTOR = 0.5f;
    private static final int MIN_CAPACITY = 16;

    private int capacity;
    private int mask;
    private int size;
    private int growThreshold;
    private long[] keys; // packed pairs
    private int[] ids;   // -1 = empty
    private int[] slotByOrderedId; // [id] = slot where this id is stored; updated on grow

    public TinyLongLongHash() {
        this(MIN_CAPACITY);
    }

    public TinyLongLongHash(int initialCapacity) {
        int cap = MIN_CAPACITY;
        while (cap < initialCapacity) {
            cap <<= 1;
        }
        allocate(cap);
    }

    private void allocate(int cap) {
        capacity = cap;
        mask = cap - 1;
        growThreshold = (int) (cap * LOAD_FACTOR);
        keys = new long[cap * 2];
        ids = new int[cap];
        Arrays.fill(ids, -1);
        // slotByOrderedId starts small and grows with size; capacity is its max possible size.
        if (slotByOrderedId == null) {
            slotByOrderedId = new int[Math.min(cap, 64)];
        }
    }

    private static int hash(long k1, long k2) {
        // Match the hash used elsewhere in the two-level wrapper so partition selection
        // is consistent. One mul, one rotate, one xor: cheap enough that the table doesn't
        // need to cache stored hashes.
        long h = (k1 ^ Long.rotateLeft(k2, 32)) * 0x9E3779B97F4A7C15L;
        return (int) (h ^ (h >>> 32));
    }

    /**
     * Adds the (k1, k2) pair. Returns the assigned (non-negative) id if newly inserted,
     * or {@code -1 - existingId} if the pair was already present.
     */
    public long add(long k1, long k2) {
        if (size >= growThreshold) {
            grow();
        }
        int slot = hash(k1, k2) & mask;
        while (true) {
            int id = ids[slot];
            if (id < 0) {
                int newId = size++;
                int p = slot << 1;
                keys[p] = k1;
                keys[p + 1] = k2;
                ids[slot] = newId;
                if (newId >= slotByOrderedId.length) {
                    slotByOrderedId = Arrays.copyOf(slotByOrderedId, Math.max(newId + 1, slotByOrderedId.length * 2));
                }
                slotByOrderedId[newId] = slot;
                return newId;
            }
            int p = slot << 1;
            if (keys[p] == k1 && keys[p + 1] == k2) {
                return -1L - id;
            }
            slot = (slot + 1) & mask;
        }
    }

    /** Returns id if present, else -1. */
    public long find(long k1, long k2) {
        int slot = hash(k1, k2) & mask;
        while (true) {
            int id = ids[slot];
            if (id < 0) {
                return -1;
            }
            int p = slot << 1;
            if (keys[p] == k1 && keys[p + 1] == k2) {
                return id;
            }
            slot = (slot + 1) & mask;
        }
    }

    public int size() {
        return size;
    }

    public long key1AtId(int id) {
        return keys[slotByOrderedId[id] << 1];
    }

    public long key2AtId(int id) {
        return keys[(slotByOrderedId[id] << 1) + 1];
    }

    private void grow() {
        int oldCap = capacity;
        long[] oldKeys = keys;
        int[] oldIds = ids;
        int oldSize = size;
        // Iterate by id-order to keep slotByOrderedId in sync.
        int[] oldSlotByOrderedId = slotByOrderedId;
        allocate(oldCap << 1);
        // Reuse the existing slotByOrderedId capacity if it was bigger than the default.
        if (oldSlotByOrderedId.length > slotByOrderedId.length) {
            slotByOrderedId = oldSlotByOrderedId; // keep same length, contents will be rewritten
        }
        size = oldSize;
        for (int id = 0; id < oldSize; id++) {
            int oldSlot = oldSlotByOrderedId[id];
            int p = oldSlot << 1;
            long k1 = oldKeys[p];
            long k2 = oldKeys[p + 1];
            int newSlot = hash(k1, k2) & mask;
            while (ids[newSlot] >= 0) {
                newSlot = (newSlot + 1) & mask;
            }
            int np = newSlot << 1;
            keys[np] = k1;
            keys[np + 1] = k2;
            ids[newSlot] = id;
            slotByOrderedId[id] = newSlot;
        }
    }

    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED
            + RamUsageEstimator.sizeOf(keys)
            + RamUsageEstimator.sizeOf(ids)
            + RamUsageEstimator.sizeOf(slotByOrderedId);
    }

    @Override
    public void close() {
        keys = null;
        ids = null;
        slotByOrderedId = null;
    }
}
