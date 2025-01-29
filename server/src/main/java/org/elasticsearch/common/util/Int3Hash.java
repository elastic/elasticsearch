/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import com.carrotsearch.hppc.BitMixer;

import org.elasticsearch.core.Releasables;

/**
 * Specialized hash table implementation similar to BytesRefHash that maps
 * three int values to ids. Collisions are resolved with open addressing and
 * linear probing, growth is smooth thanks to {@link BigArrays} and capacity
 * is always a multiple of 3 for faster identification of buckets.
 * This class is not thread-safe.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
public final class Int3Hash extends AbstractHash {
    private IntArray keys;

    // Constructor with configurable capacity and default maximum load factor.
    public Int3Hash(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    // Constructor with configurable capacity and load factor.
    public Int3Hash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        try {
            // `super` allocates a big array so we have to `close` if we fail here or we'll leak it.
            keys = bigArrays.newIntArray(3 * capacity, false);
        } finally {
            if (keys == null) {
                close();
            }
        }
    }

    public int getKey1(long id) {
        return keys.get(3 * id);
    }

    public int getKey2(long id) {
        return keys.get(3 * id + 1);
    }

    public int getKey3(long id) {
        return keys.get(3 * id + 2);
    }

    /**
     * Get the id associated with <code>key</code> or -1 if the key is not contained in the hash.
     */
    public long find(int key1, int key2, int key3) {
        final long slot = slot(hash(key1, key2, key3), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long id = id(index);
            if (id == -1) {
                return id;
            } else {
                long keyOffset = 3 * id;
                if ((keys.get(keyOffset) == key1 && keys.get(keyOffset + 1) == key2 && keys.get(keyOffset + 2) == key3)) {
                    return id;
                }
            }
        }
    }

    private long set(int key1, int key2, int key3, long id) {
        assert size < maxSize;
        long slot = slot(hash(key1, key2, key3), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                setId(index, id);
                append(id, key1, key2, key3);
                ++size;
                return id;
            } else {
                final long keyOffset = 3 * curId;
                if (keys.get(keyOffset) == key1 && keys.get(keyOffset + 1) == key2 && keys.get(keyOffset + 2) == key3) {
                    return -1 - curId;
                }
            }
        }
    }

    private void append(long id, int key1, int key2, int key3) {
        final long keyOffset = 3 * id;
        keys = bigArrays.grow(keys, keyOffset + 3);
        keys.set(keyOffset, key1);
        keys.set(keyOffset + 1, key2);
        keys.set(keyOffset + 2, key3);
    }

    private void reset(long id) {
        final IntArray keys = this.keys;
        final long keyOffset = id * 3;
        final int key1 = keys.get(keyOffset);
        final int key2 = keys.get(keyOffset + 1);
        final int key3 = keys.get(keyOffset + 2);
        final long slot = slot(hash(key1, key2, key3), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                setId(index, id);
                break;
            }
        }
    }

    /**
     * Try to add {@code key}. Return its newly allocated id if it wasn't in
     * the hash table yet, or {@code -1-id} if it was already present in
     * the hash table.
     */
    public long add(int key1, int key2, int key3) {
        if (size >= maxSize) {
            assert size == maxSize;
            grow();
        }
        assert size < maxSize;
        return set(key1, key2, key3, size);
    }

    @Override
    protected void removeAndAdd(long index) {
        final long id = getAndSetId(index, -1);
        assert id >= 0;
        reset(id);
    }

    @Override
    public void close() {
        Releasables.close(keys, super::close);
    }

    static long hash(long key1, long key2, long key3) {
        return 31L * (31L * BitMixer.mix(key1) + BitMixer.mix(key2)) + BitMixer.mix(key3);
    }
}
