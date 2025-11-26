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
 * Specialized hash table implementation maps N int values to ids.
 * Collisions are resolved with open addressing and
 * linear probing, growth is smooth thanks to {@link BigArrays} and capacity
 * is always a multiple of N for faster identification of buckets.
 * This class is not thread-safe.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
public final class IntNHash extends AbstractHash {
    private IntArray keyArray;
    private final int keySize;
    private final int[] scratch;

    // Constructor with configurable capacity and default maximum load factor.
    public IntNHash(long capacity, int keySize, BigArrays bigArrays) {
        this(capacity, keySize, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    // Constructor with configurable capacity and load factor.
    public IntNHash(long capacity, int keySize, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        this.keySize = keySize;
        this.scratch = new int[keySize];
        try {
            // `super` allocates a big array so we have to `close` if we fail here or we'll leak it.
            keyArray = bigArrays.newIntArray(keySize * capacity, false);
        } finally {
            if (keyArray == null) {
                close();
            }
        }
    }

    public int[] getKeys(long id) {
        getKeys(id, scratch);
        return scratch;
    }

    public void getKeys(long id, int[] dst) {
        assert dst.length == keySize;
        for (int i = 0; i < keySize; i++) {
            dst[i] = keyArray.get(keySize * id + i);
        }
    }

    private boolean keyEquals(long id, int[] keys) {
        long keyOffset = keySize * id;
        // TODO: fast equals in BigArray
        for (int i = 0; i < keys.length; i++) {
            if (keyArray.get(keyOffset + i) != keys[i]) {
                return false;
            }
        }
        return true;
    }

    public long find(int[] keys) {
        final long slot = slot(hash(keys), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long id = id(index);
            if (id == -1) {
                return id;
            } else if (keyEquals(id, keys)) {
                return id;
            }
        }
    }

    private long set(long id, int[] keys) {
        assert size < maxSize;
        long slot = slot(hash(keys), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                setId(index, id);
                append(id, keys);
                ++size;
                return id;
            } else {
                if (keyEquals(curId, keys)) {
                    return -1 - curId;
                }
            }
        }
    }

    private void append(long id, int[] keys) {
        final long keyOffset = keySize * id;
        keyArray = bigArrays.grow(keyArray, keyOffset + keySize);
        for (int i = 0; i < keys.length; i++) {
            keyArray.set(keyOffset + i, keys[i]);
        }
    }

    private void reset(long id) {
        final long slot = slot(hashFromKeyArray(id), mask);
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
    public long add(int[] keys) {
        if (size >= maxSize) {
            assert size == maxSize;
            grow();
        }
        assert size < maxSize;
        return set(size, keys);
    }

    @Override
    protected void removeAndAdd(long index) {
        final long id = getAndSetId(index, -1);
        assert id >= 0;
        reset(id);
    }

    @Override
    public void close() {
        Releasables.close(keyArray, super::close);
    }

    static long hash(int[] keys) {
        long hash = BitMixer.mix(keys[0]);
        for (int i = 1; i < keys.length; i++) {
            hash = 31L * hash + BitMixer.mix(keys[i]);
        }
        return hash;
    }

    long hashFromKeyArray(long id) {
        final long keyOffset = id * keySize;
        long hash = BitMixer.mix(keyArray.get(keyOffset));
        for (int i = 1; i < keySize; i++) {
            hash = 31L * hash + BitMixer.mix(keyArray.get(keyOffset + i));
        }
        return hash;
    }
}
