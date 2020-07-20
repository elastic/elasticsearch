/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import com.carrotsearch.hppc.BitMixer;

import org.elasticsearch.common.lease.Releasables;

/**
 * Specialized hash table implementation similar to BytesRefHash that maps
 * two long values to ids. Collisions are resolved with open addressing and
 * linear probing, growth is smooth thanks to {@link BigArrays} and capacity
 * is always a multiple of 2 for faster identification of buckets.
 * This class is not thread-safe.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
public final class LongLongHash extends AbstractHash {
    /**
     * The keys of the hash, stored one after another. So the keys for an id
     * are stored in {@code 2 * id} and {@code 2 * id + 1}. This arrangement
     * makes {@link #add(long, long)} about 17% faster which seems worth it
     * because it is in the critical path for aggregations.
     */
    private LongArray keys;

    // Constructor with configurable capacity and default maximum load factor.
    public LongLongHash(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    //Constructor with configurable capacity and load factor.
    public LongLongHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        keys = bigArrays.newLongArray(2 * capacity, false);
    }

    /**
     * Return the first key at {@code 0 &lt;= index &lt;= capacity()}. The
     * result is undefined if the slot is unused.
     */
    public long getKey1(long id) {
        return keys.get(2 * id);
    }

    /**
     * Return the second key at {@code 0 &lt;= index &lt;= capacity()}. The
     * result is undefined if the slot is unused.
     */
    public long getKey2(long id) {
        return keys.get(2 * id + 1);
    }

    /**
     * Get the id associated with <code>key</code> or -1 if the key is not contained in the hash.
     */
    public long find(long key1, long key2) {
        final long slot = slot(hash(key1, key2), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long id = id(index);
            long keyOffset = 2 * id;
            if (id == -1 || (keys.get(keyOffset) == key1 && keys.get(keyOffset + 1) == key2)) {
                return id;
            }
        }
    }

    private long set(long key1, long key2, long id) {
        assert size < maxSize;
        final long slot = slot(hash(key1, key2), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                id(index, id);
                append(id, key1, key2);
                ++size;
                return id;
            } else {
                long keyOffset = 2 * curId;
                if (keys.get(keyOffset) == key1 && keys.get(keyOffset + 1) == key2) {
                    return -1 - curId;
                }
            }
        }
    }

    private void append(long id, long key1, long key2) {
        long keyOffset = 2 * id;
        keys = bigArrays.grow(keys, keyOffset + 2);
        keys.set(keyOffset, key1);
        keys.set(keyOffset + 1, key2);
    }

    private void reset(long key1, long key2, long id) {
        final long slot = slot(hash(key1, key2), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                id(index, id);
                append(id, key1, key2);
                break;
            }
        }
    }

    /**
     * Try to add {@code key}. Return its newly allocated id if it wasn't in
     * the hash table yet, or {@code -1-id} if it was already present in
     * the hash table.
     */
    public long add(long key1, long key2) {
        if (size >= maxSize) {
            assert size == maxSize;
            grow();
        }
        assert size < maxSize;
        return set(key1, key2, size);
    }

    @Override
    protected void removeAndAdd(long index) {
        final long id = id(index, -1);
        assert id >= 0;
        long keyOffset = id * 2;
        final long key1 = keys.set(keyOffset, 0);
        final long key2 = keys.set(keyOffset + 1, 0);
        reset(key1, key2, id);
    }

    @Override
    public void close() {
        Releasables.close(keys, () -> super.close());
    }

    static long hash(long key1, long key2) {
        return 31 * BitMixer.mix(key1) +  BitMixer.mix(key2);
    }
}
