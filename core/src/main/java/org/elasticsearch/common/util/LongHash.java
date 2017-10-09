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

import org.elasticsearch.common.lease.Releasable;

/**
 * Specialized hash table implementation similar to BytesRefHash that maps
 * long values to ids. Collisions are resolved with open addressing and linear
 * probing, growth is smooth thanks to {@link BigArrays} and capacity is always
 * a multiple of 2 for faster identification of buckets.
 * This class is not thread-safe.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
public final class LongHash extends AbstractHash {

    private LongArray keys;

    // Constructor with configurable capacity and default maximum load factor.
    public LongHash(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    //Constructor with configurable capacity and load factor.
    public LongHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        keys = bigArrays.newLongArray(capacity, false);
    }

    /**
     * Return the key at <code>0 &lt;= index &lt;= capacity()</code>. The result is undefined if the slot is unused.
     */
    public long get(long id) {
        return keys.get(id);
    }

    /**
     * Get the id associated with <code>key</code> or -1 if the key is not contained in the hash.
     */
    public long find(long key) {
        final long slot = slot(hash(key), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long id = id(index);
            if (id == -1 || keys.get(id) == key) {
                return id;
            }
        }
    }

    private long set(long key, long id) {
        assert size < maxSize;
        final long slot = slot(hash(key), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                id(index, id);
                append(id, key);
                ++size;
                return id;
            } else if (keys.get(curId) == key) {
                return -1 - curId;
            }
        }
    }

    private void append(long id, long key) {
        keys = bigArrays.grow(keys, id + 1);
        keys.set(id, key);
    }

    private void reset(long key, long id) {
        final long slot = slot(hash(key), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                id(index, id);
                append(id, key);
                break;
            }
        }
    }

    /**
     * Try to add <code>key</code>. Return its newly allocated id if it wasn't in the hash table yet, or <code>-1-id</code>
     * if it was already present in the hash table.
     */
    public long add(long key) {
        if (size >= maxSize) {
            assert size == maxSize;
            grow();
        }
        assert size < maxSize;
        return set(key, size);
    }

    @Override
    protected void removeAndAdd(long index) {
        final long id = id(index, -1);
        assert id >= 0;
        final long key = keys.set(id, 0);
        reset(key, id);
    }

    @Override
    public void close() {
        try (Releasable releasable = keys) {
            super.close();
        }
    }

}
