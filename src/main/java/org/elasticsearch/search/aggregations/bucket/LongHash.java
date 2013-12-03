/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.aggregations.bucket;

import com.carrotsearch.hppc.hash.MurmurHash3;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;

/**
 * Specialized hash table implementation similar to BytesRefHash that maps
 *  long values to ids. Collisions are resolved with open addressing and linear
 *  probing, growth is smooth thanks to {@link BigArrays} and capacity is always
 *  a multiple of 2 for faster identification of buckets.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
public final class LongHash extends AbstractHash {

    private LongArray keys;

    // Constructor with configurable capacity and default maximum load factor.
    public LongHash(long capacity, PageCacheRecycler recycler) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, recycler);
    }

    //Constructor with configurable capacity and load factor.
    public LongHash(long capacity, float maxLoadFactor, PageCacheRecycler recycler) {
        super(capacity, maxLoadFactor, recycler);
        keys = BigArrays.newLongArray(capacity(), recycler, false);
    }

    private static long hash(long value) {
        // Don't use the value directly. Under some cases eg dates, it could be that the low bits don't carry much value and we would like
        // all bits of the hash to carry as much value
        return MurmurHash3.hash(value);
    }

    /**
     * Return the key at <code>0 &lte; index &lte; capacity()</code>. The result is undefined if the slot is unused.
     */
    public long key(long index) {
        return keys.get(index);
    }

    /**
     * Get the id associated with <code>key</code>
     */
    public long find(long key) {
        final long slot = slot(hash(key), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long id = id(index);
            if (id == -1 || keys.get(index) == key) {
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
                keys.set(index, key);
                ++size;
                return id;
            } else if (keys.get(index) == key) {
                return -1 - curId;
            }
        }
    }

    private void reset(long key, long id) {
        final long slot = slot(hash(key), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long curId = id(index);
            if (curId == -1) { // means unset
                id(index, id);
                keys.set(index, key);
                break;
            } else {
                assert keys.get(index) != key;
            }
        }
    }

    /**
     * Try to add <code>key</code>. Return its newly allocated id if it wasn't in the hash table yet, or </code>-1-id</code>
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
    protected void resizeKeys(long capacity) {
        keys = BigArrays.resize(keys, capacity);
    }

    @Override
    protected void removeAndAdd(long index, long id) {
        final long key = keys.set(index, 0);
        reset(key, id);
    }

    @Override
    public boolean release() {
        boolean success = false;
        try {
            super.release();
            success = true;
        } finally {
            Releasables.release(success, keys);
        }
        return true;
    }

}
