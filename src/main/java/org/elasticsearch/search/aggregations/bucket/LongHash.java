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
import com.google.common.base.Preconditions;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;

/**
 * Specialized hash table implementation similar to BytesRefHash that maps
 *  long values to ids. Collisions are resolved with open addressing and linear
 *  probing, growth is smooth thanks to {@link BigArrays} and capacity is always
 *  a multiple of 2 for faster identification of buckets.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
public final class LongHash {

    // Open addressing typically requires having smaller load factors compared to linked lists because
    // collisions may result into worse lookup performance.
    private static final float DEFAULT_MAX_LOAD_FACTOR = 0.6f;

    private final float maxLoadFactor;
    private long size, maxSize;
    private LongArray keys;
    private LongArray ids;
    private long mask;

    // Constructor with configurable capacity and default maximum load factor.
    public LongHash(long capacity) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR);
    }

    //Constructor with configurable capacity and load factor.
    public LongHash(long capacity, float maxLoadFactor) {
        Preconditions.checkArgument(capacity >= 0, "capacity must be >= 0");
        Preconditions.checkArgument(maxLoadFactor > 0 && maxLoadFactor < 1, "maxLoadFactor must be > 0 and < 1");
        this.maxLoadFactor = maxLoadFactor;
        long buckets = 1L + (long) (capacity / maxLoadFactor);
        buckets = Math.max(1, Long.highestOneBit(buckets - 1) << 1); // next power of two
        assert buckets == Long.highestOneBit(buckets);
        maxSize = (long) (buckets * maxLoadFactor);
        assert maxSize >= capacity;
        size = 0;
        keys = BigArrays.newLongArray(buckets);
        ids = BigArrays.newLongArray(buckets);
        mask = buckets - 1;
    }

    /**
     * Return the number of allocated slots to store this hash table.
     */
    public long capacity() {
        return keys.size();
    }

    /**
     * Return the number of longs in this hash table.
     */
    public long size() {
        return size;
    }

    private static long hash(long value) {
        // Don't use the value directly. Under some cases eg dates, it could be that the low bits don't carry much value and we would like
        // all bits of the hash to carry as much value
        return MurmurHash3.hash(value);
    }

    private static long slot(long hash, long mask) {
        return hash & mask;
    }

    private static long nextSlot(long curSlot, long mask) {
        return (curSlot + 1) & mask; // linear probing
    }

    /**
     * Get the id associated with key at <code>0 &lte; index &lte; capacity()</code> or -1 if this slot is unused.
     */
    public long id(long index) {
        return ids.get(index) - 1;
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
    public long get(long key) {
        final long slot = slot(hash(key), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long id = ids.get(index);
            if (id == 0L || keys.get(index) == key) {
                return id - 1;
            }
        }
    }

    private long set(long key, long id) {
        assert size < maxSize;
        final long slot = slot(hash(key), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long curId = ids.get(index);
            if (curId == 0) { // means unset
                ids.set(index, id + 1);
                keys.set(index, key);
                ++size;
                return id;
            } else if (keys.get(index) == key) {
                return - curId;
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

    private void grow() {
        // The difference of this implementation of grow() compared to standard hash tables is that we are growing in-place, which makes
        // the re-mapping of keys to slots a bit more tricky.
        assert size == maxSize;
        final long prevSize = size;
        final long buckets = keys.size();
        // Resize arrays
        final long newBuckets = buckets << 1;
        assert newBuckets == Long.highestOneBit(newBuckets) : newBuckets; // power of 2
        keys = BigArrays.resize(keys, newBuckets);
        ids = BigArrays.resize(ids, newBuckets);
        mask = newBuckets - 1;
        size = 0;
        // First let's remap in-place: most data will be put in its final position directly
        for (long i = 0; i < buckets; ++i) {
            final long id = ids.set(i, 0);
            if (id > 0) {
                final long key = keys.set(i, 0);
                final long newId = set(key, id - 1);
                assert newId == id - 1 : newId + " " + (id - 1);
            }
        }
        // The only entries which have not been put in their final position in the previous loop are those that were stored in a slot that
        // is < slot(key, mask). This only happens when slot(key, mask) returned a slot that was close to the end of the array and colision
        // resolution has put it back in the first slots. This time, collision resolution will have put them at the beginning of the newly
        // allocated slots. Let's re-add them to make sure they are in the right slot. This 2nd loop will typically exit very early.
        for (long i = buckets; i < newBuckets; ++i) {
            final long id = ids.set(i, 0);
            if (id > 0) {
                --size; // we just removed an entry
                final long key = keys.set(i, 0);
                final long newId = set(key, id - 1); // add it back
                assert newId == id - 1 : newId + " " + (id - 1);
                assert newId == get(key);
            } else {
                break;
            }
        }
        assert size == prevSize;
        maxSize = (long) (newBuckets * maxLoadFactor);
        assert size < maxSize;
    }
    
}
