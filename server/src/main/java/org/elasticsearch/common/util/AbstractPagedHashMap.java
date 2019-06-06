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
import org.elasticsearch.common.lease.Releasable;

/**
 * Base implementation for a hash table that is paged, recycles arrays and grows in-place.
 */
abstract class AbstractPagedHashMap implements Releasable {

    // Open addressing typically requires having smaller load factors compared to linked lists because
    // collisions may result into worse lookup performance.
    static final float DEFAULT_MAX_LOAD_FACTOR = 0.6f;

    static long hash(long value) {
        // Don't use the value directly. Under some cases eg dates, it could be that the low bits don't carry much value and we would like
        // all bits of the hash to carry as much value
        return BitMixer.mix64(value);
    }

    final BigArrays bigArrays;
    final float maxLoadFactor;
    long size, maxSize;
    long mask;

    AbstractPagedHashMap(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity must be >= 0");
        }
        if (maxLoadFactor <= 0 || maxLoadFactor >= 1) {
            throw new IllegalArgumentException("maxLoadFactor must be > 0 and < 1");
        }
        this.bigArrays = bigArrays;
        this.maxLoadFactor = maxLoadFactor;
        long buckets = 1L + (long) (capacity / maxLoadFactor);
        buckets = Math.max(1, Long.highestOneBit(buckets - 1) << 1); // next power of two
        assert buckets == Long.highestOneBit(buckets);
        maxSize = (long) (buckets * maxLoadFactor);
        assert maxSize >= capacity;
        size = 0;
        mask = buckets - 1;
    }

    /**
     * Return the number of allocated slots to store this hash table.
     */
    public long capacity() {
        return mask + 1;
    }

    /**
     * Return the number of longs in this hash table.
     */
    public long size() {
        return size;
    }

    static long slot(long hash, long mask) {
        return hash & mask;
    }

    static long nextSlot(long curSlot, long mask) {
        return (curSlot + 1) & mask; // linear probing
    }

    /** Resize to the given capacity. */
    protected abstract void resize(long capacity);

    protected abstract boolean used(long bucket);

    /** Remove the entry at the given index and add it back */
    protected abstract void removeAndAdd(long index);

    protected final void grow() {
        // The difference of this implementation of grow() compared to standard hash tables is that we are growing in-place, which makes
        // the re-mapping of keys to slots a bit more tricky.
        assert size == maxSize;
        final long prevSize = size;
        final long buckets = capacity();
        // Resize arrays
        final long newBuckets = buckets << 1;
        assert newBuckets == Long.highestOneBit(newBuckets) : newBuckets; // power of 2
        resize(newBuckets);
        mask = newBuckets - 1;
        // First let's remap in-place: most data will be put in its final position directly
        for (long i = 0; i < buckets; ++i) {
            if (used(i)) {
                removeAndAdd(i);
            }
        }
        // The only entries which have not been put in their final position in the previous loop are those that were stored in a slot that
        // is < slot(key, mask). This only happens when slot(key, mask) returned a slot that was close to the end of the array and collision
        // resolution has put it back in the first slots. This time, collision resolution will have put them at the beginning of the newly
        // allocated slots. Let's re-add them to make sure they are in the right slot. This 2nd loop will typically exit very early.
        for (long i = buckets; i < newBuckets; ++i) {
            if (used(i)) {
                removeAndAdd(i); // add it back
            } else {
                break;
            }
        }
        assert size == prevSize;
        maxSize = (long) (newBuckets * maxLoadFactor);
        assert size < maxSize;
    }

}
