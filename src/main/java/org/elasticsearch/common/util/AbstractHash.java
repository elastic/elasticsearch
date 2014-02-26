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

import com.google.common.base.Preconditions;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;

/**
 * Base implementation for {@link BytesRefHash} and {@link LongHash}.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
abstract class AbstractHash implements Releasable {

    // Open addressing typically requires having smaller load factors compared to linked lists because
    // collisions may result into worse lookup performance.
    static final float DEFAULT_MAX_LOAD_FACTOR = 0.6f;

    final float maxLoadFactor;
    long size, maxSize;
    LongArray ids;
    long mask;

    AbstractHash(long capacity, float maxLoadFactor, PageCacheRecycler recycler) {
        Preconditions.checkArgument(capacity >= 0, "capacity must be >= 0");
        Preconditions.checkArgument(maxLoadFactor > 0 && maxLoadFactor < 1, "maxLoadFactor must be > 0 and < 1");
        this.maxLoadFactor = maxLoadFactor;
        long buckets = 1L + (long) (capacity / maxLoadFactor);
        buckets = Math.max(1, Long.highestOneBit(buckets - 1) << 1); // next power of two
        assert buckets == Long.highestOneBit(buckets);
        maxSize = (long) (buckets * maxLoadFactor);
        assert maxSize >= capacity;
        size = 0;
        ids = BigArrays.newLongArray(buckets, recycler, true);
        mask = buckets - 1;
    }

    /**
     * Return the number of allocated slots to store this hash table.
     */
    public long capacity() {
        return ids.size();
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

    /**
     * Get the id associated with key at <code>0 &lte; index &lte; capacity()</code> or -1 if this slot is unused.
     */
    public long id(long index) {
        return ids.get(index) - 1;
    }

    protected final long id(long index, long id) {
        return ids.set(index, id + 1) - 1;
    }

    /** Resize keys to the given capacity. */
    protected void resizeKeys(long capacity) {}

    /** Remove key at the given index and  */
    protected abstract void removeAndAdd(long index, long id);

    protected final void grow() {
        // The difference of this implementation of grow() compared to standard hash tables is that we are growing in-place, which makes
        // the re-mapping of keys to slots a bit more tricky.
        assert size == maxSize;
        final long prevSize = size;
        final long buckets = capacity();
        // Resize arrays
        final long newBuckets = buckets << 1;
        assert newBuckets == Long.highestOneBit(newBuckets) : newBuckets; // power of 2
        resizeKeys(newBuckets);
        ids = BigArrays.resize(ids, newBuckets);
        mask = newBuckets - 1;
        // First let's remap in-place: most data will be put in its final position directly
        for (long i = 0; i < buckets; ++i) {
            final long id = id(i, -1);
            if (id != -1) {
                removeAndAdd(i, id);
            }
        }
        // The only entries which have not been put in their final position in the previous loop are those that were stored in a slot that
        // is < slot(key, mask). This only happens when slot(key, mask) returned a slot that was close to the end of the array and colision
        // resolution has put it back in the first slots. This time, collision resolution will have put them at the beginning of the newly
        // allocated slots. Let's re-add them to make sure they are in the right slot. This 2nd loop will typically exit very early.
        for (long i = buckets; i < newBuckets; ++i) {
            final long id = id(i, -1);
            if (id != -1) {
                removeAndAdd(i, id); // add it back
            } else {
                break;
            }
        }
        assert size == prevSize;
        maxSize = (long) (newBuckets * maxLoadFactor);
        assert size < maxSize;
    }

    @Override
    public boolean release() {
        Releasables.release(ids);
        return true;
    }
}
