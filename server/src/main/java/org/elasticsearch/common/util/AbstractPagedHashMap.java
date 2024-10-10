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

import org.elasticsearch.core.Releasable;

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
    public final long capacity() {
        return mask + 1;
    }

    /**
     * Return the number of longs in this hash table.
     */
    public final long size() {
        return size;
    }

    static long slot(long hash, long mask) {
        return hash & mask;
    }

    static long nextSlot(long curSlot, long mask) {
        return (curSlot + 1) & mask; // linear probing
    }

    /**
     * rehash the current hash table when the capacity have been increased from {@code buckets} to {@code newBuckets}.
     */
    protected abstract void rehash(long buckets, long newBuckets);

    protected final void grow() {
        // The difference of this implementation of grow() compared to standard hash tables is that we are growing in-place, which makes
        // the re-mapping of keys to slots a bit more tricky.
        assert size == maxSize;
        final long prevSize = size;
        final long buckets = capacity();
        // compute new sizes
        final long newBuckets = buckets << 1;
        assert newBuckets == Long.highestOneBit(newBuckets) : newBuckets; // power of 2
        mask = newBuckets - 1;
        assert size == prevSize;
        maxSize = (long) (newBuckets * maxLoadFactor);
        assert size < maxSize;
        rehash(buckets, newBuckets);
    }
}
