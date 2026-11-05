/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.blobcache.BlobCacheMetrics.RegionFillOrigin;

/**
 * A bounded FIFO memory of the regions recently evicted from the shared blob cache under capacity pressure.
 * <p>
 * Looking up a region that is about to be allocated tells whether the node is fetching data it already held and had to
 * give up (a re-fetch), how long ago that happened, and how many pressure evictions happened in between. The latter,
 * relative to the number of regions, approximates how much more capacity would have kept the region resident.
 * <p>
 * Keys are 64-bit fingerprints (see {@link #fingerprint}), so two distinct regions may share an entry; a collision only
 * causes a spurious re-fetch to be reported. All memory is preallocated, between 27 and 35 bytes per entry (see {@link #MAX_BYTES_PER_ENTRY}), and every operation
 * is O(1) so that it can run under the cache monitor.
 * <p>
 * Not thread-safe: callers must hold the {@link SharedBlobCacheService} monitor.
 */
final class RegionGhostTable {

    /**
     * A remembered eviction.
     *
     * @param evictedAtMillis relative time in millis at which the region was evicted
     * @param evictionsSince  number of pressure evictions recorded in this table after this one
     * @param evictedBy       the kind of fill that needed the slot of the evicted region
     * @param victimOrigin    the kind of fill that had originally brought the evicted region into the cache
     */
    record Ghost(long evictedAtMillis, long evictionsSince, RegionFillOrigin evictedBy, RegionFillOrigin victimOrigin) {}

    static final int MAX_CAPACITY = 1 << 29;

    /**
     * Worst-case heap per entry: 19 bytes in the ring (two longs and three one-byte fields) plus 4 bytes for each of the at most
     * four index slots per entry (the index is a power of two between two and four times the capacity).
     */
    static final int MAX_BYTES_PER_ENTRY = 35;

    /**
     * The share of the maximum heap the table may use.
     */
    static final double MAX_HEAP_FRACTION = 0.01;

    /**
     * @return the largest capacity whose worst-case footprint stays within {@link #MAX_HEAP_FRACTION} of the given heap size, bounded
     * by {@link #MAX_CAPACITY}
     */
    static long maxCapacityForHeap(long maxHeapBytes) {
        return Math.min(MAX_CAPACITY, (long) (maxHeapBytes * MAX_HEAP_FRACTION) / MAX_BYTES_PER_ENTRY);
    }

    private static final RegionFillOrigin[] ORIGINS = RegionFillOrigin.values();

    private final int capacity;

    // FIFO ring of evictions, the oldest entry being overwritten first
    private final long[] keys;
    private final long[] evictedAtMillis;
    private final byte[] evictedBy;
    private final byte[] victimOrigin;
    // false for ring positions never written or whose ghost was removed by a lookup
    private final boolean[] live;

    // open-addressing index (linear probing, load factor <= 0.5) mapping a key to its ring position + 1; 0 means empty
    private final int[] index;
    private final int indexMask;

    private long insertions;
    private int size;

    RegionGhostTable(int capacity) {
        if (capacity <= 0 || capacity > MAX_CAPACITY) {
            throw new IllegalArgumentException("ghost table capacity must be in [1, " + MAX_CAPACITY + "] but was [" + capacity + "]");
        }
        this.capacity = capacity;
        this.keys = new long[capacity];
        this.evictedAtMillis = new long[capacity];
        this.evictedBy = new byte[capacity];
        this.victimOrigin = new byte[capacity];
        this.live = new boolean[capacity];
        final int indexSize = Integer.highestOneBit(capacity * 2 - 1) << 1;
        this.index = new int[indexSize];
        this.indexMask = indexSize - 1;
    }

    /**
     * Builds the key of a region from the hash code of its file key and its region number.
     */
    static long fingerprint(int fileKeyHash, int region) {
        return ((long) fileKeyHash << 32) | Integer.toUnsignedLong(region);
    }

    /**
     * Remembers the eviction of a region, forgetting the oldest remembered eviction if the table is full. A previous
     * eviction of the same region is replaced so that the table holds the latest one.
     */
    void add(long key, long nowMillis, RegionFillOrigin evictedByOrigin, RegionFillOrigin evictedOrigin) {
        remove(key);
        final int pos = (int) (insertions % capacity);
        if (live[pos]) {
            final int slot = findSlot(keys[pos]);
            assert slot >= 0 && index[slot] == pos + 1 : "live ring position [" + pos + "] missing from index";
            deleteSlot(slot);
            live[pos] = false;
            size--;
        }
        keys[pos] = key;
        evictedAtMillis[pos] = nowMillis;
        evictedBy[pos] = (byte) evictedByOrigin.ordinal();
        victimOrigin[pos] = (byte) evictedOrigin.ordinal();
        live[pos] = true;
        size++;
        int slot = mix(key) & indexMask;
        while (index[slot] != 0) {
            slot = (slot + 1) & indexMask;
        }
        index[slot] = pos + 1;
        insertions++;
    }

    /**
     * Forgets the remembered eviction of a region and returns it, or returns {@code null} if the region is not remembered.
     */
    Ghost remove(long key) {
        final int slot = findSlot(key);
        if (slot < 0) {
            return null;
        }
        final int pos = index[slot] - 1;
        final Ghost ghost = new Ghost(
            evictedAtMillis[pos],
            Math.floorMod(insertions - 1 - pos, capacity),
            ORIGINS[evictedBy[pos]],
            ORIGINS[victimOrigin[pos]]
        );
        deleteSlot(slot);
        live[pos] = false;
        size--;
        return ghost;
    }

    int capacity() {
        return capacity;
    }

    int size() {
        return size;
    }

    private int findSlot(long key) {
        // terminates because the load factor is at most 0.5, so there is always an empty slot
        for (int slot = mix(key) & indexMask;; slot = (slot + 1) & indexMask) {
            final int value = index[slot];
            if (value == 0) {
                return -1;
            }
            if (keys[value - 1] == key) {
                return slot;
            }
        }
    }

    /**
     * Empties a slot of the index, shifting back the following entries of the probe sequence so that lookups never need
     * tombstones.
     */
    private void deleteSlot(int slot) {
        int hole = slot;
        index[hole] = 0;
        for (int next = (hole + 1) & indexMask; index[next] != 0; next = (next + 1) & indexMask) {
            final int home = mix(keys[index[next] - 1]) & indexMask;
            // the entry at next may fill the hole only if the hole lies on its probe sequence, i.e. between home and next
            if (((next - home) & indexMask) >= ((next - hole) & indexMask)) {
                index[hole] = index[next];
                index[next] = 0;
                hole = next;
            }
        }
    }

    // murmur3 64-bit finalizer, truncated to an int
    private static int mix(long key) {
        key ^= key >>> 33;
        key *= 0xff51afd7ed558ccdL;
        key ^= key >>> 33;
        key *= 0xc4ceb9fe1a85ec53L;
        key ^= key >>> 33;
        return (int) key;
    }
}
