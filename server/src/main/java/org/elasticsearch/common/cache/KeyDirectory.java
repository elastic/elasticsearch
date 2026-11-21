/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

import java.util.Arrays;

/**
 * Gives every key hash that the adaptive controller tracks a small integer id, so that the shadow caches and the table of real
 * entries can keep their per-key state in plain arrays indexed by id. Without it each of them would hash the same key into its own
 * map on every access. Keys are identified by their 32-bit {@code hashCode}: keys with equal hash codes are treated as one.
 * <p>
 * An id stays allocated while at least one structure "holds" it (see {@link #hold} and {@link #drop}); when the last holder drops it
 * the id is recycled. Not thread-safe; guarded by the cache's LRU lock.
 */
final class KeyDirectory {
    static final int ABSENT = FingerprintIndex.ABSENT;
    private static final byte FREE = -1;

    private final FingerprintIndex index = new FingerprintIndex(64);
    private int[] hashes = new int[64];   // for a free id: the next free id
    private byte[] holders = new byte[64]; // FREE for a free id
    private int used;
    private int free = ABSENT;
    private boolean created;

    /** The number of ids that can currently be addressed; per-id arrays of the structures must be at least this long. */
    int capacity() {
        return hashes.length;
    }

    /** The id of the key with this hash code, or {@link #ABSENT}. */
    int get(int hash) {
        return index.get(hash);
    }

    /** Whether the last {@link #getOrCreate} had to allocate an id. */
    boolean lastCreated() {
        return created;
    }

    /** The id of the key with this hash code, allocating one if it has none. A fresh id has no holders. */
    int getOrCreate(int hash) {
        final int candidate;
        if (free != ABSENT) {
            candidate = free;
        } else {
            if (used == hashes.length) {
                final int capacity = used + (used >> 1);
                hashes = Arrays.copyOf(hashes, capacity);
                holders = Arrays.copyOf(holders, capacity);
            }
            candidate = used;
        }
        final int existing = index.getOrPut(hash, candidate);
        if (existing != ABSENT) {
            created = false;
            return existing;
        }
        created = true;
        if (free != ABSENT) {
            free = hashes[candidate];
        } else {
            used++;
        }
        hashes[candidate] = hash;
        holders[candidate] = 0;
        return candidate;
    }

    /** The hash code of the key with this id, as a non-negative long (the "fingerprint" the shadow caches use). */
    long fingerprint(int id) {
        return hashes[id] & 0xFFFFFFFFL;
    }

    void hold(int id) {
        holders[id]++;
    }

    /** Releases a hold; the id is recycled when nobody holds it any more. */
    void drop(int id) {
        assert holders[id] > 0;
        if (--holders[id] == 0) {
            release(id);
        }
    }

    /** Recycles an id that was allocated but is held by nobody. */
    void releaseIfUnheld(int id) {
        if (holders[id] == 0) {
            release(id);
        }
    }

    private void release(int id) {
        index.remove(hashes[id]);
        hashes[id] = free;
        holders[id] = FREE;
        free = id;
    }

    int size() {
        return index.size();
    }

    /** Whether the id is allocated (not on the free list). */
    boolean isAllocated(int id) {
        return id < used && holders[id] != FREE;
    }

    int holders(int id) {
        return holders[id];
    }

    int used() {
        return used;
    }

    /** Verifies the directory's internal consistency; for tests. */
    void checkInvariants() {
        int freeCount = 0;
        for (int id = free; id != ABSENT; id = hashes[id]) {
            freeCount++;
            if (holders[id] != FREE) {
                throw new AssertionError("free id " + id + " is not marked free");
            }
            if (freeCount > used) {
                throw new AssertionError("free list has a cycle");
            }
        }
        if (index.size() != used - freeCount) {
            throw new AssertionError("index has " + index.size() + " keys but " + (used - freeCount) + " ids are allocated");
        }
        for (int id = 0; id < used; id++) {
            if (holders[id] != FREE && index.get(hashes[id]) != id) {
                throw new AssertionError("allocated id " + id + " is not what the index maps its hash to");
            }
        }
    }
}
