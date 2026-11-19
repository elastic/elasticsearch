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
import java.util.function.IntPredicate;

/**
 * Bookkeeping of the real cache entries for the adaptive controller, indexed by {@link KeyDirectory} id: which entry belongs to
 * which key, and which entries are "outside" (not held by the followed shadow cache), oldest first. The outside list is threaded
 * through the per-id arrays, so tracking an entry allocates nothing. Not thread-safe; guarded by the cache's LRU lock.
 *
 * @param <E> the entry type
 */
final class RealEntryTable<E> {
    static final int NONE = -1;

    private final KeyDirectory directory;
    private Object[] entries = new Object[0];
    private boolean[] outside = new boolean[0];
    private int[] prev = new int[0];
    private int[] next = new int[0];
    private int outsideHead = NONE;
    private int outsideTail = NONE;

    RealEntryTable(KeyDirectory directory) {
        this.directory = directory;
    }

    void ensureCapacity(int capacity) {
        if (capacity > entries.length) {
            entries = Arrays.copyOf(entries, capacity);
            outside = Arrays.copyOf(outside, capacity);
            prev = Arrays.copyOf(prev, capacity);
            next = Arrays.copyOf(next, capacity);
        }
    }

    @SuppressWarnings("unchecked")
    E entry(int id) {
        return (E) entries[id];
    }

    /**
     * Tracks a newly inserted entry. If another entry with the same id is tracked, it is replaced: fingerprints are 32-bit hash codes
     * spread over 64 bits, so equal ones are treated as one key.
     */
    void put(int id, E entry, boolean isOutside) {
        if (entries[id] == null) {
            directory.hold(id);
        }
        entries[id] = entry;
        setOutside(id, isOutside);
    }

    /** Stops tracking {@code entry}, if it is the one tracked under {@code id}. */
    void remove(int id, E entry) {
        if (entries[id] != entry) {
            return;
        }
        setOutside(id, false);
        entries[id] = null;   // do not keep the entry (and its value) alive
        directory.drop(id);
    }

    boolean isOutside(int id, E entry) {
        return entries[id] == entry && outside[id];
    }

    /** Marks {@code entry} as outside, if it is the one tracked under {@code id}. */
    void markOutside(int id, E entry) {
        if (entries[id] == entry) {
            setOutside(id, true);
        }
    }

    /** Marks whatever entry is tracked under {@code id} as outside. */
    void markOutside(int id) {
        if (id < entries.length && entries[id] != null) {
            setOutside(id, true);
        }
    }

    /** Marks whatever entry is tracked under {@code id} as inside. */
    void markInside(int id) {
        if (id < entries.length) {
            setOutside(id, false);
        }
    }

    /** The id of the entry that has been outside the longest, or {@link #NONE}. */
    int firstOutside() {
        return outsideHead;
    }

    /** Recomputes which entries are outside, from scratch, with the given membership test on ids. */
    void rebuildOutside(IntPredicate held) {
        while (outsideHead != NONE) {
            setOutside(outsideHead, false);
        }
        for (int id = 0; id < entries.length; id++) {
            if (entries[id] != null && held.test(id) == false) {
                setOutside(id, true);
            }
        }
    }

    void clear() {
        for (int id = 0; id < entries.length; id++) {
            if (entries[id] != null) {
                entries[id] = null;
                outside[id] = false;
                directory.drop(id);
            }
        }
        outsideHead = NONE;
        outsideTail = NONE;
    }

    boolean tracks(int id) {
        return id < entries.length && entries[id] != null;
    }

    boolean isOutside(int id) {
        return outside[id];
    }

    /** Verifies that the outside list and the flags agree; for tests. */
    void checkInvariants() {
        int listed = 0;
        int previous = NONE;
        for (int id = outsideHead; id != NONE; id = next[id]) {
            if (outside[id] == false || entries[id] == null || prev[id] != previous) {
                throw new AssertionError("outside list is broken at id " + id);
            }
            previous = id;
            if (++listed > entries.length) {
                throw new AssertionError("outside list has a cycle");
            }
        }
        if (previous != outsideTail) {
            throw new AssertionError("outside tail is wrong");
        }
        int flagged = 0;
        for (int id = 0; id < entries.length; id++) {
            if (outside[id]) {
                flagged++;
            }
        }
        if (flagged != listed) {
            throw new AssertionError(flagged + " entries are flagged outside but " + listed + " are listed");
        }
    }

    private void setOutside(int id, boolean isOutside) {
        if (outside[id] == isOutside) {
            return;
        }
        outside[id] = isOutside;
        if (isOutside) {
            prev[id] = outsideTail;
            next[id] = NONE;
            if (outsideTail == NONE) {
                outsideHead = id;
            } else {
                next[outsideTail] = id;
            }
            outsideTail = id;
        } else {
            final int p = prev[id];
            final int n = next[id];
            if (p == NONE) {
                outsideHead = n;
            } else {
                next[p] = n;
            }
            if (n == NONE) {
                outsideTail = p;
            } else {
                prev[n] = p;
            }
        }
    }
}
