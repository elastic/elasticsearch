/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

/**
 * Drops a document's repeated values, for a caller that must act on each of them once.
 *
 * <p>{@link SortableBinaryDocValues} allows duplicates in either order it hands values back, and how far back a
 * duplicate can be depends on that order. Values that arrive {@link SortableBinaryDocValues.ValueOrder#SORTED}
 * put equal ones next to each other, so only the value just seen can repeat and one comparison settles it - which
 * is what every field that sorts as it writes gets, unchanged. Values in
 * {@link SortableBinaryDocValues.ValueOrder#ARRAY} order can repeat at any distance, so the whole document has to
 * be remembered.
 *
 * <p>Remembering it is a scan while there is little to scan and a set once there is not: filling a table costs
 * more than the comparisons it saves until a document holds more values than {@link #SCAN_LIMIT}, and a document
 * holding hundreds cannot be compared against all of them. The set is addressed openly and stamped with a
 * generation, so starting a document clears it without touching it.
 *
 * <p>Call {@link #reset(int)} for each document, then {@link #seen(BytesRef)} for each value the caller would
 * otherwise act on.
 */
public final class ValueDeduplicator {

    /**
     * How many values a document may hold before its duplicates are found through the set rather than by
     * comparing against the ones already kept.
     */
    static final int SCAN_LIMIT = 8;

    private final boolean adjacentOnly;

    /** The values this document has given up, and their hashes, kept so the set can be rebuilt out of them. */
    private BytesRefBuilder[] seen = new BytesRefBuilder[] { new BytesRefBuilder() };
    private int[] seenHash = new int[1];
    private int seenCount;

    /** For each table slot, the {@link #seen} entry it holds and the document it belongs to. */
    private int[] slotValue = new int[0];
    private int[] slotGeneration = new int[0];
    private int mask;
    private int generation;
    /** How many values the table takes before it is rebuilt larger, held under capacity so a probe always ends. */
    private int rebuildAt;

    /** Whether the current document is using the set. */
    private boolean hashed;

    public ValueDeduplicator(SortableBinaryDocValues values) {
        this(values.getValueOrder());
    }

    public ValueDeduplicator(SortableBinaryDocValues.ValueOrder valueOrder) {
        this.adjacentOnly = valueOrder == SortableBinaryDocValues.ValueOrder.SORTED;
    }

    /**
     * Starts a document, forgetting the previous one. {@code valueCount} is how many values it is expected to hold
     * and sizes the set; a document that turns out to hold more grows it rather than filling it, so the count is a
     * hint and not a bound.
     */
    public void reset(int valueCount) {
        seenCount = 0;
        hashed = adjacentOnly == false && valueCount > SCAN_LIMIT;
        if (hashed) {
            final int capacity = Integer.highestOneBit(Math.max(valueCount, SCAN_LIMIT) * 2 - 1) * 2;
            if (slotValue.length < capacity) {
                allocate(capacity);
            }
            nextGeneration();
        }
    }

    /**
     * Whether this document has already given up this value, recording it when it has not. The {@link BytesRef} is
     * copied rather than held, so the caller is free to read on.
     */
    public boolean seen(BytesRef value) {
        if (hashed) {
            return seenInTable(value);
        }
        for (int i = 0; i < seenCount; i++) {
            if (seen[i].get().equals(value)) {
                return true;
            }
        }
        // Sorted values only ever repeat next to each other, so the one just seen is the only one worth keeping.
        final int at = adjacentOnly ? 0 : seenCount;
        keep(at, value);
        seenCount = at + 1;
        return false;
    }

    private boolean seenInTable(BytesRef value) {
        final int hash = value.hashCode();
        final int slot = probe(hash, value);
        if (slot < 0) {
            return true;
        }
        keep(seenCount, value);
        seenHash[seenCount] = hash;
        slotGeneration[slot] = generation;
        slotValue[slot] = seenCount;
        seenCount++;
        if (seenCount >= rebuildAt) {
            rebuild();
        }
        return false;
    }

    /** The slot this value belongs in, or {@code -1} when the document has already given it up. */
    private int probe(int hash, BytesRef value) {
        int slot = hash & mask;
        int probes = 0;
        // Ends because the table is rebuilt before it fills, so an empty slot is always left to find. Counted so
        // that a table which did fill fails here rather than spinning.
        while (slotGeneration[slot] == generation) {
            final int at = slotValue[slot];
            if (seenHash[at] == hash && seen[at].get().equals(value)) {
                return -1;
            }
            slot = (slot + 1) & mask;
            assert ++probes < slotValue.length : "set is full: " + seenCount + " values in " + slotValue.length + " slots";
        }
        return slot;
    }

    /** Doubles the table and puts this document's values back, for a document holding more than it was sized for. */
    private void rebuild() {
        allocate(slotValue.length * 2);
        nextGeneration();
        for (int at = 0; at < seenCount; at++) {
            int slot = seenHash[at] & mask;
            while (slotGeneration[slot] == generation) {
                slot = (slot + 1) & mask;
            }
            slotGeneration[slot] = generation;
            slotValue[slot] = at;
        }
    }

    private void allocate(int capacity) {
        slotValue = new int[capacity];
        slotGeneration = new int[capacity];
        mask = capacity - 1;
        rebuildAt = capacity / 2;
        generation = 0;
    }

    private void nextGeneration() {
        if (++generation == 0) {
            // Wrapped: every stamp left behind would read as this document's.
            Arrays.fill(slotGeneration, 0);
            generation = 1;
        }
    }

    private void keep(int at, BytesRef value) {
        if (seen.length <= at) {
            final int oldLen = seen.length;
            final int newLen = ArrayUtil.oversize(at + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            seen = Arrays.copyOf(seen, newLen);
            for (int i = oldLen; i < newLen; i++) {
                seen[i] = new BytesRefBuilder();
            }
            seenHash = Arrays.copyOf(seenHash, newLen);
        }
        seen[at].copyBytes(value);
    }
}
