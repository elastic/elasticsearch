/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Types.forciblyCast;

/**
 * A specialized {@link PriorityQueue} implementation for composite buckets.
 */
final class CompositeValuesCollectorQueue extends PriorityQueue<Integer> implements Releasable {
    private class Slot {
        int value;

        Slot(int initial) {
            this.value = initial;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Slot slot = (Slot) o;
            return CompositeValuesCollectorQueue.this.equals(value, slot.value);
        }

        @Override
        public int hashCode() {
            return CompositeValuesCollectorQueue.this.hashCode(value);
        }
    }

    // the slot for the current candidate
    private static final int CANDIDATE_SLOT = Integer.MAX_VALUE;

    private final BigArrays bigArrays;
    private final int maxSize;
    private final Map<Slot, Integer> map;
    private final SingleDimensionValuesSource<?>[] arrays;

    private LongArray docCounts;
    private boolean afterKeyIsSet = false;
    private int leafReaderOrd = -1; // current LeafReaderContext ordinal

    /**
     * Constructs a composite queue with the specified size and sources.
     *
     * @param sources The list of {@link CompositeValuesSourceConfig} to build the composite buckets.
     * @param size The number of composite buckets to keep.
     */
    CompositeValuesCollectorQueue(BigArrays bigArrays, SingleDimensionValuesSource<?>[] sources, int size) {
        super(size);
        this.bigArrays = bigArrays;
        this.maxSize = size;
        this.arrays = sources;
        this.map = new HashMap<>(size);
        this.docCounts = bigArrays.newLongArray(1, false);
    }

    /**
     * Sets after key
     * @param afterKey composite key
     */
    public void setAfterKey(CompositeKey afterKey) {
        assert afterKey.size() == arrays.length;
        afterKeyIsSet = true;
        for (int i = 0; i < afterKey.size(); i++) {
            try {
                arrays[i].setAfter(afterKey.get(i));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("incompatible value in the position " + i + ": " + ex.getMessage(), ex);
            }
        }
    }

    @Override
    protected boolean lessThan(Integer a, Integer b) {
        return compare(a, b) > 0;
    }

    /**
     * Whether the queue is full or not.
     */
    boolean isFull() {
        return size() >= maxSize;
    }

    /**
     * Compares the current candidate with the values in the queue and returns
     * the slot if the candidate is already in the queue or null if the candidate is not present.
     */
    Integer compareCurrent() {
        return map.get(new Slot(CANDIDATE_SLOT));
    }

    /**
     * Returns the lowest value (exclusive) of the leading source.
     */
    Comparable<?> getLowerValueLeadSource() {
        return afterKeyIsSet ? arrays[0].getAfter() : null;
    }

    /**
     * Returns the upper value (inclusive) of the leading source.
     */
    Comparable<?> getUpperValueLeadSource() throws IOException {
        return size() >= maxSize ? arrays[0].toComparable(top()) : null;
    }

    /**
     * Returns the document count in <code>slot</code>.
     */
    long getDocCount(int slot) {
        return docCounts.get(slot);
    }

    /**
     * Copies the current value in <code>slot</code>.
     */
    private void copyCurrent(int slot, long value) {
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].copyCurrent(slot);
        }
        docCounts = bigArrays.grow(docCounts, slot + 1);
        docCounts.set(slot, value);
    }

    /**
     * Compares the values in <code>slot1</code> with the values in <code>slot2</code>.
     */
    int compare(int slot1, int slot2) {
        assert slot2 != CANDIDATE_SLOT;
        for (int i = 0; i < arrays.length; i++) {
            final int cmp;
            if (slot1 == CANDIDATE_SLOT) {
                cmp = arrays[i].compareCurrent(slot2);
            } else {
                cmp = arrays[i].compare(slot1, slot2);
            }
            if (cmp != 0) {
                return cmp > 0 ? i + 1 : -(i + 1);
            }
        }
        return 0;
    }

    /**
     * Returns true if the values in <code>slot1</code> are equals to the value in <code>slot2</code>.
     */
    boolean equals(int slot1, int slot2) {
        assert slot2 != CANDIDATE_SLOT;
        for (int i = 0; i < arrays.length; i++) {
            final int cmp;
            if (slot1 == CANDIDATE_SLOT) {
                cmp = arrays[i].compareCurrent(slot2);
            } else {
                cmp = arrays[i].compare(slot1, slot2);
            }
            if (cmp != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a hash code value for the values in <code>slot</code>.
     */
    int hashCode(int slot) {
        int result = 1;
        for (int i = 0; i < arrays.length; i++) {
            result = 31 * result + (slot == CANDIDATE_SLOT ? arrays[i].hashCodeCurrent() : arrays[i].hashCode(slot));
        }
        return result;
    }

    /**
     * Compares the after values with the values in <code>slot</code>.
     */
    private int compareCurrentWithAfter() {
        for (int i = 0; i < arrays.length; i++) {
            int cmp = arrays[i].compareCurrentWithAfter();
            if (cmp != 0) {
                return cmp > 0 ? i + 1 : -(i + 1);
            }
        }
        return 0;
    }

    /**
     * Builds the {@link CompositeKey} for <code>slot</code>.
     */
    CompositeKey toCompositeKey(int slot) throws IOException {
        assert slot < maxSize;
        Comparable<?>[] values = new Comparable<?>[arrays.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = arrays[i].toComparable(slot);
        }
        return new CompositeKey(values);
    }

    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector in) throws IOException {
        return getLeafCollector(null, context, in);
    }

    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * If <code>forceLeadSourceValue</code> is not null, the leading source will use this value
     * for each document.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(Comparable<?> forceLeadSourceValue, LeafReaderContext context, LeafBucketCollector in)
        throws IOException {
        int last = arrays.length - 1;
        LeafBucketCollector collector = in;
        boolean requiresRehashingWhenSwitchingLeafReaders = false;
        while (last > 0) {
            SingleDimensionValuesSource<?> valuesSource = arrays[last--];
            requiresRehashingWhenSwitchingLeafReaders |= valuesSource.requiresRehashingWhenSwitchingLeafReaders();
            collector = valuesSource.getLeafCollector(context, collector);
        }
        SingleDimensionValuesSource<?> valuesSource = arrays[last];
        requiresRehashingWhenSwitchingLeafReaders |= valuesSource.requiresRehashingWhenSwitchingLeafReaders();
        if (forceLeadSourceValue != null) {
            collector = valuesSource.getLeafCollector(forciblyCast(forceLeadSourceValue), context, collector);
        } else {
            collector = valuesSource.getLeafCollector(context, collector);
        }
        boolean switchedLeafReaders = context.ord != leafReaderOrd;
        if (map.isEmpty() == false && requiresRehashingWhenSwitchingLeafReaders && switchedLeafReaders) {
            List<Map.Entry<Slot, Integer>> entries = new ArrayList<>(map.entrySet());
            map.clear();
            entries.forEach(e -> map.put(e.getKey(), e.getValue()));
        }
        leafReaderOrd = context.ord;
        return collector;
    }

    /**
     * Check if the current candidate should be added in the queue.
     * @return <code>true</code> if the candidate is competitive (added or already in the queue).
     */
    boolean addIfCompetitive(long inc) {
        return addIfCompetitive(0, inc);
    }

    /**
     * Add or update the current composite key in the queue if the values are competitive.
     *
     * @param indexSortSourcePrefix 0 if the index sort is null or doesn't match any of the sources field,
     *                              a value greater than 0 indicates the prefix len of the sources that match the index sort
     *                              and a negative value indicates that the index sort match the source field but the order is reversed.
     * @return <code>true</code> if the candidate is competitive (added or already in the queue).
     *
     * @throws CollectionTerminatedException if the current collection can be terminated early due to index sorting.
     */
    boolean addIfCompetitive(int indexSortSourcePrefix, long inc) {
        // checks if the candidate key is competitive
        Integer topSlot = compareCurrent();
        if (topSlot != null) {
            // this key is already in the top N, skip it
            docCounts.increment(topSlot, inc);
            return true;
        }
        if (afterKeyIsSet) {
            int cmp = compareCurrentWithAfter();
            if (cmp <= 0) {
                if (indexSortSourcePrefix < 0 && cmp == indexSortSourcePrefix) {
                    // the leading index sort is in the reverse order of the leading source
                    // so we can early terminate when we reach a document that is smaller
                    // than the after key (collected on a previous page).
                    throw new CollectionTerminatedException();
                }
                // key was collected on a previous page, skip it (>= afterKey).
                return false;
            }
        }
        if (size() >= maxSize) {
            // the tree map is full, check if the candidate key should be kept
            int cmp = compare(CANDIDATE_SLOT, top());
            if (cmp > 0) {
                if (cmp <= indexSortSourcePrefix) {
                    // index sort guarantees that there is no key greater or equal than the
                    // current one in the subsequent documents so we can early terminate.
                    throw new CollectionTerminatedException();
                }
                // the candidate key is not competitive, skip it.
                return false;
            }
        }
        // the candidate key is competitive
        final int newSlot;
        if (size() >= maxSize) {
            // the queue is full, we replace the last key with this candidate
            int slot = pop();
            map.remove(new Slot(slot));
            // and we recycle the deleted slot
            newSlot = slot;
        } else {
            newSlot = size();
        }
        // move the candidate key to its new slot
        copyCurrent(newSlot, inc);
        map.put(new Slot(newSlot), newSlot);
        add(newSlot);
        return true;
    }

    @Override
    public void close() {
        Releasables.close(docCounts);
    }
}
