/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.core.Types.forciblyCast;

/**
 * A specialized {@link ObjectArrayPriorityQueue} implementation for composite buckets.
 */
final class CompositeValuesCollectorQueue extends ObjectArrayPriorityQueue<Integer> {
    private class Slot {
        final int value;

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

    @FunctionalInterface
    private interface CompetitiveBoundsChangedListener {
        void boundsChanged(int topSlot) throws IOException;
    }

    // the slot for the current candidate
    private static final int CANDIDATE_SLOT = Integer.MAX_VALUE;

    private final BigArrays bigArrays;
    private final int maxSize;
    private final Map<Slot, Integer> map;
    private final SingleDimensionValuesSource<?>[] arrays;
    private final CompetitiveBoundsChangedListener competitiveBoundsChangedListener;

    private LongArray docCounts;
    private boolean afterKeyIsSet = false;

    /**
     * Constructs a composite queue with the specified size and sources.
     *
     * @param sources     The list of {@link CompositeValuesSourceConfig} to build the composite buckets.
     * @param size        The number of composite buckets to keep.
     * @param indexReader
     */
    CompositeValuesCollectorQueue(BigArrays bigArrays, SingleDimensionValuesSource<?>[] sources, int size, IndexReader indexReader) {
        super(size, bigArrays);
        this.bigArrays = bigArrays;
        this.maxSize = size;
        this.arrays = sources;

        boolean success = false;
        try {
            // If the leading source is a GlobalOrdinalValuesSource we can apply an optimization which requires
            // tracking the highest competitive value.
            if (arrays[0] instanceof GlobalOrdinalValuesSource globalOrdinalValuesSource) {
                if (shouldApplyGlobalOrdinalDynamicPruningForLeadingSource(sources, size, indexReader)) {
                    competitiveBoundsChangedListener = globalOrdinalValuesSource::updateHighestCompetitiveValue;
                } else {
                    competitiveBoundsChangedListener = null;
                }
            } else {
                competitiveBoundsChangedListener = null;
            }

            this.map = Maps.newMapWithExpectedSize(size);
            this.docCounts = bigArrays.newLongArray(1, false);
            success = true;
        } finally {
            if (success == false) {
                super.close();
            }
        }
    }

    private static boolean shouldApplyGlobalOrdinalDynamicPruningForLeadingSource(
        SingleDimensionValuesSource<?>[] sources,
        int size,
        IndexReader indexReader
    ) {
        if (sources.length == 0) {
            return false;
        }
        if (sources[0] instanceof GlobalOrdinalValuesSource firstSource) {
            if (firstSource.mayDynamicallyPrune(indexReader) == false) {
                return false;
            }

            long approximateTotalNumberOfBuckets = firstSource.getUniqueValueCount();
            if (sources.length > 1) {
                // When there are multiple sources, it's hard to guess how many
                // unique buckets there might be. Let's be conservative and
                // assume that other sources increase the number of buckets by
                // 3x.
                approximateTotalNumberOfBuckets *= 3L;
            }
            // If the size is not significantly less than the total number of
            // buckets then dynamic pruning can't help much.
            if (size >= approximateTotalNumberOfBuckets / 8) {
                return false;
            }

            // Try to estimate the width of the ordinal range that might be
            // returned on each page. Since not all ordinals might match the
            // query, we're increasing `size` by 25%.
            long rangeWidthPerPage = size + (size / 4);
            if (sources.length > 1) {
                // Again assume other sources bump the number of buckets by 3x
                rangeWidthPerPage /= 3;
            }
            if (rangeWidthPerPage > GlobalOrdinalValuesSource.MAX_TERMS_FOR_DYNAMIC_PRUNING) {
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Return true if this queue produces a {@link LeafBucketCollector} that may
     * dynamically prune hits that are not competitive.
     */
    public boolean mayDynamicallyPrune() {
        return competitiveBoundsChangedListener != null;
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
        for (SingleDimensionValuesSource<?> array : arrays) {
            array.copyCurrent(slot);
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
        for (SingleDimensionValuesSource<?> array : arrays) {
            final int cmp;
            if (slot1 == CANDIDATE_SLOT) {
                cmp = array.compareCurrent(slot2);
            } else {
                cmp = array.compare(slot1, slot2);
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
        for (SingleDimensionValuesSource<?> array : arrays) {
            result = 31 * result + (slot == CANDIDATE_SLOT ? array.hashCodeCurrent() : array.hashCode(slot));
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
        LeafBucketCollector leafBucketCollector = getLeafCollector(null, context, in);

        // As we are starting to collect from a new segment we need to update the topChangedListener if present
        // and if the queue is full.
        if (competitiveBoundsChangedListener != null && size() >= maxSize) {
            competitiveBoundsChangedListener.boundsChanged(top());
        }
        return leafBucketCollector;
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
        while (last > 0) {
            collector = arrays[last--].getLeafCollector(context, collector);
        }
        if (forceLeadSourceValue != null) {
            collector = arrays[last].getLeafCollector(forciblyCast(forceLeadSourceValue), context, collector);
        } else {
            collector = arrays[last].getLeafCollector(context, collector);
        }
        return collector;
    }

    /**
     * Check if the current candidate should be added in the queue.
     * @return <code>true</code> if the candidate is competitive (added or already in the queue).
     */
    boolean addIfCompetitive(long inc) throws IOException {
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
    boolean addIfCompetitive(int indexSortSourcePrefix, long inc) throws IOException {
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
            newSlot = (int) size();
        }
        // move the candidate key to its new slot
        copyCurrent(newSlot, inc);
        map.put(new Slot(newSlot), newSlot);
        add(newSlot);

        if (competitiveBoundsChangedListener != null && size() >= maxSize) {
            competitiveBoundsChangedListener.boundsChanged(top());
        }
        return true;
    }

    @Override
    protected void doClose() {
        Releasables.close(docCounts);
    }
}
