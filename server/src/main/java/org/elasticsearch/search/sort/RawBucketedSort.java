/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Type specialized sort implementations designed for use in aggregations.
 * Aggregations have a couple of super interesting characteristics:
 * <ul>
 * <li>They can have many, many buckets so this implementation backs to
 * {@link BigArrays} so it doesn't need to allocate any objects per bucket
 * and the circuit breaker in {@linkplain BigArrays} will automatically
 * track memory usage and abort execution if it grows too large.</li>
 * <li>Its fairly common for a bucket to be collected but not returned so
 * these implementations delay as much work as possible until collection</li>
 * </ul>
 * <p>
 * Every bucket is in one of two states: "gathering" or min/max "heap". While
 * "gathering" the next empty slot is stored in the "root" offset of the
 * bucket and collecting a value is just adding it in the next slot bumping
 * the tracking value at the root. So collecting values is {@code O(1)}.
 * Extracting the results in sorted order is {@code O(n * log n)} because,
 * well, sorting is {@code O(n * log n)}. When a bucket has collected
 * {@link #bucketSize} entries it is converted into a min "heap" in
 * {@code O(n)} time. Or into max heap, if {@link #order} is ascending.
 * </p>
 * <p>
 * Once a "heap", collecting a document is the heap-standard {@code O(log n)}
 * worst case. Critically, it is a very fast {@code O(1)} to check if a value
 * is competitive at all which, so long as buckets aren't hit in reverse
 * order, they mostly won't be. Extracting results in sorted order is still
 * {@code O(n * log n)}.
 * </p>
 * <p>
 * When we first collect a bucket we make sure that we've allocated enough
 * slots to hold all sort values for the entire bucket. In other words: the
 * storage is "dense" and we don't try to save space when storing partially
 * filled buckets.
 * </p>
 * <p>
 * We actually *oversize* the allocations
 * (like {@link BigArrays#overSize(long)}) to get amortized linear number
 * of allocations and to play well with our paged arrays.
 * </p>
 * @param <T> The type of the values to sort.
 */
public abstract class RawBucketedSort<T> implements Releasable {

    protected final BigArrays bigArrays;
    private final SortOrder order;
    private final int bucketSize;
    /**
     * {@code true} if the bucket is in heap mode, {@code false} if
     * it is still gathering.
     */
    private final BitArray heapMode;

    protected RawBucketedSort(BigArrays bigArrays, SortOrder order, int bucketSize) {
        this.bigArrays = bigArrays;
        this.order = order;
        this.bucketSize = bucketSize;
        heapMode = new BitArray(1, bigArrays);
    }

    /**
     * The order of the sort.
     */
    public final SortOrder getOrder() {
        return order;
    }

    /**
     * The number of values to store per bucket.
     */
    public int getBucketSize() {
        return bucketSize;
    }

    /**
     * Get the bound indexes (inclusive, exclusive) of the values for a bucket.
     * Returns [0, 0] if the bucket has never been collected.
     * <p>
     *     To be used by {@link #getValues(long)} implementations.
     * </p>
     */
    protected final Tuple<Long, Long> getBucketValuesBounds(long bucket) {
        long rootIndex = bucket * bucketSize + 1;
        if (rootIndex >= values().size()) {
            // We've never seen this bucket.
            return Tuple.tuple(0L, 0L);
        }
        long start = inHeapMode(bucket) ? rootIndex : (rootIndex + getNextGatherOffset(rootIndex) + 1);
        long end = rootIndex + bucketSize;
        return Tuple.tuple(start, end);
    }

    /**
     * Get the values for a bucket if it has been collected.
     * If it hasn't, then returns an empty list.
     */
    public abstract List<T> getValues(long bucket);

    /**
     * Is this bucket a min heap {@code true} or in gathering mode {@code false}?
     */
    public boolean inHeapMode(long bucket) {
        return heapMode.get(bucket);
    }

    /**
     * The {@linkplain BigArray} backing this sort.
     */
    protected abstract BigArray values();

    /**
     * Grow the {@linkplain BigArray} backing this sort to account for new buckets.
     * This will only be called if the array is too small.
     */
    protected abstract void growValues(long minSize);

    /**
     * Get the next index that should be "gathered" for a bucket rooted
     * at {@code rootIndex}.
     */
    protected abstract int getNextGatherOffset(long rootIndex);

    /**
     * Set the next index that should be "gathered" for a bucket rooted
     * at {@code rootIndex}.
     */
    protected abstract void setNextGatherOffset(long rootIndex, int offset);

    /**
     * Get the value at an index.
     */
    protected abstract SortValue getValue(long index);

    /**
     * {@code true} if the entry at index {@code lhs} is "better" than
     * the entry at {@code rhs}. "Better" in this means "lower" for
     * {@link SortOrder#ASC} and "higher" for {@link SortOrder#DESC}.
     */
    protected abstract boolean betterThan(long lhs, long rhs);

    /**
     * Swap the data at two indices.
     */
    protected abstract void swap(long lhs, long rhs);

    /**
     * Initialize the gather offsets after setting up values. Subclasses
     * should call this once, after setting up their {@link #values()}.
     */
    /*protected final void initGatherOffsets() {
        setNextGatherOffsets(1);
    }*/

    /**
     * Allocate storage for more buckets and store the "next gather offset"
     * for those new buckets.
     */
    private void grow(long minSize) {
        long oldMax = values().size();
        growValues(minSize);
        // Set the next gather offsets for all newly allocated buckets.
        // Subtracting 1 from oldMax to ignore the first element, which is the temporal value.
        setNextGatherOffsets(oldMax - ((oldMax - 1) % getBucketSize()));
    }

    /**
     * Maintain the "next gather offsets" for newly allocated buckets.
     */
    private void setNextGatherOffsets(long startingAt) {
        int nextOffset = getBucketSize() - 1;
        for (long bucketRoot = startingAt; bucketRoot < values().size(); bucketRoot += getBucketSize()) {
            setNextGatherOffset(bucketRoot, nextOffset);
        }
    }

    /**
     * Heapify a bucket whose entries are in random order.
     * <p>
     * This works by validating the heap property on each node, iterating
     * "upwards", pushing any out of order parents "down". Check out the
     * <a href="https://en.wikipedia.org/w/index.php?title=Binary_heap&oldid=940542991#Building_a_heap">wikipedia</a>
     * entry on binary heaps for more about this.
     * </p>
     * <p>
     * While this *looks* like it could easily be {@code O(n * log n)}, it is
     * a fairly well studied algorithm attributed to Floyd. There's
     * been a bunch of work that puts this at {@code O(n)}, close to 1.88n worst
     * case.
     * </p>
     * <ul>
     * <li>Hayward, Ryan; McDiarmid, Colin (1991).
     * <a href="https://web.archive.org/web/20160205023201/http://www.stats.ox.ac.uk/__data/assets/pdf_file/0015/4173/heapbuildjalg.pdf">
     * Average Case Analysis of Heap Building byRepeated Insertion</a> J. Algorithms.
     * <li>D.E. Knuth, ”The Art of Computer Programming, Vol. 3, Sorting and Searching”</li>
     * </ul>
     * @param rootIndex the index the start of the bucket
     */
    private void heapify(long rootIndex) {
        int maxParent = bucketSize / 2 - 1;
        for (int parent = maxParent; parent >= 0; parent--) {
            downHeap(rootIndex, parent);
        }
    }

    /**
     * Correct the heap invariant of a parent and its children. This
     * runs in {@code O(log n)} time.
     * @param rootIndex index of the start of the bucket
     * @param parent Index within the bucket of the parent to check.
     *               For example, 0 is the "root".
     */
    private void downHeap(long rootIndex, int parent) {
        while (true) {
            long parentIndex = rootIndex + parent;
            int worst = parent;
            long worstIndex = parentIndex;
            int leftChild = parent * 2 + 1;
            long leftIndex = rootIndex + leftChild;
            if (leftChild < bucketSize) {
                if (betterThan(worstIndex, leftIndex)) {
                    worst = leftChild;
                    worstIndex = leftIndex;
                }
                int rightChild = leftChild + 1;
                long rightIndex = rootIndex + rightChild;
                if (rightChild < bucketSize && betterThan(worstIndex, rightIndex)) {
                    worst = rightChild;
                    worstIndex = rightIndex;
                }
            }
            if (worst == parent) {
                break;
            }
            swap(worstIndex, parentIndex);
            parent = worst;
        }
    }

    @Override
    public final void close() {
        Releasables.close(values(), heapMode);
    }

    /**
     * Collects a value stored in the array[0] position.
     * <p>
     *     It may or may not be inserted in the heap, depending on if it is better than the current root.
     * </p>
     */
    protected final void collect(long bucket) {
        long rootIndex = bucket * bucketSize + 1;
        if (inHeapMode(bucket)) {
            if (betterThan(0, rootIndex)) {
                // Insert the temporal value into the heap
                swap(0, rootIndex);
                downHeap(rootIndex, 0);
            }
            return;
        }
        // Gathering mode
        long requiredSize = rootIndex + bucketSize;
        if (values().size() < requiredSize) {
            grow(requiredSize);
        }
        int next = getNextGatherOffset(rootIndex);
        assert 0 <= next && next < bucketSize
            : "Expected next to be in the range of valid buckets [0 <= " + next + " < " + bucketSize + "]";
        long index = next + rootIndex;
        swap(0, index);
        if (next == 0) {
            heapMode.set(bucket);
            heapify(rootIndex);
        } else {
            setNextGatherOffset(rootIndex, next - 1);
        }
    }

    /**
     * Superclass for implementations of {@linkplain RawBucketedSort} for {@code long} keys.
     */
    public static class ForLongs extends RawBucketedSort<Long> {
        private LongArray values;

        @SuppressWarnings("this-escape")
        public ForLongs(BigArrays bigArrays, SortOrder sortOrder, int bucketSize) {
            super(bigArrays, sortOrder, bucketSize);
            boolean success = false;
            try {
                values = bigArrays.newLongArray(1, false);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        @Override
        protected final BigArray values() {
            return values;
        }

        @Override
        protected final void growValues(long minSize) {
            values = bigArrays.grow(values, minSize);
        }

        @Override
        protected final int getNextGatherOffset(long rootIndex) {
            return (int) values.get(rootIndex);
        }

        @Override
        protected final void setNextGatherOffset(long rootIndex, int offset) {
            values.set(rootIndex, offset);
        }

        @Override
        protected final SortValue getValue(long index) {
            return SortValue.from(values.get(index));
        }

        @Override
        protected final boolean betterThan(long lhs, long rhs) {
            return getOrder().reverseMul() * Long.compare(values.get(lhs), values.get(rhs)) < 0;
        }

        @Override
        protected final void swap(long lhs, long rhs) {
            long tmp = values.get(lhs);
            values.set(lhs, values.get(rhs));
            values.set(rhs, tmp);
        }

        // Type-specific methods

        /**
         * Collects a value into a bucket.
         */
        public void collect(long bucket, long value) {
            values.set(0, value);
            super.collect(bucket);
        }

        @Override
        public List<Long> getValues(long bucket) {
            var bounds = getBucketValuesBounds(bucket);
            var size = bounds.v2() - bounds.v1();

            if (size == 0) {
                return emptyList();
            }

            var list = new ArrayList<Long>();

            for (long i = bounds.v1(); i < bounds.v2(); i++) {
                list.add(values.get(i));
            }

            list.sort(getOrder().wrap(Comparator.<Long>naturalOrder()));

            return list;
        }
    }
}
