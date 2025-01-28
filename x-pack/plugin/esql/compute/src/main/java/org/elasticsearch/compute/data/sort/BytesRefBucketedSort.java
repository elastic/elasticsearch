/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Aggregates the top N variable length {@link BytesRef} values per bucket.
 * See {@link BucketedSort} for more information.
 */
public class BytesRefBucketedSort implements Releasable {
    private final BucketedSortCommon common;
    private final CircuitBreaker breaker;
    private final String label;

    /**
     * An array containing all the values on all buckets. The structure is as follows:
     * <p>
     *     For each bucket, there are {@link BucketedSortCommon#bucketSize} elements, based
     *     on the bucket id (0, 1, 2...). Then, for each bucket, it can be in 2 states:
     * </p>
     * <ul>
     *     <li>
     *         Gather mode: All buckets start in gather mode, and remain here while they have
     *         less than bucketSize elements. In gather mode, the elements are stored in the
     *         array from the highest index to the lowest index. The lowest index contains
     *         the offset to the next slot to be filled.
     *         <p>
     *             This allows us to insert elements in O(1) time.
     *         </p>
     *         <p>
     *             When the bucketSize-th element is collected, the bucket transitions to heap
     *             mode, by heapifying its contents.
     *         </p>
     *     </li>
     *     <li>
     *         Heap mode: The bucket slots are organized as a min heap structure.
     *         <p>
     *             The root of the heap is the minimum value in the bucket,
     *             which allows us to quickly discard new values that are not in the top N.
     *         </p>
     *     </li>
     * </ul>
     */
    private ObjectArray<BreakingBytesRefBuilder> values;

    public BytesRefBucketedSort(CircuitBreaker breaker, String label, BigArrays bigArrays, SortOrder order, int bucketSize) {
        this.breaker = breaker;
        this.label = label;
        common = new BucketedSortCommon(bigArrays, order, bucketSize);
        boolean success = false;
        try {
            values = bigArrays.newObjectArray(0);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    private void checkInvariant(int bucket) {
        if (Assertions.ENABLED == false) {
            return;
        }
        long rootIndex = common.rootIndex(bucket);
        long requiredSize = common.endIndex(rootIndex);
        if (values.size() < requiredSize) {
            throw new AssertionError("values too short " + values.size() + " < " + requiredSize);
        }
        if (values.get(rootIndex) == null) {
            throw new AssertionError("new gather offset can't be null");
        }
        if (common.inHeapMode(bucket) == false) {
            common.assertValidNextOffset(getNextGatherOffset(rootIndex));
        } else {
            for (long l = rootIndex; l < common.endIndex(rootIndex); l++) {
                if (values.get(rootIndex) == null) {
                    throw new AssertionError("values missing in heap mode");
                }
            }
        }
    }

    /**
     * Collects a {@code value} into a {@code bucket}.
     * <p>
     *     It may or may not be inserted in the heap, depending on if it is better than the current root.
     * </p>
     */
    public void collect(BytesRef value, int bucket) {
        long rootIndex = common.rootIndex(bucket);
        if (common.inHeapMode(bucket)) {
            if (betterThan(value, values.get(rootIndex).bytesRefView())) {
                clearedBytesAt(rootIndex).append(value);
                downHeap(rootIndex, 0);
            }
            checkInvariant(bucket);
            return;
        }
        // Gathering mode
        long requiredSize = common.endIndex(rootIndex);
        if (values.size() < requiredSize) {
            grow(requiredSize);
        }
        int next = getNextGatherOffset(rootIndex);
        common.assertValidNextOffset(next);
        long index = next + rootIndex;
        clearedBytesAt(index).append(value);
        if (next == 0) {
            common.enableHeapMode(bucket);
            heapify(rootIndex);
        } else {
            ByteUtils.writeIntLE(next - 1, values.get(rootIndex).bytes(), 0);
        }
        checkInvariant(bucket);
    }

    /**
     * Merge the values from {@code other}'s {@code otherGroupId} into {@code groupId}.
     */
    public void merge(int bucket, BytesRefBucketedSort other, int otherBucket) {
        long otherRootIndex = other.common.rootIndex(otherBucket);
        if (otherRootIndex >= other.values.size()) {
            // The value was never collected.
            return;
        }
        other.checkInvariant(otherBucket);
        long otherStart = other.startIndex(otherBucket, otherRootIndex);
        long otherEnd = other.common.endIndex(otherRootIndex);
        // TODO: This can be improved for heapified buckets by making use of the heap structures
        for (long i = otherStart; i < otherEnd; i++) {
            collect(other.values.get(i).bytesRefView(), bucket);
        }
    }

    /**
     * Creates a block with the values from the {@code selected} groups.
     */
    public Block toBlock(BlockFactory blockFactory, IntVector selected) {
        // Check if the selected groups are all empty, to avoid allocating extra memory
        if (IntStream.range(0, selected.getPositionCount()).map(selected::getInt).noneMatch(bucket -> {
            long rootIndex = common.rootIndex(bucket);
            if (rootIndex >= values.size()) {
                // Never collected
                return false;
            }
            long start = startIndex(bucket, rootIndex);
            long end = common.endIndex(rootIndex);
            long size = end - start;
            return size > 0;
        })) {
            return blockFactory.newConstantNullBlock(selected.getPositionCount());
        }

        // Used to sort the values in the bucket.
        BytesRef[] bucketValues = new BytesRef[common.bucketSize];

        try (var builder = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
            for (int s = 0; s < selected.getPositionCount(); s++) {
                int bucket = selected.getInt(s);
                long rootIndex = common.rootIndex(bucket);
                if (rootIndex >= values.size()) {
                    // Never collected
                    builder.appendNull();
                    continue;
                }

                long start = startIndex(bucket, rootIndex);
                long end = common.endIndex(rootIndex);
                long size = end - start;

                if (size == 0) {
                    builder.appendNull();
                    continue;
                }

                if (size == 1) {
                    try (BreakingBytesRefBuilder bytes = values.get(start)) {
                        builder.appendBytesRef(bytes.bytesRefView());
                    }
                    values.set(start, null);
                    continue;
                }

                for (int i = 0; i < size; i++) {
                    try (BreakingBytesRefBuilder bytes = values.get(start + i)) {
                        bucketValues[i] = bytes.bytesRefView();
                    }
                    values.set(start + i, null);
                }

                // TODO: Make use of heap structures to faster iterate in order instead of copying and sorting
                Arrays.sort(bucketValues, 0, (int) size);

                builder.beginPositionEntry();
                if (common.order == SortOrder.ASC) {
                    for (int i = 0; i < size; i++) {
                        builder.appendBytesRef(bucketValues[i]);
                    }
                } else {
                    for (int i = (int) size - 1; i >= 0; i--) {
                        builder.appendBytesRef(bucketValues[i]);
                    }
                }
                builder.endPositionEntry();
            }
            return builder.build();
        }
    }

    private long startIndex(int bucket, long rootIndex) {
        if (common.inHeapMode(bucket)) {
            return rootIndex;
        }
        return rootIndex + getNextGatherOffset(rootIndex) + 1;
    }

    /**
     * Get the next index that should be "gathered" for a bucket rooted
     * at {@code rootIndex}.
     * <p>
     *     Using the first 4 bytes of the element to store the next gather offset.
     * </p>
     */
    private int getNextGatherOffset(long rootIndex) {
        BreakingBytesRefBuilder bytes = values.get(rootIndex);
        assert bytes.length() == Integer.BYTES;
        return ByteUtils.readIntLE(bytes.bytes(), 0);
    }

    /**
     * {@code true} if the entry at index {@code lhs} is "better" than
     * the entry at {@code rhs}. "Better" in this means "lower" for
     * {@link SortOrder#ASC} and "higher" for {@link SortOrder#DESC}.
     */
    private boolean betterThan(BytesRef lhs, BytesRef rhs) {
        return common.order.reverseMul() * lhs.compareTo(rhs) < 0;
    }

    /**
     * Swap the data at two indices.
     */
    private void swap(long lhs, long rhs) {
        BreakingBytesRefBuilder tmp = values.get(lhs);
        values.set(lhs, values.get(rhs));
        values.set(rhs, tmp);
    }

    /**
     * Allocate storage for more buckets and store the "next gather offset"
     * for those new buckets.
     */
    private void grow(long requiredSize) {
        long oldMax = values.size();
        values = common.bigArrays.grow(values, requiredSize);
        // Set the next gather offsets for all newly allocated buckets.
        fillGatherOffsets(oldMax - (oldMax % common.bucketSize));
    }

    /**
     * Maintain the "next gather offsets" for newly allocated buckets.
     */
    private void fillGatherOffsets(long startingAt) {
        assert startingAt % common.bucketSize == 0;
        int nextOffset = common.bucketSize - 1;
        for (long bucketRoot = startingAt; bucketRoot < values.size(); bucketRoot += common.bucketSize) {
            BreakingBytesRefBuilder bytes = values.get(bucketRoot);
            if (bytes != null) {
                continue;
            }
            bytes = new BreakingBytesRefBuilder(breaker, label);
            values.set(bucketRoot, bytes);
            bytes.grow(Integer.BYTES);
            bytes.setLength(Integer.BYTES);
            ByteUtils.writeIntLE(nextOffset, bytes.bytes(), 0);
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
        int maxParent = common.bucketSize / 2 - 1;
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
            if (leftChild < common.bucketSize) {
                if (betterThan(values.get(worstIndex).bytesRefView(), values.get(leftIndex).bytesRefView())) {
                    worst = leftChild;
                    worstIndex = leftIndex;
                }
                int rightChild = leftChild + 1;
                long rightIndex = rootIndex + rightChild;
                if (rightChild < common.bucketSize
                    && betterThan(values.get(worstIndex).bytesRefView(), values.get(rightIndex).bytesRefView())) {

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

    private BreakingBytesRefBuilder clearedBytesAt(long index) {
        BreakingBytesRefBuilder bytes = values.get(index);
        if (bytes == null) {
            bytes = new BreakingBytesRefBuilder(breaker, label);
            values.set(index, bytes);
        } else {
            bytes.clear();
        }
        return bytes;
    }

    @Override
    public final void close() {
        Releasable allValues = values == null ? () -> {} : Releasables.wrap(LongStream.range(0, values.size()).mapToObj(i -> {
            BreakingBytesRefBuilder bytes = values.get(i);
            return bytes == null ? (Releasable) () -> {} : bytes;
        }).toList().iterator());
        Releasables.close(allValues, values, common);
    }
}
