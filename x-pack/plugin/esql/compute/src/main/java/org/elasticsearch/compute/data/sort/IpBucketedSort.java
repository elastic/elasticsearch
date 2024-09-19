/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Aggregates the top N IP values per bucket.
 * See {@link BucketedSort} for more information.
 */
public class IpBucketedSort implements Releasable {
    private static final int IP_LENGTH = 16;

    // BytesRefs used in internal methods
    private final BytesRef scratch1 = new BytesRef();
    private final BytesRef scratch2 = new BytesRef();
    /**
     * Bytes used as temporal storage for scratches
     */
    private final byte[] scratchBytes = new byte[IP_LENGTH];

    private final BigArrays bigArrays;
    private final SortOrder order;
    private final int bucketSize;
    /**
     * {@code true} if the bucket is in heap mode, {@code false} if
     * it is still gathering.
     */
    private final BitArray heapMode;
    /**
     * An array containing all the values on all buckets. The structure is as follows:
     * <p>
     *     For each bucket, there are bucketSize elements, based on the bucket id (0, 1, 2...).
     *     Then, for each bucket, it can be in 2 states:
     * </p>
     * <ul>
     *     <li>
     *         Gather mode: All buckets start in gather mode, and remain here while they have less than bucketSize elements.
     *         In gather mode, the elements are stored in the array from the highest index to the lowest index.
     *         The lowest index contains the offset to the next slot to be filled.
     *         <p>
     *             This allows us to insert elements in O(1) time.
     *         </p>
     *         <p>
     *             When the bucketSize-th element is collected, the bucket transitions to heap mode, by heapifying its contents.
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
    private ByteArray values;

    public IpBucketedSort(BigArrays bigArrays, SortOrder order, int bucketSize) {
        this.bigArrays = bigArrays;
        this.order = order;
        this.bucketSize = bucketSize;
        heapMode = new BitArray(0, bigArrays);

        boolean success = false;
        try {
            values = bigArrays.newByteArray(0, false);
            success = true;
        } finally {
            if (success == false) {
                close();
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
        assert value.length == IP_LENGTH;
        long rootIndex = (long) bucket * bucketSize;
        if (inHeapMode(bucket)) {
            if (betterThan(value, get(rootIndex, scratch1))) {
                set(rootIndex, value);
                downHeap(rootIndex, 0);
            }
            return;
        }
        // Gathering mode
        long requiredSize = (rootIndex + bucketSize) * IP_LENGTH;
        if (values.size() < requiredSize) {
            grow(requiredSize);
        }
        int next = getNextGatherOffset(rootIndex);
        assert 0 <= next && next < bucketSize
            : "Expected next to be in the range of valid buckets [0 <= " + next + " < " + bucketSize + "]";
        long index = next + rootIndex;
        set(index, value);
        if (next == 0) {
            heapMode.set(bucket);
            heapify(rootIndex);
        } else {
            setNextGatherOffset(rootIndex, next - 1);
        }
    }

    /**
     * The order of the sort.
     */
    public SortOrder getOrder() {
        return order;
    }

    /**
     * The number of values to store per bucket.
     */
    public int getBucketSize() {
        return bucketSize;
    }

    /**
     * Get the first and last indexes (inclusive, exclusive) of the values for a bucket.
     * Returns [0, 0] if the bucket has never been collected.
     */
    private Tuple<Long, Long> getBucketValuesIndexes(int bucket) {
        long rootIndex = (long) bucket * bucketSize;
        if (rootIndex >= values.size() / IP_LENGTH) {
            // We've never seen this bucket.
            return Tuple.tuple(0L, 0L);
        }
        long start = inHeapMode(bucket) ? rootIndex : (rootIndex + getNextGatherOffset(rootIndex) + 1);
        long end = rootIndex + bucketSize;
        return Tuple.tuple(start, end);
    }

    /**
     * Merge the values from {@code other}'s {@code otherGroupId} into {@code groupId}.
     */
    public void merge(int groupId, IpBucketedSort other, int otherGroupId) {
        var otherBounds = other.getBucketValuesIndexes(otherGroupId);
        var scratch = new BytesRef();

        // TODO: This can be improved for heapified buckets by making use of the heap structures
        for (long i = otherBounds.v1(); i < otherBounds.v2(); i++) {
            collect(other.get(i, scratch), groupId);
        }
    }

    /**
     * Creates a block with the values from the {@code selected} groups.
     */
    public Block toBlock(BlockFactory blockFactory, IntVector selected) {
        // Check if the selected groups are all empty, to avoid allocating extra memory
        if (IntStream.range(0, selected.getPositionCount()).map(selected::getInt).noneMatch(bucket -> {
            var bounds = this.getBucketValuesIndexes(bucket);
            var size = bounds.v2() - bounds.v1();

            return size > 0;
        })) {
            return blockFactory.newConstantNullBlock(selected.getPositionCount());
        }

        // Used to sort the values in the bucket.
        var bucketValues = new BytesRef[bucketSize];

        try (var builder = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
            for (int s = 0; s < selected.getPositionCount(); s++) {
                int bucket = selected.getInt(s);

                var bounds = getBucketValuesIndexes(bucket);
                var size = bounds.v2() - bounds.v1();

                if (size == 0) {
                    builder.appendNull();
                    continue;
                }

                if (size == 1) {
                    builder.appendBytesRef(get(bounds.v1(), scratch1));
                    continue;
                }

                for (int i = 0; i < size; i++) {
                    bucketValues[i] = get(bounds.v1() + i, new BytesRef());
                }

                // TODO: Make use of heap structures to faster iterate in order instead of copying and sorting
                Arrays.sort(bucketValues, 0, (int) size);

                builder.beginPositionEntry();
                if (order == SortOrder.ASC) {
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

    /**
     * Is this bucket a min heap {@code true} or in gathering mode {@code false}?
     */
    private boolean inHeapMode(int bucket) {
        return heapMode.get(bucket);
    }

    /**
     * Get the next index that should be "gathered" for a bucket rooted
     * at {@code rootIndex}.
     * <p>
     *     Using the first 4 bytes of the element to store the next gather offset.
     * </p>
     */
    private int getNextGatherOffset(long rootIndex) {
        values.get(rootIndex * IP_LENGTH, Integer.BYTES, scratch1);
        assert scratch1.length == Integer.BYTES;
        return ByteUtils.readIntLE(scratch1.bytes, scratch1.offset);
    }

    /**
     * Set the next index that should be "gathered" for a bucket rooted
     * at {@code rootIndex}.
     * <p>
     *     Using the first {@code Integer.BYTES} bytes of the element to store the next gather offset.
     * </p>
     */
    private void setNextGatherOffset(long rootIndex, int offset) {
        scratch1.bytes = scratchBytes;
        scratch1.offset = 0;
        scratch1.length = Integer.BYTES;
        ByteUtils.writeIntLE(offset, scratch1.bytes, scratch1.offset);
        values.set(rootIndex * IP_LENGTH, scratch1.bytes, scratch1.offset, scratch1.length);
    }

    /**
     * {@code true} if the entry at index {@code lhs} is "better" than
     * the entry at {@code rhs}. "Better" in this means "lower" for
     * {@link SortOrder#ASC} and "higher" for {@link SortOrder#DESC}.
     */
    private boolean betterThan(BytesRef lhs, BytesRef rhs) {
        return getOrder().reverseMul() * lhs.compareTo(rhs) < 0;
    }

    /**
     * Swap the data at two indices.
     */
    private void swap(long lhs, long rhs) {
        // var tmp = values.get(lhs);
        values.get(lhs * IP_LENGTH, IP_LENGTH, scratch1);
        assert scratch1.length == IP_LENGTH;
        System.arraycopy(scratch1.bytes, scratch1.offset, scratchBytes, 0, scratch1.length);

        // values.set(lhs, values.get(rhs));
        values.get(rhs * IP_LENGTH, IP_LENGTH, scratch2);
        assert scratch2.length == IP_LENGTH;
        values.set(lhs * IP_LENGTH, scratch2.bytes, scratch2.offset, scratch2.length);

        // values.set(rhs, tmp);
        scratch1.bytes = scratchBytes;
        scratch1.offset = 0;
        values.set(rhs * IP_LENGTH, scratch1.bytes, scratch1.offset, scratch1.length);
    }

    /**
     * Allocate storage for more buckets and store the "next gather offset"
     * for those new buckets.
     */
    private void grow(long minSize) {
        long oldMax = values.size() / IP_LENGTH;
        values = bigArrays.grow(values, minSize);
        // Set the next gather offsets for all newly allocated buckets.
        setNextGatherOffsets(oldMax - (oldMax % bucketSize));
    }

    /**
     * Maintain the "next gather offsets" for newly allocated buckets.
     */
    private void setNextGatherOffsets(long startingAt) {
        int nextOffset = bucketSize - 1;
        for (long bucketRoot = startingAt; bucketRoot < values.size() / IP_LENGTH; bucketRoot += bucketSize) {
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
                if (betterThan(get(worstIndex, scratch1), get(leftIndex, scratch2))) {
                    worst = leftChild;
                    worstIndex = leftIndex;
                }
                int rightChild = leftChild + 1;
                long rightIndex = rootIndex + rightChild;
                if (rightChild < bucketSize && betterThan(get(worstIndex, scratch1), get(rightIndex, scratch2))) {
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

    /**
     * Get the IP value at {@code index} and store it in {@code scratch}.
     * Returns {@code scratch}.
     * <p>
     *     {@code index} is an IP index, not a byte index.
     * </p>
     */
    private BytesRef get(long index, BytesRef scratch) {
        values.get(index * IP_LENGTH, IP_LENGTH, scratch);
        assert scratch.length == IP_LENGTH;
        return scratch;
    }

    /**
     * Set the IP value at {@code index}.
     * <p>
     *     {@code index} is an IP index, not a byte index.
     * </p>
     */
    private void set(long index, BytesRef value) {
        assert value.length == IP_LENGTH;
        values.set(index * IP_LENGTH, value.bytes, value.offset, value.length);
    }

    @Override
    public final void close() {
        Releasables.close(values, heapMode);
    }
}
