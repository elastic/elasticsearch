/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

// begin generated imports
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
$if(hasExtra && !(long && Extralong))$
import org.elasticsearch.common.util.$ExtraType$Array;
$endif$
import org.elasticsearch.common.util.$Type$Array;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.util.stream.IntStream;
// end generated imports

/**
 * Aggregates the top N {@code $type$} values per bucket.
 * See {@link BucketedSort} for more information.
 * This class is generated. Edit @{code X-BucketedSort.java.st} instead of this file.
 */
public class $Name$$ExtraName$BucketedSort implements Releasable {

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
$if(hasExtra)$
    private $Array$ values;
    private $ExtraArray$ extraValues;

$else$
    private $Array$ values;
$endif$

    public $Name$$ExtraName$BucketedSort(BigArrays bigArrays, SortOrder order, int bucketSize) {
        this.bigArrays = bigArrays;
        this.order = order;
        this.bucketSize = bucketSize;
        heapMode = new BitArray(0, bigArrays);

        boolean success = false;
        try {
            values = bigArrays.new$Type$Array(0, false);
$if(hasExtra)$
            extraValues = bigArrays.new$ExtraType$Array(0, false);
$endif$
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
    public void collect($type$ value, $if(hasExtra)$$Extratype$ extraValue, $endif$int bucket) {
        long rootIndex = (long) bucket * bucketSize;
        if (inHeapMode(bucket)) {
            if (betterThan(value, values.get(rootIndex)$if(hasExtra)$, extraValue, extraValues.get(rootIndex)$endif$)) {
                values.set(rootIndex, value);
$if(hasExtra)$
                extraValues.set(rootIndex, extraValue);
$endif$
                downHeap(rootIndex, 0, bucketSize);
            }
            return;
        }
        // Gathering mode
        long requiredSize = rootIndex + bucketSize;
        if (values.size() < requiredSize) {
            grow(bucket);
        }
        int next = getNextGatherOffset(rootIndex);
        assert 0 <= next && next < bucketSize
            : "Expected next to be in the range of valid buckets [0 <= " + next + " < " + bucketSize + "]";
        long index = next + rootIndex;
        values.set(index, value);
$if(hasExtra)$
        extraValues.set(index, extraValue);
$endif$
        if (next == 0) {
            heapMode.set(bucket);
            heapify(rootIndex, bucketSize);
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
        if (rootIndex >= values.size()) {
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
    public void merge(int groupId, $Name$$ExtraName$BucketedSort other, int otherGroupId) {
        var otherBounds = other.getBucketValuesIndexes(otherGroupId);

        // TODO: This can be improved for heapified buckets by making use of the heap structures
        for (long i = otherBounds.v1(); i < otherBounds.v2(); i++) {
            collect(other.values.get(i), $if(hasExtra)$other.extraValues.get(i), $endif$groupId);
        }
    }

$if(hasExtra)$
    /**
     * Creates a block with the values from the {@code selected} groups.
     */
    public void toBlocks(BlockFactory blockFactory, Block[] blocks, int offset, IntVector selected) {
        // Check if the selected groups are all empty, to avoid allocating extra memory
        if (allSelectedGroupsAreEmpty(selected)) {
            Block constantNullBlock = blockFactory.newConstantNullBlock(selected.getPositionCount());
            constantNullBlock.incRef();
            blocks[offset] = constantNullBlock;
            blocks[offset + 1] = constantNullBlock;
            return;
        }

        try (
            var builder = blockFactory.new$Type$BlockBuilder(selected.getPositionCount());
            var extraBuilder = blockFactory.new$ExtraType$BlockBuilder(selected.getPositionCount())
        ) {
            for (int s = 0; s < selected.getPositionCount(); s++) {
                int bucket = selected.getInt(s);

                var bounds = getBucketValuesIndexes(bucket);
                var rootIndex = bounds.v1();
                var size = bounds.v2() - bounds.v1();

                if (size == 0) {
                    builder.appendNull();
                    extraBuilder.appendNull();
                    continue;
                }

                if (size == 1) {
                    builder.append$Type$(values.get(rootIndex));
                    extraBuilder.append$ExtraType$(extraValues.get(rootIndex));
                    continue;
                }

                // If we are in the gathering mode, we need to heapify before sorting.
                if (inHeapMode(bucket) == false) {
                    heapify(rootIndex, (int) size);
                }
                heapSort(rootIndex, (int) size);

                builder.beginPositionEntry();
                extraBuilder.beginPositionEntry();
                for (int i = 0; i < size; i++) {
                    builder.append$Type$(values.get(rootIndex + i));
                    extraBuilder.append$ExtraType$(extraValues.get(rootIndex + i));
                }
                builder.endPositionEntry();
                extraBuilder.endPositionEntry();
            }
            blocks[offset] = builder.build();
            blocks[offset + 1] = extraBuilder.build();
        }
    }

$else$
    /**
     * Creates a block with the values from the {@code selected} groups.
     */
    public Block toBlock(BlockFactory blockFactory, IntVector selected) {
        // Check if the selected groups are all empty, to avoid allocating extra memory
        if (allSelectedGroupsAreEmpty(selected)) {
            return blockFactory.newConstantNullBlock(selected.getPositionCount());
        }

        try (var builder = blockFactory.new$Type$BlockBuilder(selected.getPositionCount())) {
            for (int s = 0; s < selected.getPositionCount(); s++) {
                int bucket = selected.getInt(s);

                var bounds = getBucketValuesIndexes(bucket);
                var rootIndex = bounds.v1();
                var size = bounds.v2() - bounds.v1();

                if (size == 0) {
                    builder.appendNull();
                    continue;
                }

                if (size == 1) {
                    builder.append$Type$(values.get(rootIndex));
                    continue;
                }

                // If we are in the gathering mode, we need to heapify before sorting.
                if (inHeapMode(bucket) == false) {
                    heapify(rootIndex, (int) size);
                }
                heapSort(rootIndex, (int) size);

                builder.beginPositionEntry();
                for (int i = 0; i < size; i++) {
                    builder.append$Type$(values.get(rootIndex + i));
                }
                builder.endPositionEntry();
            }
            return builder.build();
        }
    }
$endif$

    /**
     * Checks if the selected groups are all empty.
     */
    private boolean allSelectedGroupsAreEmpty(IntVector selected) {
        return IntStream.range(0, selected.getPositionCount()).map(selected::getInt).noneMatch(bucket -> {
            var bounds = this.getBucketValuesIndexes(bucket);
            var size = bounds.v2() - bounds.v1();
            return size > 0;
        });
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
     */
    private int getNextGatherOffset(long rootIndex) {
$if(int)$
        return values.get(rootIndex);
$else$
        return (int) values.get(rootIndex);
$endif$
    }

    /**
     * Set the next index that should be "gathered" for a bucket rooted
     * at {@code rootIndex}.
     */
    private void setNextGatherOffset(long rootIndex, int offset) {
        values.set(rootIndex, offset);
    }

    /**
     * {@code true} if the entry at index {@code lhs} is "better" than
     * the entry at {@code rhs}. "Better" in this means "lower" for
     * {@link SortOrder#ASC} and "higher" for {@link SortOrder#DESC}.
     */
    private boolean betterThan($type$ lhs, $type$ rhs$if(hasExtra)$, $Extratype$ lhsExtra, $Extratype$ rhsExtra$endif$) {
        int res = $Wrapper$.compare(lhs, rhs);
$if(hasExtra)$
        if (res != 0) {
            return getOrder().reverseMul() * res < 0;
        }
        res = $ExtraWrapper$.compare(lhsExtra, rhsExtra);
$endif$
        return getOrder().reverseMul() * res < 0;
    }

    /**
     * Swap the data at two indices.
     */
    private void swap(long lhs, long rhs) {
        var tmp = values.get(lhs);
        values.set(lhs, values.get(rhs));
        values.set(rhs, tmp);
$if(hasExtra)$
        var tmpExtra = extraValues.get(lhs);
        extraValues.set(lhs, extraValues.get(rhs));
        extraValues.set(rhs, tmpExtra);
$endif$
    }

    /**
     * Allocate storage for more buckets and store the "next gather offset"
     * for those new buckets. We always grow the storage by whole bucket's
     * worth of slots at a time. We never allocate space for partial buckets.
     */
    private void grow(int bucket) {
        long oldMax = values.size();
        assert oldMax % bucketSize == 0;

        long newSize = BigArrays.overSize(((long) bucket + 1) * bucketSize, PageCacheRecycler.$TYPE$_PAGE_SIZE, $BYTES$);
        // Round up to the next full bucket.
        newSize = (newSize + bucketSize - 1) / bucketSize;
        values = bigArrays.resize(values, newSize * bucketSize);
$if(hasExtra)$
        // Round up to the next full bucket.
        extraValues = bigArrays.resize(extraValues, newSize * bucketSize);
$endif$
        // Set the next gather offsets for all newly allocated buckets.
        fillGatherOffsets(oldMax);
    }

    /**
     * Maintain the "next gather offsets" for newly allocated buckets.
     */
    private void fillGatherOffsets(long startingAt) {
        int nextOffset = getBucketSize() - 1;
        for (long bucketRoot = startingAt; bucketRoot < values.size(); bucketRoot += getBucketSize()) {
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
    private void heapify(long rootIndex, int heapSize) {
        int maxParent = heapSize / 2 - 1;
        for (int parent = maxParent; parent >= 0; parent--) {
            downHeap(rootIndex, parent, heapSize);
        }
    }

    /**
     * Sorts all the values in the heap using heap sort algorithm.
     * This runs in {@code O(n log n)} time.
     * @param rootIndex index of the start of the bucket
     * @param heapSize Number of values that belong to the heap.
     *                 Can be less than bucketSize.
     *                 In such a case, the remaining values in range
     *                 (rootIndex + heapSize, rootIndex + bucketSize)
     *                 are *not* considered part of the heap.
     */
    private void heapSort(long rootIndex, int heapSize) {
        while (heapSize > 0) {
            swap(rootIndex, rootIndex + heapSize - 1);
            heapSize--;
            downHeap(rootIndex, 0, heapSize);
        }
    }

    /**
     * Correct the heap invariant of a parent and its children. This
     * runs in {@code O(log n)} time.
     * @param rootIndex index of the start of the bucket
     * @param parent Index within the bucket of the parent to check.
     *               For example, 0 is the "root".
     * @param heapSize Number of values that belong to the heap.
     *                 Can be less than bucketSize.
     *                 In such a case, the remaining values in range
     *                 (rootIndex + heapSize, rootIndex + bucketSize)
     *                 are *not* considered part of the heap.
     */
    private void downHeap(long rootIndex, int parent, int heapSize) {
        while (true) {
            long parentIndex = rootIndex + parent;
            int worst = parent;
            long worstIndex = parentIndex;
            int leftChild = parent * 2 + 1;
            long leftIndex = rootIndex + leftChild;
            if (leftChild < heapSize) {
                if (betterThan(values.get(worstIndex), values.get(leftIndex)$if(hasExtra)$, extraValues.get(worstIndex), extraValues.get(leftIndex)$endif$)) {
                    worst = leftChild;
                    worstIndex = leftIndex;
                }
                int rightChild = leftChild + 1;
                long rightIndex = rootIndex + rightChild;
$if(hasExtra)$
                if (rightChild < heapSize
                    && betterThan(
                        values.get(worstIndex),
                        values.get(rightIndex),
                        extraValues.get(worstIndex),
                        extraValues.get(rightIndex)
                    )) {
$else$
                if (rightChild < heapSize && betterThan(values.get(worstIndex), values.get(rightIndex))) {
$endif$
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
        Releasables.close(values, $if(hasExtra)$extraValues, $endif$heapMode);
    }
}
