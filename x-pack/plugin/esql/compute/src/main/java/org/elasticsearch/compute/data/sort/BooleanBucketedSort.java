/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

import java.util.stream.IntStream;

/**
 * Aggregates the top N boolean values per bucket.
 * This class collects by just keeping the count of true and false values.
 */
public class BooleanBucketedSort implements Releasable {

    private final BigArrays bigArrays;
    private final SortOrder order;
    private final int bucketSize;
    /**
     * An array containing all the values on all buckets. The structure is as follows:
     * <p>
     *     For each bucket, there are 2 values: The first keeps the count of true values, and the second the count of false values.
     * </p>
     */
    private IntArray values;

    public BooleanBucketedSort(BigArrays bigArrays, SortOrder order, int bucketSize) {
        this.bigArrays = bigArrays;
        this.order = order;
        this.bucketSize = bucketSize;

        boolean success = false;
        try {
            values = bigArrays.newIntArray(0, true);
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
    public void collect(boolean value, int bucket) {
        long rootIndex = (long) bucket * 2;

        long requiredSize = rootIndex + 2;
        if (values.size() < requiredSize) {
            grow(requiredSize);
        }

        if (value) {
            values.increment(rootIndex + 1, 1);
        } else {
            values.increment(rootIndex, 1);
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
     * Merge the values from {@code other}'s {@code otherGroupId} into {@code groupId}.
     */
    public void merge(int groupId, BooleanBucketedSort other, int otherGroupId) {
        long otherRootIndex = (long) otherGroupId * 2;

        if (other.values.size() < otherRootIndex + 2) {
            return;
        }

        int falseValues = other.values.get(otherRootIndex);
        int trueValues = other.values.get(otherRootIndex + 1);

        if (falseValues + trueValues == 0) {
            return;
        }

        long rootIndex = (long) groupId * 2;

        long requiredSize = rootIndex + 2;
        if (values.size() < requiredSize) {
            grow(requiredSize);
        }

        values.increment(rootIndex, falseValues);
        values.increment(rootIndex + 1, trueValues);
    }

    /**
     * Creates a block with the values from the {@code selected} groups.
     */
    public Block toBlock(BlockFactory blockFactory, IntVector selected) {
        // Check if the selected groups are all empty, to avoid allocating extra memory
        if (bucketSize == 0 || IntStream.range(0, selected.getPositionCount()).map(selected::getInt).noneMatch(bucket -> {
            long rootIndex = (long) bucket * 2;

            if (values.size() < rootIndex + 2) {
                return false;
            }

            var size = values.get(rootIndex) + values.get(rootIndex + 1);
            return size > 0;
        })) {
            return blockFactory.newConstantNullBlock(selected.getPositionCount());
        }

        try (var builder = blockFactory.newBooleanBlockBuilder(selected.getPositionCount())) {
            for (int s = 0; s < selected.getPositionCount(); s++) {
                int bucket = selected.getInt(s);

                long rootIndex = (long) bucket * 2;

                if (values.size() < rootIndex + 2) {
                    builder.appendNull();
                    continue;
                }

                int falseValues = values.get(rootIndex);
                int trueValues = values.get(rootIndex + 1);
                long totalValues = (long) falseValues + trueValues;

                if (totalValues == 0) {
                    builder.appendNull();
                    continue;
                }

                if (totalValues == 1) {
                    builder.appendBoolean(trueValues > 0);
                    continue;
                }

                builder.beginPositionEntry();
                if (order == SortOrder.ASC) {
                    int falseValuesToAdd = Math.min(falseValues, bucketSize);
                    int trueValuesToAdd = Math.min(trueValues, bucketSize - falseValuesToAdd);
                    for (int i = 0; i < falseValuesToAdd; i++) {
                        builder.appendBoolean(false);
                    }
                    for (int i = 0; i < trueValuesToAdd; i++) {
                        builder.appendBoolean(true);
                    }
                } else {
                    int trueValuesToAdd = Math.min(trueValues, bucketSize);
                    int falseValuesToAdd = Math.min(falseValues, bucketSize - trueValuesToAdd);
                    for (int i = 0; i < trueValuesToAdd; i++) {
                        builder.appendBoolean(true);
                    }
                    for (int i = 0; i < falseValuesToAdd; i++) {
                        builder.appendBoolean(false);
                    }
                }
                builder.endPositionEntry();
            }
            return builder.build();
        }
    }

    /**
     * Allocate storage for more buckets and store the "next gather offset"
     * for those new buckets.
     */
    private void grow(long minSize) {
        values = bigArrays.grow(values, minSize);
    }

    @Override
    public final void close() {
        Releasables.close(values);
    }
}
