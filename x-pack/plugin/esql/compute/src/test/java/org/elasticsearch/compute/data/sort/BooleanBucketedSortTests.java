/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.search.sort.SortOrder;

import static org.hamcrest.Matchers.equalTo;

public class BooleanBucketedSortTests extends BucketedSortTestCase<BooleanBucketedSort> {
    @Override
    protected BooleanBucketedSort build(SortOrder sortOrder, int bucketSize) {
        return new BooleanBucketedSort(bigArrays(), sortOrder, bucketSize);
    }

    @Override
    protected Object expectedValue(double v) {
        return toBoolean(v);
    }

    @Override
    protected double randomValue() {
        return randomBoolean() ? 0d : 1d;
    }

    @Override
    protected void collect(BooleanBucketedSort sort, double value, int bucket) {
        sort.collect(toBoolean(value), bucket);
    }

    @Override
    protected void merge(BooleanBucketedSort sort, int groupId, BooleanBucketedSort other, int otherGroupId) {
        sort.merge(groupId, other, otherGroupId);
    }

    @Override
    protected Block toBlock(BooleanBucketedSort sort, BlockFactory blockFactory, IntVector selected) {
        return sort.toBlock(blockFactory, selected);
    }

    @Override
    protected void assertBlockTypeAndValues(Block block, Object... values) {
        assertThat(block.elementType(), equalTo(ElementType.BOOLEAN));
        var typedBlock = (BooleanBlock) block;
        for (int i = 0; i < values.length; i++) {
            assertThat(typedBlock.getBoolean(i), equalTo(values[i]));
        }
    }

    private boolean toBoolean(double value) {
        return value > 0;
    }
}
