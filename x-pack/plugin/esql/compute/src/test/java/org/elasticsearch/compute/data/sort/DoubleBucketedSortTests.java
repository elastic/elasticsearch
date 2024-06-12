/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.search.sort.SortOrder;

import static org.hamcrest.Matchers.equalTo;

public class DoubleBucketedSortTests extends BucketedSortTestCase<DoubleBucketedSort> {
    @Override
    protected DoubleBucketedSort build(SortOrder sortOrder, int bucketSize) {
        return new DoubleBucketedSort(bigArrays(), sortOrder, bucketSize);
    }

    @Override
    protected Object expectedValue(double v) {
        return v;
    }

    @Override
    protected double randomValue() {
        // 2L^50 fits in the mantisa of a double which the test sort of needs.
        return randomLongBetween(-2L ^ 50, 2L ^ 50);
    }

    @Override
    protected void collect(DoubleBucketedSort sort, double value, int bucket) {
        sort.collect((long) value, bucket);
    }

    @Override
    protected void merge(DoubleBucketedSort sort, int groupId, DoubleBucketedSort other, int otherGroupId) {
        sort.merge(groupId, other, otherGroupId);
    }

    @Override
    protected Block toBlock(DoubleBucketedSort sort, BlockFactory blockFactory, IntVector selected) {
        return sort.toBlock(blockFactory, selected);
    }

    @Override
    protected void assertBlockTypeAndValues(Block block, Object... values) {
        assertThat(block.elementType(), equalTo(ElementType.DOUBLE));
        var typedBlock = (DoubleBlock) block;
        for (int i = 0; i < values.length; i++) {
            assertThat(typedBlock.getDouble(i), equalTo(values[i]));
        }
    }
}
