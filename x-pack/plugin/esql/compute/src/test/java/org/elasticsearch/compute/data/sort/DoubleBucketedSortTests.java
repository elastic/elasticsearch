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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DoubleBucketedSortTests extends BucketedSortTestCase<DoubleBucketedSort, Double> {
    @Override
    protected DoubleBucketedSort build(SortOrder sortOrder, int bucketSize) {
        return new DoubleBucketedSort(bigArrays(), sortOrder, bucketSize);
    }

    @Override
    protected Double randomValue() {
        return randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true);
    }

    @Override
    protected List<Double> threeSortedValues() {
        return List.of(-Double.MAX_VALUE, randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true), Double.MAX_VALUE);
    }

    @Override
    protected void collect(DoubleBucketedSort sort, Double value, int bucket) {
        sort.collect(value, bucket);
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
    protected void assertBlockTypeAndValues(Block block, List<Double> values) {
        assertThat(block.elementType(), equalTo(ElementType.DOUBLE));
        var typedBlock = (DoubleBlock) block;
        for (int i = 0; i < values.size(); i++) {
            assertThat(typedBlock.getDouble(i), equalTo(values.get(i)));
        }
    }
}
