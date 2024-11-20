/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class FloatBucketedSortTests extends BucketedSortTestCase<FloatBucketedSort, Float> {
    @Override
    protected FloatBucketedSort build(SortOrder sortOrder, int bucketSize) {
        return new FloatBucketedSort(bigArrays(), sortOrder, bucketSize);
    }

    @Override
    protected Float randomValue() {
        return randomFloatBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true);
    }

    @Override
    protected List<Float> threeSortedValues() {
        return List.of(-Float.MAX_VALUE, randomFloatBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true), Float.MAX_VALUE);
    }

    @Override
    protected void collect(FloatBucketedSort sort, Float value, int bucket) {
        sort.collect(value, bucket);
    }

    @Override
    protected void merge(FloatBucketedSort sort, int groupId, FloatBucketedSort other, int otherGroupId) {
        sort.merge(groupId, other, otherGroupId);
    }

    @Override
    protected Block toBlock(FloatBucketedSort sort, BlockFactory blockFactory, IntVector selected) {
        return sort.toBlock(blockFactory, selected);
    }

    @Override
    protected void assertBlockTypeAndValues(Block block, List<Float> values) {
        assertThat(block.elementType(), equalTo(ElementType.FLOAT));
        var typedBlock = (FloatBlock) block;
        for (int i = 0; i < values.size(); i++) {
            assertThat(typedBlock.getFloat(i), equalTo(values.get(i)));
        }
    }
}
