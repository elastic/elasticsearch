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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class IntBucketedSortTests extends BucketedSortTestCase<IntBucketedSort, Integer> {
    @Override
    protected IntBucketedSort build(SortOrder sortOrder, int bucketSize) {
        return new IntBucketedSort(bigArrays(), sortOrder, bucketSize);
    }

    @Override
    protected Integer randomValue() {
        return randomInt();
    }

    @Override
    protected List<Integer> threeSortedValues() {
        return List.of(Integer.MIN_VALUE, randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE), Integer.MAX_VALUE);
    }

    @Override
    protected void collect(IntBucketedSort sort, Integer value, int bucket) {
        sort.collect(value, bucket);
    }

    @Override
    protected void merge(IntBucketedSort sort, int groupId, IntBucketedSort other, int otherGroupId) {
        sort.merge(groupId, other, otherGroupId);
    }

    @Override
    protected Block toBlock(IntBucketedSort sort, BlockFactory blockFactory, IntVector selected) {
        return sort.toBlock(blockFactory, selected);
    }

    @Override
    protected void assertBlockTypeAndValues(Block block, List<Integer> values) {
        assertThat(block.elementType(), equalTo(ElementType.INT));
        var typedBlock = (IntBlock) block;
        for (int i = 0; i < values.size(); i++) {
            assertThat(typedBlock.getInt(i), equalTo(values.get(i)));
        }
    }
}
