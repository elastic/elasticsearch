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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LongBucketedSortTests extends BucketedSortTestCase<LongBucketedSort, Long> {
    @Override
    protected LongBucketedSort build(SortOrder sortOrder, int bucketSize) {
        return new LongBucketedSort(bigArrays(), sortOrder, bucketSize);
    }

    @Override
    protected Long randomValue() {
        return randomLong();
    }

    @Override
    protected List<Long> threeSortedValues() {
        return List.of(Long.MIN_VALUE, randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE), Long.MAX_VALUE);
    }

    @Override
    protected void collect(LongBucketedSort sort, Long value, int bucket) {
        sort.collect(value, bucket);
    }

    @Override
    protected void merge(LongBucketedSort sort, int groupId, LongBucketedSort other, int otherGroupId) {
        sort.merge(groupId, other, otherGroupId);
    }

    @Override
    protected Block toBlock(LongBucketedSort sort, BlockFactory blockFactory, IntVector selected) {
        return sort.toBlock(blockFactory, selected);
    }

    @Override
    protected void assertBlockTypeAndValues(Block block, List<Long> values) {
        assertThat(block.elementType(), equalTo(ElementType.LONG));
        var typedBlock = (LongBlock) block;
        for (int i = 0; i < values.size(); i++) {
            assertThat(typedBlock.getLong(i), equalTo(values.get(i)));
        }
    }
}
