/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class PartitionedAggregationBlockTests extends ComputeTestCase {

    public void testSimple() {
        BlockFactory blockFactory = blockFactory();
        int numKeys = randomIntBetween(1, 100);
        var block = new PartitionedAggregationBlock(
            blockFactory,
            numKeys,
            new TestKeys(blockFactory.breaker()),
            new GroupingAggregatorFunction.PartitionedState[] { new TestAggState(blockFactory.breaker()) }
        );
        assertThat(block.getPositionCount(), equalTo(numKeys));
        assertThat(block.elementType(), equalTo(ElementType.UNKNOWN));
        var keys = block.takeKeys();
        var aggs = block.takeAggs();
        assertNull(block.keys());
        assertNull(block.aggs());
        block.close();
        assertThat(blockFactory.breaker().getUsed(), greaterThan(0L));
        keys.releaseAll(blockFactory.breaker());
        for (var agg : aggs) {
            agg.releaseAll(blockFactory.breaker());
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    static class TestKeys implements PartitionedHashTable.PartitionedHashKeys {
        final int usedBytes;

        TestKeys(CircuitBreaker breaker) {
            usedBytes = randomIntBetween(1, 1024);
            breaker.addEstimateBytesAndMaybeBreak(usedBytes, "test");
        }

        @Override
        public int keysInPartition(int partition) {
            return 0;
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {

        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            breaker.addWithoutBreaking(-usedBytes);
        }
    }

    static class TestAggState implements GroupingAggregatorFunction.PartitionedState {
        final int usedBytes;

        TestAggState(CircuitBreaker breaker) {
            usedBytes = randomIntBetween(1, 1024);
            breaker.addEstimateBytesAndMaybeBreak(usedBytes, "test");
        }

        @Override
        public boolean hasAllValues(int partition) {
            return true;
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {

        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            breaker.addWithoutBreaking(-usedBytes);
        }
    }
}
