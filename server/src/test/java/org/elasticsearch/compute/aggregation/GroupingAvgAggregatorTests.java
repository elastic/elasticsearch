/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class GroupingAvgAggregatorTests extends ESTestCase {
    public void testNoBreaking() {
        assertSimple(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofKb(1)));
    }

    public void testWithCranky() {
        AggregatorTestCase.CrankyCircuitBreakerService breaker = new AggregatorTestCase.CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker).withCircuitBreaking();
        try {
            assertSimple(bigArrays);
            // Either we get lucky and cranky doesn't throw and the test completes or we don't and it throws
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(AggregatorTestCase.CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    public void testCircuitBreaking() {
        Exception e = expectThrows(
            CircuitBreakingException.class,
            () -> assertSimple(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(between(1, 32))))
        );
        assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
    }

    private void assertSimple(BigArrays bigArrays) {
        try (GroupingAvgAggregator agg = GroupingAvgAggregator.create(bigArrays.withCircuitBreaking(), 0)) {
            int[] groups = new int[] { 0, 1, 2, 1, 2, 3 };
            double[] values = new double[] { 1, 2, 3, 4, 5, 6 };
            agg.addRawInput(new IntArrayBlock(groups, groups.length), new Page(new DoubleArrayBlock(values, values.length)));
            Block avgs = agg.evaluateFinal();
            assertThat(avgs.getDouble(0), equalTo(1.0));
            assertThat(avgs.getDouble(1), equalTo(3.0));
            assertThat(avgs.getDouble(2), equalTo(4.0));
            assertThat(avgs.getDouble(3), equalTo(6.0));
        }
    }
}
