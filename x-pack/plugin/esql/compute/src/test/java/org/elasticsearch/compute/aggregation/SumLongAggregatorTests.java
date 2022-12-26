/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SequenceLongBlockSourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class SumLongAggregatorTests extends AggregatorTestCase {
    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.SUM_LONGS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of longs";
    }

    @Override
    protected void assertSimpleResult(int end, Block result) {
        assertThat(result.getLong(0), equalTo(LongStream.range(0, end).sum()));
    }

    public void testOverflowFails() {
        try (
            Driver d = new Driver(
                new SequenceLongBlockSourceOperator(LongStream.of(Long.MAX_VALUE - 1, 2)),
                List.of(simple(nonBreakingBigArrays()).get()),
                new PageConsumerOperator(page -> fail("shouldn't have made it this far")),
                () -> {}
            )
        ) {
            Exception e = expectThrows(ArithmeticException.class, d::run);
            assertThat(e.getMessage(), equalTo("long overflow"));
        }
    }

    public void testRejectsDouble() {
        try (
            Driver d = new Driver(
                new CannedSourceOperator(Iterators.single(new Page(new DoubleVector(new double[] { 1.0 }, 1).asBlock()))),
                List.of(simple(nonBreakingBigArrays()).get()),
                new PageConsumerOperator(page -> fail("shouldn't have made it this far")),
                () -> {}
            )
        ) {
            expectThrows(UnsupportedOperationException.class, d::run);
        }
    }
}
