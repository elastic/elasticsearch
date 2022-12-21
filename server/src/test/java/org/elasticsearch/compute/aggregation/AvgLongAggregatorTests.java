/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SequenceLongBlockSourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class AvgLongAggregatorTests extends AggregatorTestCase {
    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.AVG_LONG;
    }

    @Override
    public void assertSimpleResult(int end, Block result) {
        double expected = LongStream.range(0, end).mapToDouble(Double::valueOf).sum() / end;
        assertThat(result.getDouble(0), equalTo(expected));
    }

    public void testOverflowFails() {
        try (
            Driver d = new Driver(
                new SequenceLongBlockSourceOperator(LongStream.of(Long.MAX_VALUE - 1, 2)),
                List.of(operator(AggregatorMode.SINGLE)),
                new PageConsumerOperator(page -> fail("shouldn't have made it this far")),
                () -> {}
            )
        ) {
            Exception e = expectThrows(ArithmeticException.class, d::run);
            assertThat(e.getMessage(), equalTo("long overflow"));
        }
    }
}
