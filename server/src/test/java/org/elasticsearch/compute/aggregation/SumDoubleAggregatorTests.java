/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SequenceLongBlockSourceOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class SumDoubleAggregatorTests extends AggregatorTestCase {
    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.SUM_DOUBLES;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of doubles";
    }

    @Override
    protected void assertSimpleResult(int end, Block result) {
        double expected = LongStream.range(0, end).mapToDouble(Double::valueOf).sum();
        assertThat(result.getDouble(0), equalTo(expected));
    }

    public void testLongOverflowSucceeds() {
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                new SequenceLongBlockSourceOperator(LongStream.of(Long.MAX_VALUE - 1, 2)),
                List.of(simple(nonBreakingBigArrays()).get()),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertThat(results.get(0).getBlock(0).getDouble(0), equalTo((double) Long.MAX_VALUE + 1));
    }
}
