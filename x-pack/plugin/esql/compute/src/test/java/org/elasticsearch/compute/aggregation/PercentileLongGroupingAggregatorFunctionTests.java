/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.junit.Before;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class PercentileLongGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    private double percentile = 0;

    @Before
    public void initParameters() {
        percentile = randomFrom(0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new PercentileLongAggregatorFunctionSupplier(inputChannels, percentile);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "percentile of longs";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size / 5);
        return new TupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLongBetween(-0, max)))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        TDigestState td = TDigestState.create(QuantileStates.DEFAULT_COMPRESSION);
        input.stream().flatMapToLong(p -> allLongs(p, group)).forEach(td::add);
        if (td.size() > 0) {
            double expected = td.quantile(percentile / 100);
            double value = ((DoubleBlock) result).getDouble(position);
            assertThat(value, closeTo(expected, expected * 0.1));
        } else {
            assertTrue(result.isNull(position));
        }
    }
}
