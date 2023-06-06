/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.junit.Before;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class PercentileIntGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    private double percentile = 0;

    @Before
    public void initParameters() {
        percentile = randomFrom(0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100);
    }

    @Override
    protected Object[] aggregatorParameters() {
        return new Object[] { percentile };
    }

    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.PERCENTILE_INTS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "percentile of ints";
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        int max = between(1, (int) Math.min(Integer.MAX_VALUE, Long.MAX_VALUE / size));
        return new LongIntBlockSourceOperator(
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), between(-1, max)))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        TDigestState td = new TDigestState(QuantileStates.DEFAULT_COMPRESSION);
        input.stream().flatMapToInt(p -> allInts(p, group)).forEach(td::add);
        double expected = td.quantile(percentile / 100);
        double value = ((DoubleBlock) result).getDouble(position);
        assertThat(value, closeTo(expected, expected * 0.1));
    }
}
