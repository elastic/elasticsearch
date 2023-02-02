/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.operator.SequenceIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class AvgIntAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(int size) {
        int max = between(1, (int) Math.min(Integer.MAX_VALUE, Long.MAX_VALUE / size));
        return new SequenceIntBlockSourceOperator(LongStream.range(0, size).mapToInt(l -> between(-max, max)));
    }

    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.AVG_INTS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "avg of ints";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        long sum = input.stream()
            .flatMapToLong(
                b -> IntStream.range(0, b.getTotalValueCount())
                    .filter(p -> false == b.isNull(p))
                    .mapToLong(p -> (long) ((IntBlock) b).getInt(p))
            )
            .sum();
        long count = input.stream().flatMapToInt(b -> IntStream.range(0, b.getPositionCount()).filter(p -> false == b.isNull(p))).count();
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(((double) sum) / count));
    }
}
