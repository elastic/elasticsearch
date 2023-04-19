/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.SequenceBooleanBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class CountDistinctBooleanAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(int size) {
        return new SequenceBooleanBlockSourceOperator(LongStream.range(0, size).mapToObj(l -> randomBoolean()).toList());
    }

    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.COUNT_DISTINCT_BOOLEANS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count_distinct of booleans";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        long expected = input.stream()
            .flatMap(
                b -> IntStream.range(0, b.getTotalValueCount())
                    .filter(p -> false == b.isNull(p))
                    .mapToObj(p -> ((BooleanBlock) b).getBoolean(p))
            )
            .distinct()
            .count();

        long count = ((LongBlock) result).getLong(0);
        assertThat(count, equalTo(expected));
    }
}
