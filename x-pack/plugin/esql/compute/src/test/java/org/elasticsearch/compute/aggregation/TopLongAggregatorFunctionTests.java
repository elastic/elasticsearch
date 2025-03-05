/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.contains;

public class TopLongAggregatorFunctionTests extends AggregatorFunctionTestCase {
    private static final int LIMIT = 100;

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLong()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new TopLongAggregatorFunctionSupplier(LIMIT, true);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "top of longs";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        Object[] values = input.stream().flatMapToLong(b -> allLongs(b)).sorted().limit(LIMIT).boxed().toArray(Object[]::new);
        assertThat((List<?>) BlockUtils.toJavaObject(result, 0), contains(values));
    }
}
