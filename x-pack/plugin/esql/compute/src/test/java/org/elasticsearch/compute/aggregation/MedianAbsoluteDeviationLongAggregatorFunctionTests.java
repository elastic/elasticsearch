/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MedianAbsoluteDeviationLongAggregatorFunctionTests extends AggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        List<Long> values = Arrays.asList(12L, 125L, 20L, 20L, 43L, 60L, 90L);
        Randomness.shuffle(values);
        return new SequenceLongBlockSourceOperator(blockFactory, values.subList(0, Math.min(values.size(), end)));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MedianAbsoluteDeviationLongAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "median_absolute_deviation of longs";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(23.0));
    }
}
