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
import org.elasticsearch.compute.operator.SequenceFloatBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;

public class MedianAbsoluteDeviationFloatAggregatorFunctionTests extends AggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        List<Float> values = Arrays.asList(1.2f, 1.25f, 2.0f, 2.0f, 4.3f, 6.0f, 9.0f);
        Randomness.shuffle(values);
        return new SequenceFloatBlockSourceOperator(blockFactory, values.subList(0, Math.min(values.size(), end)));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MedianAbsoluteDeviationFloatAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "median_absolute_deviation of floats";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        assertThat(((DoubleBlock) result).getDouble(0), closeTo(0.8, 0.001d));
    }
}
