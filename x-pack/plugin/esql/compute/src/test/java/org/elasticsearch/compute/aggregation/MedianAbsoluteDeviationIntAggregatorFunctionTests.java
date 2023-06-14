/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.operator.SequenceIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MedianAbsoluteDeviationIntAggregatorFunctionTests extends AggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(int end) {
        List<Integer> values = Arrays.asList(12, 125, 20, 20, 43, 60, 90);
        Randomness.shuffle(values);
        return new SequenceIntBlockSourceOperator(values);
    }

    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.MEDIAN_ABSOLUTE_DEVIATION_INTS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "median_absolute_deviation of ints";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(23.0));
    }
}
