/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.operator.SequenceDoubleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class AvgDoubleAggregatorTests extends AggregatorTestCase {
    @Override
    protected SourceOperator simpleInput(int end) {
        return new SequenceDoubleBlockSourceOperator(LongStream.range(0, end).asDoubleStream());
    }

    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.AVG_DOUBLES;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "avg of doubles";
    }

    @Override
    protected void assertSimpleResult(int end, Block result) {
        double expected = LongStream.range(0, end).mapToDouble(Double::valueOf).sum() / end;
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(expected));
    }
}
