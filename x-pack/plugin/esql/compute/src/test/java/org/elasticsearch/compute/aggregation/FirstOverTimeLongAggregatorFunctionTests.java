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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class FirstOverTimeLongAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLong(), randomLong()))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new FirstOverTimeLongAggregatorFunctionSupplier();
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "first_over_time of longs";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        long v = (Long) BlockUtils.toJavaObject(result, 0);
        boolean first = true;
        long expectedTimestamp = 0;
        long expected = 0;
        for (Page page : input) {
            LongVector values = page.<LongBlock>getBlock(0).asVector();
            LongVector timestamps = page.<LongBlock>getBlock(1).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                long timestamp = timestamps.getLong(p);
                long value = values.getLong(p);
                if (first) {
                    first = false;
                    expectedTimestamp = timestamp;
                    expected = value;
                } else if (timestamp < expectedTimestamp) {
                    expectedTimestamp = timestamp;
                    expected = value;
                } else if (timestamp == expectedTimestamp && value < expected) {
                    expected = value;
                }
            }
        }
        assertThat(v, equalTo(expected));
    }
}
