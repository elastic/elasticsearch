/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class GroupingAggregatorTestCase extends ForkingOperatorTestCase {
    protected abstract GroupingAggregatorFunction.Factory aggregatorFunction();

    protected abstract String expectedDescriptionOfAggregator();

    protected abstract void assertSimpleBucket(Block result, int end, int position, int bucket);

    @Override
    protected SourceOperator simpleInput(int end) {
        return new TupleBlockSourceOperator(LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l % 5, l)));
    }

    @Override
    protected Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode) {
        return new HashAggregationOperator.HashAggregationOperatorFactory(
            0,
            List.of(new GroupingAggregator.GroupingAggregatorFactory(bigArrays, aggregatorFunction(), mode, 1)),
            () -> BlockHash.newLongHash(bigArrays),
            mode
        );
    }

    @Override
    protected final String expectedDescriptionOfSimple() {
        return "HashAggregationOperator(mode = SINGLE, aggs = " + expectedDescriptionOfAggregator() + ")";
    }

    @Override
    protected final void assertSimpleOutput(int end, List<Page> results) {
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(2));
        assertThat(results.get(0).getPositionCount(), equalTo(5));

        Block groups = results.get(0).getBlock(0);
        Block result = results.get(0).getBlock(1);
        for (int i = 0; i < 5; i++) {
            int bucket = (int) groups.getLong(i);
            assertSimpleBucket(result, end, i, bucket);
        }
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        return ByteSizeValue.ofBytes(between(1, 32));
    }
}
