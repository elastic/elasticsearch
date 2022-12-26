/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAvgAggregatorTests;
import org.elasticsearch.compute.aggregation.GroupingMaxAggregatorTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class HashAggregationOperatorTests extends ForkingOperatorTestCase {
    @Override
    protected SourceOperator simpleInput(int end) {
        return new TupleBlockSourceOperator(LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l % 5, l)));
    }

    @Override
    protected Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode) {
        return new HashAggregationOperator.HashAggregationOperatorFactory(
            0,
            List.of(
                new GroupingAggregator.GroupingAggregatorFactory(bigArrays, GroupingAggregatorFunction.AVG, mode, 1),
                new GroupingAggregator.GroupingAggregatorFactory(
                    bigArrays,
                    GroupingAggregatorFunction.MAX,
                    mode,
                    mode.isInputPartial() ? 2 : 1
                )
            ),
            () -> BlockHash.newLongHash(bigArrays)
        );
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "HashAggregationOperator(mode = <not-needed>, aggs = avg, max)";
    }

    @Override
    protected void assertSimpleOutput(int end, List<Page> results) {
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(3));
        assertThat(results.get(0).getPositionCount(), equalTo(5));

        GroupingAvgAggregatorTests avg = new GroupingAvgAggregatorTests();
        GroupingMaxAggregatorTests max = new GroupingMaxAggregatorTests();

        Block groups = results.get(0).getBlock(0);
        Block avgs = results.get(0).getBlock(1);
        Block maxs = results.get(0).getBlock(2);
        for (int i = 0; i < 5; i++) {
            int bucket = (int) groups.getLong(i);
            avg.assertSimpleBucket(avgs, end, i, bucket);
            max.assertSimpleBucket(maxs, end, i, bucket);
        }
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        return ByteSizeValue.ofBytes(between(1, 32));
    }
}
