/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class GroupingAggregatorTestCase extends OperatorTestCase {
    protected abstract GroupingAggregatorFunction.GroupingAggregatorFunctionFactory aggregatorFunction();

    protected abstract void assertSimpleBucket(Block result, int end, int bucket);

    @Override
    protected SourceOperator simpleInput(int end) {
        return new TupleBlockSourceOperator(LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l % 5, l)));
    }

    @Override
    protected final Operator simple(BigArrays bigArrays) {
        return operator(bigArrays, AggregatorMode.SINGLE);
    }

    @Override
    protected final void assertSimpleOutput(int end, List<Page> results) {
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(2));
        assertThat(results.get(0).getPositionCount(), equalTo(5));

        Block groups = results.get(0).getBlock(0);
        Block result = results.get(0).getBlock(1);
        assertThat(groups.getLong(0), equalTo(0L));
        assertSimpleBucket(result, end, 0);
        assertThat(groups.getLong(1), equalTo(1L));
        assertSimpleBucket(result, end, 1);
        assertThat(groups.getLong(2), equalTo(2L));
        assertSimpleBucket(result, end, 2);
        assertThat(groups.getLong(3), equalTo(3L));
        assertSimpleBucket(result, end, 3);
        assertThat(groups.getLong(4), equalTo(4L));
        assertSimpleBucket(result, end, 4);
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        return ByteSizeValue.ofBytes(between(1, 32));
    }

    public void testInitialFinal() {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        try (
            Driver d = new Driver(
                simpleInput(end),
                List.of(operator(bigArrays, AggregatorMode.INITIAL), operator(bigArrays, AggregatorMode.FINAL)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    public void testInitialIntermediateFinal() {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        try (
            Driver d = new Driver(
                simpleInput(end),
                List.of(
                    operator(bigArrays, AggregatorMode.INITIAL),
                    operator(bigArrays, AggregatorMode.INTERMEDIATE),
                    operator(bigArrays, AggregatorMode.FINAL)
                ),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    private Operator operator(BigArrays bigArrays, AggregatorMode mode) {
        return new HashAggregationOperator(
            0,
            List.of(new GroupingAggregator.GroupingAggregatorFactory(bigArrays, aggregatorFunction(), mode, 1)),
            () -> BlockHash.newLongHash(bigArrays)
        );
    }
}
