/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAvgAggregatorTests;
import org.elasticsearch.compute.aggregation.GroupingMaxAggregatorTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class HashAggregationOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(int end) {
        return new TupleBlockSourceOperator(LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l % 5, l)));
    }

    @Override
    protected Operator simple(BigArrays bigArrays) {
        return operator(bigArrays, AggregatorMode.SINGLE, 1, 1);
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
        assertThat(groups.getLong(0), equalTo(0L));
        avg.assertSimpleBucket(avgs, end, 0);
        max.assertSimpleBucket(maxs, end, 0);
        assertThat(groups.getLong(1), equalTo(1L));
        avg.assertSimpleBucket(avgs, end, 1);
        max.assertSimpleBucket(maxs, end, 1);
        assertThat(groups.getLong(2), equalTo(2L));
        avg.assertSimpleBucket(avgs, end, 2);
        max.assertSimpleBucket(maxs, end, 2);
        assertThat(groups.getLong(3), equalTo(3L));
        avg.assertSimpleBucket(avgs, end, 3);
        max.assertSimpleBucket(maxs, end, 3);
        assertThat(groups.getLong(4), equalTo(4L));
        avg.assertSimpleBucket(avgs, end, 4);
        max.assertSimpleBucket(maxs, end, 4);
    }

    public void testInitialFinal() {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        try (
            Driver d = new Driver(
                simpleInput(end),
                List.of(operator(bigArrays, AggregatorMode.INITIAL, 1, 1), operator(bigArrays, AggregatorMode.FINAL, 1, 2)),
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
                    operator(bigArrays, AggregatorMode.INITIAL, 1, 1),
                    operator(bigArrays, AggregatorMode.INTERMEDIATE, 1, 2),
                    operator(bigArrays, AggregatorMode.FINAL, 1, 2)
                ),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    private Operator operator(BigArrays bigArrays, AggregatorMode mode, int channel1, int channel2) {
        return new HashAggregationOperator(
            0,
            List.of(
                new GroupingAggregator.GroupingAggregatorFactory(bigArrays, GroupingAggregatorFunction.avg, mode, channel1),
                new GroupingAggregator.GroupingAggregatorFactory(bigArrays, GroupingAggregatorFunction.max, mode, channel2)
            ),
            () -> BlockHash.newLongHash(bigArrays)
        );
    }
}
