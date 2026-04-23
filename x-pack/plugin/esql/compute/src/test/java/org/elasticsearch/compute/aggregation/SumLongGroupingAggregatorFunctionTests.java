/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class SumLongGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of longs";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size / 5);
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLongBetween(-max, max)))
        );
    }

    @Override
    public void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        long sum = input.stream().flatMapToLong(p -> allLongs(p, group)).sum();
        assertThat(((LongBlock) result).getLong(position), equalTo(sum));
    }

    /**
     * When one group overflows, that group gets null and a warning; other groups get correct sums.
     * Uses 3 groups (working, failing, working) to exercise the full flow.
     */
    public void testOverflowInGroupingProducesNullAndWarning() {
        List<Page> results = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        DriverContext driverContext = driverContext();
        // Group 0: 1 + 2 (works)
        // Group 1: Long.MAX_VALUE-1 + 2 (overflows)
        // Group 2: 4 + 5 = 9 (works).
        List<Page> input = CannedSourceOperator.collectPages(
            new TupleLongLongBlockSourceOperator(
                driverContext.blockFactory(),
                List.of(
                    Tuple.tuple(0L, 1L),
                    Tuple.tuple(0L, 2L),
                    Tuple.tuple(1L, Long.MAX_VALUE - 1),
                    Tuple.tuple(1L, 2L),
                    Tuple.tuple(2L, 4L),
                    Tuple.tuple(2L, 5L)
                )
            )
        );
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(input.iterator()),
                List.of(simpleWithMode(AggregatorMode.SINGLE).get(driverContext)),
                new TestResultPageSinkOperator(results::add),
                () -> warnings.addAll(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()))
            )
        ) {
            new TestDriverRunner().run(driver);
        }

        assertDriverContext(driverContext);
        assertThat(results.size(), equalTo(1));
        Page page = results.get(0);
        assertThat(page.getBlockCount(), equalTo(2));
        assertThat(page.getPositionCount(), equalTo(3));

        LongBlock groupsBlock = (LongBlock) page.getBlock(0);
        LongBlock sumBlock = (LongBlock) page.getBlock(1);
        for (int i = 0; i < 3; i++) {
            long group = groupsBlock.getLong(i);
            if (group == 0L) {
                assertThat(sumBlock.getLong(i), equalTo(3L));
            } else if (group == 1L) {
                assertThat(sumBlock.isNull(i), equalTo(true));
            } else if (group == 2L) {
                assertThat(sumBlock.getLong(i), equalTo(9L));
            } else {
                fail("unexpected group " + group);
            }
        }

        assertThat(warnings, hasItem(containsString("long overflow")));
    }
}
