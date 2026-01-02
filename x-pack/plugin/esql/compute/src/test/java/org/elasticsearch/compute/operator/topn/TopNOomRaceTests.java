/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Runs a few topns in parallel trying to cause an OOM. This is parameterized because
 * we'd like for it to run a bunch of times on each test execution AND because we'd like
 * to see if failures at different points are more likely.
 */
public class TopNOomRaceTests extends ESTestCase {
    @ParametersFactory(argumentFormatting = "repeats = %s")
    public static List<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (int repeats = 5; repeats <= 100; repeats += 5) {
            parameters.add(new Object[] { repeats });
        }
        return parameters;
    }

    @Rule(order = Integer.MIN_VALUE)
    public static final AnyFailedWatcher watcher = new AnyFailedWatcher();
    private final int repeats;

    public TopNOomRaceTests(int repeats) {
        this.repeats = repeats;
    }

    public void testRace() {
        assumeFalse("previous test failed", watcher.anyFailed);
        int driverCount = 6;
        double usedByEachThread = .3;
        ByteSizeValue limit = ByteSizeValue.ofBytes(Runtime.getRuntime().freeMemory());
        int approxSizePerFilledRow = RamUsageEstimator.NUM_BYTES_OBJECT_REF * 5 + repeats * Integer.BYTES;
        int topCount = (int) ((double) limit.getBytes() / approxSizePerFilledRow * usedByEachThread) - 1;
        int maxInput = topCount * 3;
        assertThat(topCount, greaterThan(0));

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit);
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        List<DriverContext> contexts = new ArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        for (int d = 0; d < driverCount; d++) {
            MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);
            DriverContext driverContext = new DriverContext(bigArrays, blockFactory);
            contexts.add(driverContext);

            SourceOperator source = new AbstractBlockSourceOperator(blockFactory, 1024) {
                int lastEnd = 0;

                @Override
                protected int remaining() {
                    return maxInput - lastEnd;
                }

                @Override
                protected Page createPage(int positionOffset, int length) {
                    lastEnd = positionOffset + length;
                    Block ints = IntVector.range(positionOffset, lastEnd, blockFactory).asBlock();
                    Block[] blocks = new Block[repeats];
                    blocks[0] = ints;
                    for (int b = 1; b < repeats; b++) {
                        ints.incRef();
                        blocks[b] = ints;
                    }
                    return new Page(blocks);
                }
            };
            TopNOperator topn = new TopNOperator(
                blockFactory,
                breaker,
                topCount,
                IntStream.range(0, repeats).mapToObj(i -> ElementType.INT).toList(),
                IntStream.range(0, repeats).mapToObj(i -> (TopNEncoder) TopNEncoder.DEFAULT_SORTABLE).toList(),
                List.of(new TopNOperator.SortOrder(0, false, false)),
                Integer.MAX_VALUE
            );
            drivers.add(TestDriverFactory.create(driverContext, source, List.of(topn), new PageConsumerOperator(page -> {
                assertThat(page.getPositionCount(), equalTo(topCount));
                page.close();
            })));
        }
        try {
            OperatorTestCase.runDriver(drivers);
        } catch (CircuitBreakingException e) {
            logger.info("expected breaker trip", e);
        }
        for (DriverContext c : contexts) {
            OperatorTestCase.assertDriverContext(c);
        }
    }

    public static class AnyFailedWatcher extends TestWatcher {
        private boolean anyFailed = false;

        @Override
        protected void failed(Throwable e, Description description) {
            anyFailed = true;
        }
    }
}
