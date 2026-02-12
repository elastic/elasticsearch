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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator.InputOrdering;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Runs a few topns in parallel trying to cause an out of memory. {@link TopNOperator} can
 * run the JVM out of memory if it's {@link TopNOperator#close} reduces the circuit breaker
 * but doesn't {@code null} the references as it goes. We're looking to throw a
 * {@link CircuitBreakingException} instead.
 * <p>
 *     It'd be ideal if the nulling and the breaker reductions were atomic, but that's not how
 *     Elasticsearch is. Instead, we figure for some amount of overhead of things that have
 *     been reduced by the breaker but are not ready for GC. And that's mostly fine, so long as
 *     the things-not-ready-for-GC are <strong>small</strong>. The entire {@linkplain TopNOperator}
 *     can be quite a bit too big. Instead, it frees row-by-row. What's too big? We don't know
 *     precisely, but a full {@linkplain TopNOperator} can be 100s of MB which is very much too big.
 * </p>
 * <p>
 *     This is parameterized because we'd like for it to run a bunch of times on each test
 *     execution AND because we'd like to see if failures at different points are more likely. We
 *     don't believe it is, but we want the test executions anyway.
 * </p>
 */
public class TopNOomRaceTests extends ESTestCase {
    @ParametersFactory(argumentFormatting = "repeats = %s")
    public static List<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        /*
         * 5 here just limits the total number of runs. I could use 1, the test would
         * just run 5 times longer.
         */
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
        int approxObjectRefsPerFilledRow = 5;
        int approxSizePerFilledRow = RamUsageEstimator.NUM_BYTES_OBJECT_REF * approxObjectRefsPerFilledRow + repeats * Integer.BYTES;
        int topCount = (int) ((double) limit.getBytes() / approxSizePerFilledRow * usedByEachThread) - 1;
        int maxInput = topCount * 3;
        assertThat(topCount, greaterThan(0));

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit);
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        List<DriverContext> contexts = new ArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        for (int d = 0; d < driverCount; d++) {
            MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);
            DriverContext driverContext = new DriverContext(bigArrays, blockFactory, null);
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
                    Block ints = blockFactory.newIntRangeVector(positionOffset, lastEnd).asBlock();
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
                Collections.nCopies(repeats, ElementType.INT),
                Collections.nCopies(repeats, TopNEncoder.DEFAULT_SORTABLE),
                List.of(new TopNOperator.SortOrder(0, false, false)),
                Integer.MAX_VALUE,
                InputOrdering.NOT_SORTED
            );
            drivers.add(TestDriverFactory.create(driverContext, source, List.of(topn), new PageConsumerOperator(page -> {
                assertThat(page.getPositionCount(), equalTo(topCount));
                page.close();
            })));
        }
        expectThrows(CircuitBreakingException.class, () -> new TestDriverRunner().run(drivers));
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
