/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.util;

import org.elasticsearch.Build;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverEarlyTerminationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class DelayTests extends ESTestCase {

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    /**
     * Regression test for slow query cancellation: {@code delay()} must observe cancellation while it is
     * sleeping, not only once before it starts. The driver's cancellation is cooperative and the sleep is
     * not interruptible, so the evaluator has to poll {@link DriverContext#checkForEarlyTermination()}
     * periodically. Here the checker lets the first poll through (query still running) and trips on the next
     * one (query cancelled mid-sleep); with the previous single-check implementation the evaluator would
     * sleep for the whole {@code delayMs} and never see the cancellation.
     */
    public void testDelayObservesCancellationWhileSleeping() {
        assumeTrue("delay() is only available in snapshot builds", Build.current().isSnapshot());
        DriverContext driverContext = driverContext();
        AtomicInteger checks = new AtomicInteger();
        driverContext.initializeEarlyTerminationChecker(() -> {
            if (checks.incrementAndGet() >= 2) {
                throw new DriverEarlyTerminationException("simulated cancellation");
            }
        });

        // Long enough to span many cancellation-check slices; the fix must abort well before it elapses.
        long delayMs = 5000;
        try (Delay.DelayEvaluator evaluator = new Delay.DelayEvaluator(driverContext, delayMs); Page page = new Page(1)) {
            long start = System.nanoTime();
            expectThrows(DriverEarlyTerminationException.class, () -> {
                Block block = evaluator.eval(page);
                block.close();
            });
            long elapsedMs = TimeValue.nsecToMSec(System.nanoTime() - start);
            assertThat("delay() should abort mid-sleep, not sleep the whole duration", elapsedMs, lessThan(delayMs));
            assertThat(checks.get(), greaterThanOrEqualTo(2));
        }
    }

    /**
     * Companion to {@link #testDelayObservesCancellationWhileSleeping}, but for graceful async STOP. Unlike a
     * hard cancel, STOP winds a query down by firing the driver's {@link DriverContext#runStopHooks() stop
     * hooks} (and closing the exchange source) rather than setting the driver's cancel/early-finished flags, so
     * on a coordinator pipeline driver ending in an {@code OutputOperator} the sleep would otherwise run to
     * completion. The evaluator registers a stop hook so a sleeping delay aborts with a
     * {@link DriverEarlyTerminationException} (clean wind-down, partial results). Here a background thread fires
     * the stop hooks mid-sleep, mimicking the STOP transport thread.
     */
    public void testDelayObservesStopWhileSleeping() throws Exception {
        assumeTrue("delay() is only available in snapshot builds", Build.current().isSnapshot());
        DriverContext driverContext = driverContext();

        // Long enough to span many cancellation-check slices; STOP must cut the sleep short well before it elapses.
        long delayMs = 5000;
        try (Delay.DelayEvaluator evaluator = new Delay.DelayEvaluator(driverContext, delayMs); Page page = new Page(1)) {
            // The evaluator's constructor registered the stop hook; fire it from another thread as STOP does.
            Thread stopper = new Thread(() -> {
                safeSleep(50);
                assertThat("stop hook should report it cut running work", driverContext.runStopHooks(), equalTo(true));
            }, "delay-stopper");
            stopper.start();
            long start = System.nanoTime();
            // Unlike hard cancel, STOP does not throw: the sleep returns early so the in-flight row can flow through
            // and the pipeline drains to natural completion.
            try (Block block = evaluator.eval(page)) {
                assertThat(block.getPositionCount(), equalTo(1));
                assertThat(((BooleanBlock) block).getBoolean(0), equalTo(true));
            }
            long elapsedMs = TimeValue.nsecToMSec(System.nanoTime() - start);
            stopper.join();
            assertThat("delay() should stop sleeping on STOP, not sleep the whole duration", elapsedMs, lessThan(delayMs));
        }
    }

    /**
     * Sanity check that a delay which is never cancelled still produces a boolean {@code true} for every row.
     */
    public void testDelayReturnsResultWhenNotCancelled() {
        assumeTrue("delay() is only available in snapshot builds", Build.current().isSnapshot());
        DriverContext driverContext = driverContext();
        int positions = between(1, 10);
        try (Delay.DelayEvaluator evaluator = new Delay.DelayEvaluator(driverContext, 1); Page page = new Page(positions)) {
            try (Block block = evaluator.eval(page)) {
                assertThat(block.getPositionCount(), equalTo(positions));
                BooleanBlock booleanBlock = (BooleanBlock) block;
                for (int p = 0; p < positions; p++) {
                    assertThat(booleanBlock.getBoolean(p), equalTo(true));
                }
            }
        }
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
