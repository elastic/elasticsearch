/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.stringContainsInOrder;

/**
 * Regression test guarding against ESQL response warnings going missing when a {@link Driver}
 * hops between worker threads mid-execution.
 * <p>
 *     For {@link Warnings#registerWarning} to work, we need to carefully hand off the thread
 *     context every time the {@link Driver} shifts from one thread to another.
 * </p>
 */
public class DriverThreadContextWarningLossTests extends ESTestCase {

    private static final String WARNING_MESSAGE = "driver-thread-hop warning that must survive completion";
    private static final String SECOND_WARNING_MESSAGE = "second driver-thread-hop warning, from a later hop";

    /**
     * Test that rescheduling preserves the thread context.
     * <p>
     *     To make the thread hop deterministic (rather than a rare scheduling race), this test pins
     *     each driver iteration to a specific, dedicated, single-thread executor: the first iteration
     *     (where the warning is registered) always runs on thread "A", and the second iteration
     *     (where the driver finishes and the response headers are collected) always runs on a
     *     genuinely different thread "B" that has never had anything stashed on its {@link
     *     ThreadContext} slot. This relies on {@code maxIterations == 1}, which forces {@link
     *     Driver#schedule} to resubmit to the executor after every single loop iteration, and on the
     *     operator chain needing exactly one iteration per page (one page registers the warning, a
     *     second page drains the source and finishes the driver).
     * </p>
     */
    public void testWarningRegisteredOnHoppedThreadSurvivesCompletion() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        ExecutorService threadA = Executors.newSingleThreadExecutor(r -> new Thread(r, "warning-loss-repro-A"));
        ExecutorService threadB = Executors.newSingleThreadExecutor(r -> new Thread(r, "warning-loss-repro-B"));
        try {
            // Every call to schedule() ends by submitting exactly one task to this executor. With
            // maxIterations == 1 and a two-page, one-iteration-per-page operator chain (see below),
            // the driver submits exactly twice: once for the iteration that registers the warning,
            // and once for the iteration that drains the last page and completes the driver. Pinning
            // those two submissions to two distinct, never-shared, single-thread executors guarantees
            // (rather than merely risks) that the registering thread and the completing thread differ.
            AtomicInteger submissionCount = new AtomicInteger();
            Executor twoThreadHoppingExecutor = task -> {
                ExecutorService target = submissionCount.getAndIncrement() == 0 ? threadA : threadB;
                target.execute(task);
            };

            DriverContext driverContext = driverContext();
            List<Page> inPages = List.of(onePositionPage(driverContext), onePositionPage(driverContext));
            AtomicBoolean warned = new AtomicBoolean();
            WarnOnFirstPageOperator warnOperator = new WarnOnFirstPageOperator(driverContext, warned, WARNING_MESSAGE);
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(inPages.iterator()),
                List.of(warnOperator),
                new PageConsumerOperator(page -> {})
            );

            DriverRunner runner = new DriverRunner(threadContext) {
                @Override
                protected void start(Driver driver, ActionListener<Void> driverListener) {
                    Driver.start(threadContext, twoThreadHoppingExecutor, driver, 1, driverListener);
                }
            };

            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<List<String>> warningsSeenOnCompletion = new AtomicReference<>();
            AtomicReference<Exception> failure = new AtomicReference<>();
            runner.runToCompletion(List.of(driver), ActionListener.wrap(ignored -> {
                // Read the response headers on whatever thread the driver actually completed on -
                // exactly what production code does immediately after runToCompletion's listener
                // fires, before anything might hop to yet another thread.
                warningsSeenOnCompletion.set(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()));
                completed.countDown();
            }, e -> {
                failure.set(e);
                completed.countDown();
            }));

            assertTrue("driver did not complete in time", completed.await(30, TimeUnit.SECONDS));
            assertNull(failure.get());
            assertTrue("warning was never registered", warned.get());
            assertThat(
                "warning registered on the hopped-away thread should still be visible when the driver completes",
                warningsSeenOnCompletion.get(),
                hasItem(stringContainsInOrder(WARNING_MESSAGE))
            );
        } finally {
            threadA.shutdownNow();
            threadB.shutdownNow();
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    /**
     * Same shape as {@link #testWarningRegisteredOnHoppedThreadSurvivesCompletion}, but with a
     * third page/executor hop and a second warning registered on that middle hop, to guard
     * against a fix that only patches the first {@code schedule} resubmission (e.g. one that
     * special-cases {@link Driver#start} instead of fixing every recursive call).
     */
    public void testWarningsRegisteredAcrossMultipleHopsAllSurviveCompletion() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        ExecutorService threadA = Executors.newSingleThreadExecutor(r -> new Thread(r, "warning-loss-repro-A"));
        ExecutorService threadB = Executors.newSingleThreadExecutor(r -> new Thread(r, "warning-loss-repro-B"));
        ExecutorService threadC = Executors.newSingleThreadExecutor(r -> new Thread(r, "warning-loss-repro-C"));
        try {
            AtomicInteger submissionCount = new AtomicInteger();
            List<ExecutorService> hops = List.of(threadA, threadB, threadC);
            Executor threeThreadHoppingExecutor = task -> {
                int index = Math.min(submissionCount.getAndIncrement(), hops.size() - 1);
                hops.get(index).execute(task);
            };

            DriverContext driverContext = driverContext();
            List<Page> inPages = List.of(onePositionPage(driverContext), onePositionPage(driverContext), onePositionPage(driverContext));
            AtomicBoolean warnedFirst = new AtomicBoolean();
            AtomicBoolean warnedSecond = new AtomicBoolean();
            // Warn on the first page (registers WARNING_MESSAGE on thread A) and again on the second
            // page (registers SECOND_WARNING_MESSAGE on thread B), so both the first and second hops
            // must preserve context for either warning to survive to completion on thread C.
            WarnOnNthPageOperator warnOperator = new WarnOnNthPageOperator(
                driverContext,
                Map.of(
                    0,
                    new WarnOnNthPageOperator.Warning(warnedFirst, WARNING_MESSAGE),
                    1,
                    new WarnOnNthPageOperator.Warning(warnedSecond, SECOND_WARNING_MESSAGE)
                )
            );
            Driver driver = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(inPages.iterator()),
                List.of(warnOperator),
                new PageConsumerOperator(page -> {})
            );

            DriverRunner runner = new DriverRunner(threadContext) {
                @Override
                protected void start(Driver driver, ActionListener<Void> driverListener) {
                    Driver.start(threadContext, threeThreadHoppingExecutor, driver, 1, driverListener);
                }
            };

            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<List<String>> warningsSeenOnCompletion = new AtomicReference<>();
            AtomicReference<Exception> failure = new AtomicReference<>();
            runner.runToCompletion(List.of(driver), ActionListener.wrap(ignored -> {
                warningsSeenOnCompletion.set(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()));
                completed.countDown();
            }, e -> {
                failure.set(e);
                completed.countDown();
            }));

            assertTrue("driver did not complete in time", completed.await(30, TimeUnit.SECONDS));
            assertNull(failure.get());
            assertTrue("first warning was never registered", warnedFirst.get());
            assertTrue("second warning was never registered", warnedSecond.get());
            assertThat(
                "warning registered on the first hopped-away thread should still be visible when the driver completes",
                warningsSeenOnCompletion.get(),
                hasItem(stringContainsInOrder(WARNING_MESSAGE))
            );
            assertThat(
                "warning registered on the second hopped-away thread should still be visible when the driver completes",
                warningsSeenOnCompletion.get(),
                hasItem(stringContainsInOrder(SECOND_WARNING_MESSAGE))
            );
        } finally {
            threadA.shutdownNow();
            threadB.shutdownNow();
            threadC.shutdownNow();
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    private static Page onePositionPage(DriverContext driverContext) {
        return new Page(driverContext.blockFactory().newConstantIntBlockWith(1, 1));
    }

    private static DriverContext driverContext() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    /**
     * Registers a given warning message exactly once, on the very first page it processes, and
     * passes every page through unchanged otherwise.
     */
    private static final class WarnOnFirstPageOperator extends AbstractPageMappingOperator {
        private final DriverContext driverContext;
        private final AtomicBoolean warned;
        private final String warningMessage;

        WarnOnFirstPageOperator(DriverContext driverContext, AtomicBoolean warned, String warningMessage) {
            this.driverContext = driverContext;
            this.warned = warned;
            this.warningMessage = warningMessage;
        }

        @Override
        protected Page process(Page page) {
            if (warned.compareAndSet(false, true)) {
                Warnings warnings = Warnings.createOnlyWarnings(driverContext.warningsMode(), TEST_SOURCE_LOCATION);
                warnings.registerWarning(warningMessage);
            }
            return page;
        }

        @Override
        public String toString() {
            return "WarnOnFirstPageOperator";
        }

        @Override
        public void close() {

        }

        private static final WarningSourceLocation TEST_SOURCE_LOCATION = new WarningSourceLocation() {
            @Override
            public int lineNumber() {
                return 1;
            }

            @Override
            public int columnNumber() {
                return 1;
            }

            @Override
            public String viewName() {
                return null;
            }

            @Override
            public String text() {
                return "test";
            }
        };
    }

    /**
     * Registers a distinct warning message on each of a set of specific, zero-indexed pages (by
     * position in the pages the operator sees), and passes every page through unchanged
     * otherwise. Used to confirm that warnings registered on more than one thread hop all survive
     * to completion, not just the first one.
     */
    private static final class WarnOnNthPageOperator extends AbstractPageMappingOperator {
        record Warning(AtomicBoolean warned, String message) {}

        private final DriverContext driverContext;
        private final Map<Integer, Warning> warningsByPageIndex;
        private final AtomicInteger pageIndex = new AtomicInteger();

        WarnOnNthPageOperator(DriverContext driverContext, Map<Integer, Warning> warningsByPageIndex) {
            this.driverContext = driverContext;
            this.warningsByPageIndex = warningsByPageIndex;
        }

        @Override
        protected Page process(Page page) {
            Warning warning = warningsByPageIndex.get(pageIndex.getAndIncrement());
            if (warning != null && warning.warned().compareAndSet(false, true)) {
                Warnings warnings = Warnings.createOnlyWarnings(driverContext.warningsMode(), WarnOnFirstPageOperator.TEST_SOURCE_LOCATION);
                warnings.registerWarning(warning.message());
            }
            return page;
        }

        @Override
        public String toString() {
            return "WarnOnNthPageOperator";
        }

        @Override
        public void close() {

        }
    }
}
