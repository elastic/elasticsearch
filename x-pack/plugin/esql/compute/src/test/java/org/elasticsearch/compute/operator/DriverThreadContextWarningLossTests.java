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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.stringContainsInOrder;

/**
 * Reproduces ESQL response warnings going missing when a {@link Driver} hops between worker
 * threads mid-execution.
 * <p>
 *     {@link Warnings#registerWarning} ultimately calls the static
 *     {@link HeaderWarning#addWarning}, which writes into whatever {@link ThreadContext} is
 *     registered for the node, keyed by the <em>calling</em> thread's thread-local slot. {@link
 *     Driver#schedule} re-submits its per-iteration task to the shared executor on every
 *     iteration via a plain {@link org.elasticsearch.common.util.concurrent.AbstractRunnable}
 *     that is <em>not</em> wrapped with {@code threadContext.preserveContext(...)}, so successive
 *     iterations of the same driver can run on different worker threads. Only the final
 *     completion path (the {@code onComplete} handler in {@link Driver#schedule}) restores
 *     context — via {@code ContextPreservingActionListener.wrapPreservingContext(listener,
 *     threadContext)} — and it does so relative to whatever context is active on the
 *     <em>completing</em> thread at that moment, not the (possibly different) thread that ran the
 *     iteration which registered the warning. If that thread never had anything stashed on it
 *     (a fresh worker thread pulled from the pool), the response header written on the earlier
 *     thread is silently dropped.
 * </p>
 * <p>
 *     To make this reproducible (rather than a rare scheduling race), this test pins each driver
 *     iteration to a specific, dedicated, single-thread executor: the first iteration (where the
 *     warning is registered) always runs on thread "A", and the second iteration (where the
 *     driver finishes and the response headers are collected) always runs on a genuinely
 *     different thread "B" that has never had anything stashed on its {@link ThreadContext}
 *     slot. This relies on {@code maxIterations == 1}, which forces {@link Driver#schedule} to
 *     resubmit to the executor after every single loop iteration, and on the operator chain
 *     needing exactly one iteration per page (one page registers the warning, a second page
 *     drains the source and finishes the driver).
 * </p>
 * <p>
 *     This is expected to fail until a later commit replaces this ambient, {@link ThreadContext}
 *     -based warning propagation with an explicit typed field threaded through {@code
 *     DriverCompletionInfo}. Until then, keep this muted (see {@code muted-tests.yml}) — the
 *     failure is the point, it proves the bug exists ahead of the real fix landing in a later
 *     commit of this series.
 * </p>
 */
public class DriverThreadContextWarningLossTests extends ESTestCase {

    private static final String WARNING_MESSAGE = "driver-thread-hop warning that must survive completion";

    public void testWarningRegisteredOnHoppedThreadIsLostOnCompletion() throws Exception {
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
            java.util.concurrent.Executor twoThreadHoppingExecutor = task -> {
                ExecutorService target = submissionCount.getAndIncrement() == 0 ? threadA : threadB;
                target.execute(task);
            };

            DriverContext driverContext = driverContext();
            List<Page> inPages = List.of(onePositionPage(driverContext), onePositionPage(driverContext));
            AtomicBoolean warned = new AtomicBoolean();
            WarnOnFirstPageOperator warnOperator = new WarnOnFirstPageOperator(driverContext, warned);
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

    private static Page onePositionPage(DriverContext driverContext) {
        return new Page(driverContext.blockFactory().newConstantIntBlockWith(1, 1));
    }

    private static DriverContext driverContext() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    /**
     * Registers {@link #WARNING_MESSAGE} exactly once, on the very first page it processes, and
     * passes every page through unchanged otherwise.
     */
    private static final class WarnOnFirstPageOperator extends AbstractPageMappingOperator {
        private final DriverContext driverContext;
        private final AtomicBoolean warned;

        WarnOnFirstPageOperator(DriverContext driverContext, AtomicBoolean warned) {
            this.driverContext = driverContext;
            this.warned = warned;
        }

        @Override
        protected Page process(Page page) {
            if (warned.compareAndSet(false, true)) {
                Warnings warnings = Warnings.createOnlyWarnings(driverContext.warningsMode(), TEST_SOURCE_LOCATION);
                warnings.registerWarning(WARNING_MESSAGE);
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
}
