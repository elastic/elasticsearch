/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.data.BasicBlockTests;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;

public class DriverTests extends ESTestCase {
    /**
     * Runs a driver to completion in a single call and asserts that the
     * status and profile returned makes sense.
     */
    public void testProfileAndStatusFinishInOneRound() {
        DriverContext driverContext = driverContext();
        List<Page> inPages = randomList(1, 100, DriverTests::randomPage);
        List<Page> outPages = new ArrayList<>();

        long startEpoch = randomNonNegativeLong();
        long startNanos = randomLong();
        long waitTime = randomLongBetween(1000, 100000);
        long tickTime = randomLongBetween(1, 10000);

        Driver driver = new Driver(
            "unset",
            startEpoch,
            startNanos,
            driverContext,
            () -> "unset",
            new CannedSourceOperator(inPages.iterator()),
            List.of(),
            new TestResultPageSinkOperator(outPages::add),
            TimeValue.timeValueDays(10),
            () -> {}
        );

        NowSupplier nowSupplier = new NowSupplier(startNanos, waitTime, tickTime);

        logger.info("status {}", driver.status());
        assertThat(driver.status().status(), equalTo(DriverStatus.Status.QUEUED));
        assertThat(driver.status().started(), equalTo(startEpoch));
        assertThat(driver.status().cpuNanos(), equalTo(0L));
        assertThat(driver.status().iterations(), equalTo(0L));
        driver.run(TimeValue.timeValueSeconds(Long.MAX_VALUE), Integer.MAX_VALUE, nowSupplier);
        logger.info("status {}", driver.status());
        assertThat(driver.status().status(), equalTo(DriverStatus.Status.DONE));
        assertThat(driver.status().started(), equalTo(startEpoch));
        long sumRunningTime = tickTime * (nowSupplier.callCount - 1);
        assertThat(driver.status().cpuNanos(), equalTo(sumRunningTime));
        assertThat(driver.status().iterations(), equalTo((long) inPages.size()));

        logger.info("profile {}", driver.profile());
        assertThat(driver.profile().tookNanos(), equalTo(waitTime + sumRunningTime));
        assertThat(driver.profile().cpuNanos(), equalTo(sumRunningTime));
        assertThat(driver.profile().iterations(), equalTo((long) inPages.size()));
    }

    /**
     * Runs the driver processing a single page at a time and asserting that
     * the status reported between each call is sane. And that the profile
     * returned after completion is sane.
     */
    public void testProfileAndStatusOneIterationAtATime() {
        DriverContext driverContext = driverContext();
        List<Page> inPages = randomList(2, 100, DriverTests::randomPage);
        List<Page> outPages = new ArrayList<>();

        long startEpoch = randomNonNegativeLong();
        long startNanos = randomLong();
        long waitTime = randomLongBetween(1000, 100000);
        long tickTime = randomLongBetween(1, 10000);

        Driver driver = new Driver(
            "unset",
            startEpoch,
            startNanos,
            driverContext,
            () -> "unset",
            new CannedSourceOperator(inPages.iterator()),
            List.of(),
            new TestResultPageSinkOperator(outPages::add),
            TimeValue.timeValueDays(10),
            () -> {}
        );

        NowSupplier nowSupplier = new NowSupplier(startNanos, waitTime, tickTime);
        for (int i = 0; i < inPages.size(); i++) {
            logger.info("status {} {}", i, driver.status());
            assertThat(driver.status().status(), equalTo(i == 0 ? DriverStatus.Status.QUEUED : DriverStatus.Status.WAITING));
            assertThat(driver.status().started(), equalTo(startEpoch));
            assertThat(driver.status().iterations(), equalTo((long) i));
            assertThat(driver.status().cpuNanos(), equalTo(tickTime * i));
            driver.run(TimeValue.timeValueSeconds(Long.MAX_VALUE), 1, nowSupplier);
        }

        logger.info("status {}", driver.status());
        assertThat(driver.status().status(), equalTo(DriverStatus.Status.DONE));
        assertThat(driver.status().started(), equalTo(startEpoch));
        assertThat(driver.status().iterations(), equalTo((long) inPages.size()));
        assertThat(driver.status().cpuNanos(), equalTo(tickTime * inPages.size()));

        logger.info("profile {}", driver.profile());
        assertThat(driver.profile().tookNanos(), equalTo(waitTime + tickTime * (nowSupplier.callCount - 1)));
        assertThat(driver.profile().cpuNanos(), equalTo(tickTime * inPages.size()));
        assertThat(driver.profile().iterations(), equalTo((long) inPages.size()));
    }

    /**
     * Runs the driver processing a single page at a time via a synthetic timeout
     * and asserting that the status reported between each call is sane. And that
     * the profile returned after completion is sane.
     */
    public void testProfileAndStatusTimeout() {
        DriverContext driverContext = driverContext();
        List<Page> inPages = randomList(2, 100, DriverTests::randomPage);
        List<Page> outPages = new ArrayList<>();

        long startEpoch = randomNonNegativeLong();
        long startNanos = randomLong();
        long waitTime = randomLongBetween(1000, 100000);
        long tickTime = randomLongBetween(1, 10000);

        Driver driver = new Driver(
            "unset",
            startEpoch,
            startNanos,
            driverContext,
            () -> "unset",
            new CannedSourceOperator(inPages.iterator()),
            List.of(),
            new TestResultPageSinkOperator(outPages::add),
            TimeValue.timeValueNanos(tickTime),
            () -> {}
        );

        NowSupplier nowSupplier = new NowSupplier(startNanos, waitTime, tickTime);
        for (int i = 0; i < inPages.size(); i++) {
            logger.info("status {} {}", i, driver.status());
            assertThat(driver.status().status(), equalTo(i == 0 ? DriverStatus.Status.QUEUED : DriverStatus.Status.WAITING));
            assertThat(driver.status().started(), equalTo(startEpoch));
            assertThat(driver.status().iterations(), equalTo((long) i));
            assertThat(driver.status().cpuNanos(), equalTo(tickTime * i));
            driver.run(TimeValue.timeValueNanos(tickTime), Integer.MAX_VALUE, nowSupplier);
        }

        logger.info("status {}", driver.status());
        assertThat(driver.status().status(), equalTo(DriverStatus.Status.DONE));
        assertThat(driver.status().started(), equalTo(startEpoch));
        assertThat(driver.status().iterations(), equalTo((long) inPages.size()));
        assertThat(driver.status().cpuNanos(), equalTo(tickTime * inPages.size()));

        logger.info("profile {}", driver.profile());
        assertThat(driver.profile().tookNanos(), equalTo(waitTime + tickTime * (nowSupplier.callCount - 1)));
        assertThat(driver.profile().cpuNanos(), equalTo(tickTime * inPages.size()));
        assertThat(driver.profile().iterations(), equalTo((long) inPages.size()));
    }

    class NowSupplier implements LongSupplier {
        private final long startNanos;
        private final long waitTime;
        private final long tickTime;

        private int callCount;

        NowSupplier(long startNanos, long waitTime, long tickTime) {
            this.startNanos = startNanos;
            this.waitTime = waitTime;
            this.tickTime = tickTime;
        }

        @Override
        public long getAsLong() {
            return startNanos + waitTime + tickTime * callCount++;
        }
    }

    public void testThreadContext() throws Exception {
        DriverContext driverContext = driverContext();
        int asyncActions = randomIntBetween(0, 5);
        for (int i = 0; i < asyncActions; i++) {
            driverContext.addAsyncAction();
        }
        ThreadPool threadPool = threadPool();
        try {
            List<Page> inPages = randomList(1, 100, DriverTests::randomPage);
            List<Page> outPages = new ArrayList<>();
            WarningsOperator warning1 = new WarningsOperator(threadPool);
            WarningsOperator warning2 = new WarningsOperator(threadPool);
            CyclicBarrier allPagesProcessed = new CyclicBarrier(2);
            Driver driver = new Driver(driverContext, new CannedSourceOperator(inPages.iterator()) {
                @Override
                public Page getOutput() {
                    assertRunningWithRegularUser(threadPool);
                    return super.getOutput();
                }
            }, List.of(warning1, new SwitchContextOperator(driverContext, threadPool), warning2), new PageConsumerOperator(page -> {
                assertRunningWithRegularUser(threadPool);
                outPages.add(page);
                if (outPages.size() == inPages.size()) {
                    try {
                        allPagesProcessed.await(30, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            }), () -> {});
            ThreadContext threadContext = threadPool.getThreadContext();
            CountDownLatch driverCompleted = new CountDownLatch(1);
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putHeader("user", "user1");
                Driver.start(threadContext, threadPool.executor("esql"), driver, between(1, 1000), ActionListener.running(() -> {
                    try {
                        assertRunningWithRegularUser(threadPool);
                        assertThat(outPages, equalTo(inPages));
                        Map<String, Set<String>> actualResponseHeaders = new HashMap<>();
                        for (Map.Entry<String, List<String>> e : threadPool.getThreadContext().getResponseHeaders().entrySet()) {
                            actualResponseHeaders.put(e.getKey(), Sets.newHashSet(e.getValue()));
                        }
                        Map<String, Set<String>> expectedResponseHeaders = new HashMap<>(warning1.warnings);
                        for (Map.Entry<String, Set<String>> e : warning2.warnings.entrySet()) {
                            expectedResponseHeaders.merge(e.getKey(), e.getValue(), Sets::union);
                        }
                        assertThat(actualResponseHeaders, equalTo(expectedResponseHeaders));
                    } finally {
                        driverCompleted.countDown();
                    }
                }));
            }
            allPagesProcessed.await(30, TimeUnit.SECONDS);
            // race with the Driver to notify the listener
            for (int i = 0; i < asyncActions; i++) {
                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    threadContext.putHeader("user", "system");
                    driverContext.removeAsyncAction();
                }
            }
            assertTrue(driverCompleted.await(30, TimeUnit.SECONDS));
        } finally {
            terminate(threadPool);
        }
    }

    private static void assertRunningWithRegularUser(ThreadPool threadPool) {
        String user = threadPool.getThreadContext().getHeader("user");
        assertThat(user, equalTo("user1"));
    }

    private static Page randomPage() {
        BasicBlockTests.RandomBlock block = BasicBlockTests.randomBlock(
            randomFrom(ElementType.BOOLEAN, ElementType.INT, ElementType.BYTES_REF),
            between(1, 10),
            randomBoolean(),
            1,
            between(1, 2),
            0,
            2
        );
        return new Page(block.block());
    }

    static class SwitchContextOperator extends AsyncOperator {
        private final ThreadPool threadPool;

        SwitchContextOperator(DriverContext driverContext, ThreadPool threadPool) {
            super(driverContext, between(1, 3));
            this.threadPool = threadPool;
        }

        @Override
        protected void performAsync(Page page, ActionListener<Page> listener) {
            assertRunningWithRegularUser(threadPool);
            if (randomBoolean()) {
                listener.onResponse(page);
                return;
            }
            threadPool.schedule(ActionRunnable.wrap(listener, innerListener -> {
                try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
                    threadPool.getThreadContext().putHeader("user", "system");
                    innerListener.onResponse(page);
                }
            }), TimeValue.timeValueNanos(between(1, 1_000_000)), threadPool.executor("esql"));
        }

        @Override
        protected void doClose() {

        }
    }

    static class WarningsOperator extends AbstractPageMappingOperator {
        private final ThreadPool threadPool;
        private final Map<String, Set<String>> warnings = new HashMap<>();

        WarningsOperator(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        protected Page process(Page page) {
            assertRunningWithRegularUser(threadPool);
            if (randomInt(100) < 10) {
                String k = "header-" + between(1, 10);
                Set<String> vs = Sets.newHashSet(randomList(1, 2, () -> "value-" + between(1, 20)));
                warnings.merge(k, vs, Sets::union);
                for (String v : vs) {
                    threadPool.getThreadContext().addResponseHeader(k, v);
                }
            }
            return page;
        }

        @Override
        public String toString() {
            return "WarningsOperator";
        }

        @Override
        public void close() {

        }
    }

    private ThreadPool threadPool() {
        int numThreads = randomIntBetween(1, 10);
        return new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(Settings.EMPTY, "esql", numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    private DriverContext driverContext() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = new BlockFactory(breaker, bigArrays);
        return new DriverContext(bigArrays, blockFactory);
    }

}
