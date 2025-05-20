/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.either;
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

        Driver driver = createDriver(startEpoch, startNanos, driverContext, inPages, outPages, TimeValue.timeValueDays(10));

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

        Driver driver = createDriver(startEpoch, startNanos, driverContext, inPages, outPages, TimeValue.timeValueDays(10));

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

        Driver driver = createDriver(startEpoch, startNanos, driverContext, inPages, outPages, TimeValue.timeValueNanos(tickTime));

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

    public void testProfileAndStatusInterval() {
        DriverContext driverContext = driverContext();
        List<Page> inPages = randomList(2, 100, DriverTests::randomPage);
        List<Page> outPages = new ArrayList<>();

        long startEpoch = randomNonNegativeLong();
        long startNanos = randomLong();
        long waitTime = randomLongBetween(10000, 100000);
        long tickTime = randomLongBetween(10000, 100000);
        long statusInterval = randomLongBetween(1, 10);

        Driver driver = createDriver(startEpoch, startNanos, driverContext, inPages, outPages, TimeValue.timeValueNanos(statusInterval));

        NowSupplier nowSupplier = new NowSupplier(startNanos, waitTime, tickTime);

        int iterationsPerTick = randomIntBetween(1, 10);

        for (int i = 0; i < inPages.size(); i += iterationsPerTick) {
            logger.info("status {} {}", i, driver.status());
            assertThat(driver.status().status(), equalTo(i == 0 ? DriverStatus.Status.QUEUED : DriverStatus.Status.WAITING));
            assertThat(driver.status().started(), equalTo(startEpoch));
            assertThat(driver.status().iterations(), equalTo((long) i));
            assertThat(driver.status().cpuNanos(), equalTo(tickTime * i));
            driver.run(TimeValue.timeValueDays(10), iterationsPerTick, nowSupplier);
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

    private static Driver createDriver(
        long startEpoch,
        long startNanos,
        DriverContext driverContext,
        List<Page> inPages,
        List<Page> outPages,
        TimeValue statusInterval
    ) {
        return new Driver(
            "unset",
            "test",
            "test",
            "test",
            startEpoch,
            startNanos,
            driverContext,
            () -> "unset",
            new CannedSourceOperator(inPages.iterator()),
            List.of(),
            new TestResultPageSinkOperator(outPages::add),
            statusInterval,
            () -> {}
        );
    }

    static class NowSupplier implements LongSupplier {
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
            Driver driver = TestDriverFactory.create(driverContext, new CannedSourceOperator(inPages.iterator()) {
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
            }));
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

    public void testEarlyTermination() {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            int positions = between(1000, 5000);
            List<Page> inPages = randomList(1, 100, () -> {
                var block = driverContext.blockFactory().newConstantIntBlockWith(randomInt(), positions);
                return new Page(block);
            });
            final var sourceOperator = new CannedSourceOperator(inPages.iterator());
            final int maxAllowedRows = between(1, 100);
            final AtomicInteger processedRows = new AtomicInteger(0);
            var sinkHandler = new ExchangeSinkHandler(driverContext.blockFactory(), positions, System::currentTimeMillis);
            var sinkOperator = new ExchangeSinkOperator(sinkHandler.createExchangeSink(() -> {}));
            final var delayOperator = new EvalOperator(driverContext.blockFactory(), new EvalOperator.ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        driverContext.checkForEarlyTermination();
                        if (processedRows.incrementAndGet() >= maxAllowedRows) {
                            sinkHandler.fetchPageAsync(true, ActionListener.noop());
                        }
                    }
                    return driverContext.blockFactory().newConstantBooleanBlockWith(true, page.getPositionCount());
                }

                @Override
                public void close() {

                }
            });
            Driver driver = TestDriverFactory.create(driverContext, sourceOperator, List.of(delayOperator), sinkOperator);
            ThreadContext threadContext = threadPool.getThreadContext();
            PlainActionFuture<Void> future = new PlainActionFuture<>();

            Driver.start(threadContext, threadPool.executor("esql"), driver, between(1, 1000), future);
            future.actionGet(30, TimeUnit.SECONDS);
            assertThat(processedRows.get(), equalTo(maxAllowedRows));
        } finally {
            terminate(threadPool);
        }
    }

    public void testResumeOnEarlyFinish() throws Exception {
        DriverContext driverContext = driverContext();
        ThreadPool threadPool = threadPool();
        try {
            var sourceHandler = new ExchangeSourceHandler(between(1, 5), threadPool.executor("esql"));
            var sinkHandler = new ExchangeSinkHandler(driverContext.blockFactory(), between(1, 5), System::currentTimeMillis);
            var sourceOperator = new ExchangeSourceOperator(sourceHandler.createExchangeSource());
            var sinkOperator = new ExchangeSinkOperator(sinkHandler.createExchangeSink(() -> {}));
            Driver driver = TestDriverFactory.create(driverContext, sourceOperator, List.of(), sinkOperator);
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            Driver.start(threadPool.getThreadContext(), threadPool.executor("esql"), driver, between(1, 1000), future);
            assertBusy(
                () -> assertThat(
                    driver.status().status(),
                    either(equalTo(DriverStatus.Status.ASYNC)).or(equalTo(DriverStatus.Status.STARTING))
                )
            );
            sinkHandler.fetchPageAsync(true, ActionListener.noop());
            future.actionGet(5, TimeUnit.SECONDS);
            assertThat(driver.status().status(), equalTo(DriverStatus.Status.DONE));
        } finally {
            terminate(threadPool);
        }
    }

    private static void assertRunningWithRegularUser(ThreadPool threadPool) {
        String user = threadPool.getThreadContext().getHeader("user");
        assertThat(user, equalTo("user1"));
    }

    private static Page randomPage() {
        RandomBlock block = RandomBlock.randomBlock(
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

    static class SwitchContextOperator extends AsyncOperator<Page> {
        private final ThreadPool threadPool;

        SwitchContextOperator(DriverContext driverContext, ThreadPool threadPool) {
            super(driverContext, threadPool.getThreadContext(), between(1, 3));
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
        public Page getOutput() {
            return fetchFromBuffer();
        }

        @Override
        protected void releaseFetchedOnAnyThread(Page page) {
            releasePageOnAnyThread(page);
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
