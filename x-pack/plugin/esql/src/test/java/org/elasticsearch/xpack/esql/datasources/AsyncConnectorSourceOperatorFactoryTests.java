/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Limiter;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.ThreadCpuTimer;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Observed-LIMIT stop for {@link AsyncConnectorSourceOperatorFactory}: remaining 0 must stop
 * claiming further splits. Connectors do not reserve a pushed budget.
 */
public class AsyncConnectorSourceOperatorFactoryTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    public void testObservedRemainingZeroStopsClaiming() throws Exception {
        int splitCount = 12;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new StubSplit("s" + i));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger executeCount = new AtomicInteger();
        LatchedConnector connector = new LatchedConnector(executeCount, entered, proceed);
        QueryRequest request = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);
        ExecutorService pool = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory("test", "acsof-limit"));
        try {
            AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(connector, request, 10, pool, sliceQueue);
            Limiter observed = new Limiter(1);
            factory.setObservedLimiter(observed);

            DriverContext ctx = mockDriverContext();
            CountDownLatch done = new CountDownLatch(1);
            doAnswer(inv -> {
                done.countDown();
                return null;
            }).when(ctx).removeAsyncAction();

            SourceOperator operator = factory.get(ctx);
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            observed.tryAccumulateHits(1);
            proceed.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            drainRows(operator);
            operator.close();

            assertThat(executeCount.get(), equalTo(1));
            assertThat(sliceQueue.remaining(), equalTo(splitCount - 1));
            assertThat(connector.closeCount.get(), equalTo(1));
        } finally {
            proceed.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testFilteredLimitRampDoesNotStartEveryReader() throws Exception {
        int splitCount = 12;
        int drivers = 4;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new StubSplit("s" + i));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger executeCount = new AtomicInteger();
        LatchedConnector connector = new LatchedConnector(executeCount, entered, proceed);
        QueryRequest request = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);
        ExecutorService pool = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory("test", "acsof-ramp"));
        try {
            AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(connector, request, 10, pool, sliceQueue);
            Limiter observed = new Limiter(1);
            factory.setObservedLimiter(observed);

            CountDownLatch done = new CountDownLatch(drivers);
            List<SourceOperator> operators = new ArrayList<>(drivers);
            DriverContext first = mockDriverContext();
            doAnswer(inv -> {
                done.countDown();
                return null;
            }).when(first).removeAsyncAction();
            operators.add(factory.get(first));
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            for (int i = 1; i < drivers; i++) {
                DriverContext ctx = mockDriverContext();
                doAnswer(inv -> {
                    done.countDown();
                    return null;
                }).when(ctx).removeAsyncAction();
                operators.add(factory.get(ctx));
            }
            observed.tryAccumulateHits(1);
            proceed.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            for (SourceOperator operator : operators) {
                drainRows(operator);
                operator.close();
            }
            assertThat(executeCount.get(), equalTo(1));
            assertThat(sliceQueue.remaining(), equalTo(splitCount - 1));
            assertThat(connector.closeCount.get(), equalTo(1));
        } finally {
            proceed.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /**
     * Two drivers both start (no observed limiter). The first to finish must not close the
     * shared connector while the sibling is still in {@code execute()}.
     */
    public void testSiblingProducerDoesNotSeeClosedConnector() throws Exception {
        int splitCount = 4;
        int drivers = 2;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new StubSplit("s" + i));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        CountDownLatch entered = new CountDownLatch(drivers);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger executeCount = new AtomicInteger();
        LatchedConnector connector = new LatchedConnector(executeCount, entered, proceed);
        QueryRequest request = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);
        ExecutorService pool = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory("test", "acsof-close"));
        try {
            AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(connector, request, 10, pool, sliceQueue);

            CountDownLatch done = new CountDownLatch(drivers);
            List<SourceOperator> operators = new ArrayList<>(drivers);
            for (int i = 0; i < drivers; i++) {
                DriverContext ctx = mockDriverContext();
                doAnswer(inv -> {
                    done.countDown();
                    return null;
                }).when(ctx).removeAsyncAction();
                operators.add(factory.get(ctx));
            }
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            proceed.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            for (SourceOperator operator : operators) {
                drainRows(operator);
                operator.close();
            }
            assertThat(executeCount.get(), equalTo(splitCount));
            assertThat(connector.closeCount.get(), equalTo(1));
        } finally {
            proceed.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /**
     * {@code createDrivers} calls {@code get()} sequentially while the first producer already
     * runs. A producer-only refcount would close the connector before the sibling {@code get()}.
     */
    public void testLateGetDoesNotSeeClosedConnector() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger executeCount = new AtomicInteger();
        LatchedConnector connector = new LatchedConnector(executeCount, entered, proceed);
        QueryRequest request = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);
        ExecutorService pool = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory("test", "acsof-late-get"));
        try {
            AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(connector, request, 10, pool);

            CountDownLatch firstDone = new CountDownLatch(1);
            DriverContext first = mockDriverContext();
            doAnswer(inv -> {
                firstDone.countDown();
                return null;
            }).when(first).removeAsyncAction();
            SourceOperator op1 = factory.get(first);
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            proceed.countDown();
            assertTrue(firstDone.await(30, TimeUnit.SECONDS));
            assertThat(connector.closeCount.get(), equalTo(0));

            CountDownLatch secondDone = new CountDownLatch(1);
            DriverContext second = mockDriverContext();
            doAnswer(inv -> {
                secondDone.countDown();
                return null;
            }).when(second).removeAsyncAction();
            SourceOperator op2 = factory.get(second);
            assertTrue(secondDone.await(30, TimeUnit.SECONDS));
            assertThat(connector.closeCount.get(), equalTo(0));
            assertThat(executeCount.get(), equalTo(2));

            drainRows(op1);
            drainRows(op2);
            op1.close();
            assertThat(connector.closeCount.get(), equalTo(0));
            op2.close();
            assertThat(connector.closeCount.get(), equalTo(1));
        } finally {
            proceed.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testObservedRemainingZeroReleasesPageAfterBlockingNext() throws Exception {
        List<ExternalSplit> splits = List.of(new StubSplit("s0"));
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
        CountDownLatch enteredSecond = new CountDownLatch(1);
        CountDownLatch proceedSecond = new CountDownLatch(1);
        AtomicInteger nextCalls = new AtomicInteger();
        TwoPageLatchedConnector connector = new TwoPageLatchedConnector(nextCalls, enteredSecond, proceedSecond);
        QueryRequest request = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);
        ExecutorService pool = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory("test", "acsof-leftover"));
        try {
            AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(connector, request, 10, pool, sliceQueue);
            Limiter observed = new Limiter(100);
            factory.setObservedLimiter(observed);

            DriverContext ctx = mockDriverContext();
            CountDownLatch done = new CountDownLatch(1);
            doAnswer(inv -> {
                done.countDown();
                return null;
            }).when(ctx).removeAsyncAction();

            SourceOperator operator = factory.get(ctx);
            assertTrue(enteredSecond.await(30, TimeUnit.SECONDS));
            observed.tryAccumulateHits(observed.remaining());
            proceedSecond.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            int rows = drainRows(operator);
            operator.close();

            assertThat(nextCalls.get(), equalTo(2));
            assertThat("page popped after remaining hit 0 must be released", rows, equalTo(1));
        } finally {
            proceedSecond.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testProducerCpuIsMeteredIntoDriver() throws Exception {
        assumeTrue("needs thread CPU time", ThreadCpuTimer.currentNanos() >= 0);
        int splitCount = between(1, 4);
        long burnNanos = TimeUnit.MILLISECONDS.toNanos(between(1, 3));
        long cpuNanos = drainIntoDriver(new CpuBurningConnector(burnNanos, 0), splitCount);
        assertThat(cpuNanos, greaterThanOrEqualTo(splitCount * burnNanos));
    }

    /** The close burns well above the producer's own overhead, which can be a few milliseconds on a cold JVM. */
    public void testConnectorCloseIsMeteredIntoDriver() throws Exception {
        assumeTrue("needs thread CPU time", ThreadCpuTimer.currentNanos() >= 0);
        long closeBurnNanos = TimeUnit.MILLISECONDS.toNanos(20);
        long cpuNanos = drainIntoDriver(new CpuBurningConnector(0, closeBurnNanos), between(1, 4));
        assertThat(cpuNanos, greaterThanOrEqualTo(closeBurnNanos));
    }

    /** Drains {@code splitCount} splits from {@code connector} into a driver and returns the driver's CPU time. */
    private static long drainIntoDriver(Connector connector, int splitCount) throws Exception {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new StubSplit("s" + i));
        }
        QueryRequest request = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);
        ExecutorService pool = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory("test", "acsof-cpu"));
        try {
            AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(
                connector,
                request,
                10,
                pool,
                new ExternalSliceQueue(splits)
            );
            DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TEST_BLOCK_FACTORY, null);
            SourceOperator operator = factory.get(ctx);
            assertThat(drainRows(operator), equalTo(splitCount));
            operator.close();
            ctx.finish();
            PlainActionFuture<Void> producerDone = new PlainActionFuture<>();
            ctx.waitForAsyncActions(producerDone);
            producerDone.actionGet(30, TimeUnit.SECONDS);
            return ctx.cpuNanos();
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static DriverContext mockDriverContext() {
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
        // The producer loop runs inside meteredCpu, which a plain mock would skip, so run the work as the real method does.
        doAnswer(inv -> inv.<CheckedSupplier<?, ?>>getArgument(0).get()).when(driverContext)
            .meteredCpu(ArgumentMatchers.<CheckedSupplier<Object, RuntimeException>>any());
        return driverContext;
    }

    private static int drainRows(SourceOperator operator) {
        int rows = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            Page page = operator.getOutput();
            if (page != null) {
                rows += page.getPositionCount();
                page.releaseBlocks();
                continue;
            }
            if (operator.isFinished()) {
                return rows;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out draining connector operator");
            }
        }
    }

    private static class LatchedConnector implements Connector {
        private final AtomicInteger executeCount;
        private final CountDownLatch entered;
        private final CountDownLatch proceed;
        private final AtomicInteger closeCount = new AtomicInteger();
        private final AtomicBoolean closed = new AtomicBoolean();

        LatchedConnector(AtomicInteger executeCount, CountDownLatch entered, CountDownLatch proceed) {
            this.executeCount = executeCount;
            this.entered = entered;
            this.proceed = proceed;
        }

        @Override
        public ResultCursor execute(QueryRequest request, Split split) {
            return latchedExecute(request);
        }

        @Override
        public ResultCursor execute(QueryRequest request, ExternalSplit split) {
            return latchedExecute(request);
        }

        private ResultCursor latchedExecute(QueryRequest request) {
            if (closed.get()) {
                throw new IllegalStateException("Connector is closed");
            }
            assertEquals(FormatReader.NO_LIMIT, request.rowLimit());
            executeCount.incrementAndGet();
            entered.countDown();
            try {
                if (proceed.await(30, TimeUnit.SECONDS) == false) {
                    throw new AssertionError("timed out waiting to return connector cursor");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            if (closed.get()) {
                throw new IllegalStateException("Connector is closed");
            }
            return cursor();
        }

        @Override
        public void close() {
            closed.set(true);
            closeCount.incrementAndGet();
        }

        private static ResultCursor cursor() {
            return cursor(null, null, null);
        }

        private static ResultCursor cursor(AtomicInteger nextCalls, CountDownLatch enteredSecond, CountDownLatch proceedSecond) {
            return new ResultCursor() {
                private int emitted = 0;

                @Override
                public boolean hasNext() {
                    return emitted < (nextCalls == null ? 1 : 2);
                }

                @Override
                public Page next() {
                    if (hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    emitted++;
                    if (nextCalls != null) {
                        nextCalls.incrementAndGet();
                    }
                    if (emitted == 2 && enteredSecond != null) {
                        enteredSecond.countDown();
                        try {
                            if (proceedSecond.await(30, TimeUnit.SECONDS) == false) {
                                throw new AssertionError("timed out waiting to emit leftover connector page");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                    IntBlock block = TEST_BLOCK_FACTORY.newIntBlockBuilder(1).appendInt(1).build();
                    return new Page(block);
                }

                @Override
                public void close() {}
            };
        }
    }

    private static class TwoPageLatchedConnector implements Connector {
        private final AtomicInteger nextCalls;
        private final CountDownLatch enteredSecond;
        private final CountDownLatch proceedSecond;

        TwoPageLatchedConnector(AtomicInteger nextCalls, CountDownLatch enteredSecond, CountDownLatch proceedSecond) {
            this.nextCalls = nextCalls;
            this.enteredSecond = enteredSecond;
            this.proceedSecond = proceedSecond;
        }

        @Override
        public ResultCursor execute(QueryRequest request, Split split) {
            return LatchedConnector.cursor(nextCalls, enteredSecond, proceedSecond);
        }

        @Override
        public ResultCursor execute(QueryRequest request, ExternalSplit split) {
            assertEquals(FormatReader.NO_LIMIT, request.rowLimit());
            return LatchedConnector.cursor(nextCalls, enteredSecond, proceedSecond);
        }

        @Override
        public void close() {}
    }

    /**
     * Emits one single-row page per split, spending {@code burnNanos} of CPU time to produce it, and spends
     * {@code closeBurnNanos} to close.
     */
    private static class CpuBurningConnector implements Connector {
        private final long burnNanos;
        private final long closeBurnNanos;

        CpuBurningConnector(long burnNanos, long closeBurnNanos) {
            this.burnNanos = burnNanos;
            this.closeBurnNanos = closeBurnNanos;
        }

        @Override
        public ResultCursor execute(QueryRequest request, Split split) {
            return cursor();
        }

        @Override
        public ResultCursor execute(QueryRequest request, ExternalSplit split) {
            return cursor();
        }

        @Override
        public void close() {
            burnCpu(closeBurnNanos);
        }

        private ResultCursor cursor() {
            return new ResultCursor() {
                private boolean emitted;

                @Override
                public boolean hasNext() {
                    return emitted == false;
                }

                @Override
                public Page next() {
                    if (hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    emitted = true;
                    burnCpu(burnNanos);
                    return new Page(TEST_BLOCK_FACTORY.newConstantIntBlockWith(1, 1));
                }

                @Override
                public void close() {}
            };
        }

        private static void burnCpu(long nanos) {
            long start = ThreadCpuTimer.currentNanos();
            while (ThreadCpuTimer.currentNanos() - start < nanos) {
                Thread.onSpinWait();
            }
        }
    }

    private static class StubSplit implements ExternalSplit {
        private final String id;

        StubSplit(String id) {
            this.id = id;
        }

        @Override
        public String sourceType() {
            return "test";
        }

        @Override
        public String getWriteableName() {
            return "stub";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }
    }
}
