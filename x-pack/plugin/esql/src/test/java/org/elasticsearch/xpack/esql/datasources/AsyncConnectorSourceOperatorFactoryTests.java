/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Observed-LIMIT stop for {@link AsyncConnectorSourceOperatorFactory}: remaining 0 must stop
 * claiming further splits. Connectors do not reserve a pushed budget.
 */
public class AsyncConnectorSourceOperatorFactoryTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
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

    private static DriverContext mockDriverContext() {
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
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

        LatchedConnector(AtomicInteger executeCount, CountDownLatch entered, CountDownLatch proceed) {
            this.executeCount = executeCount;
            this.entered = entered;
            this.proceed = proceed;
        }

        @Override
        public ResultCursor execute(QueryRequest request, Split split) {
            return cursor();
        }

        @Override
        public ResultCursor execute(QueryRequest request, ExternalSplit split) {
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
            return cursor();
        }

        @Override
        public void close() {}

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
