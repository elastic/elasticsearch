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
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link AsyncConnectorSourceOperatorFactory} correctly routes
 * {@link ExternalSplit}s from the slice queue to the connector's
 * {@link Connector#execute(QueryRequest, ExternalSplit)} overload, and uses
 * {@link Split#SINGLE} via the legacy overload when no queue is present.
 */
public class AsyncConnectorSourceOperatorFactorySplitTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    public void testExternalSplitsPassedFromSliceQueue() throws Exception {
        List<ExternalSplit> receivedSplits = new CopyOnWriteArrayList<>();
        StubExternalSplit splitA = new StubExternalSplit("a");
        StubExternalSplit splitB = new StubExternalSplit("b");
        StubExternalSplit splitC = new StubExternalSplit("c");

        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(List.of(splitA, splitB, splitC));

        SplitCapturingConnector connector = new SplitCapturingConnector(receivedSplits);
        QueryRequest baseRequest = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);

        AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(
            connector,
            baseRequest,
            10,
            Runnable::run,
            sliceQueue
        );

        DriverContext driverContext = mockDriverContext();
        SourceOperator operator = factory.get(driverContext);

        drainOperator(operator);
        operator.close();

        assertEquals(3, receivedSplits.size());
        assertSame(splitA, receivedSplits.get(0));
        assertSame(splitB, receivedSplits.get(1));
        assertSame(splitC, receivedSplits.get(2));
    }

    public void testNoSliceQueueUsesSplitSingle() throws Exception {
        List<Split> receivedSplits = new CopyOnWriteArrayList<>();
        LegacySplitCapturingConnector connector = new LegacySplitCapturingConnector(receivedSplits);
        QueryRequest baseRequest = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);

        AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(connector, baseRequest, 10, Runnable::run);

        DriverContext driverContext = mockDriverContext();
        SourceOperator operator = factory.get(driverContext);

        drainOperator(operator);
        operator.close();

        assertEquals(1, receivedSplits.size());
        assertSame(Split.SINGLE, receivedSplits.get(0));
    }

    public void testDefaultExternalSplitOverloadDelegatesToLegacy() {
        List<Split> receivedSplits = new CopyOnWriteArrayList<>();
        LegacySplitCapturingConnector connector = new LegacySplitCapturingConnector(receivedSplits);

        QueryRequest request = new QueryRequest("t", List.of(), List.of(), Map.of(), 10, TEST_BLOCK_FACTORY);
        StubExternalSplit split = new StubExternalSplit("test");
        ResultCursor cursor = connector.execute(request, split);
        assertNotNull(cursor);
        assertFalse(cursor.hasNext());

        assertEquals(1, receivedSplits.size());
        assertSame(Split.SINGLE, receivedSplits.get(0));
    }

    // ===== Lifecycle tests =====

    /**
     * Single-split connector path: removeAsyncAction fires exactly once on success.
     */
    public void testSingleSplitRemoveAsyncActionExactlyOnce() throws Exception {
        List<Split> receivedSplits = new CopyOnWriteArrayList<>();
        LegacySplitCapturingConnector connector = new LegacySplitCapturingConnector(receivedSplits);
        QueryRequest baseRequest = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        doAnswer(inv -> {
            addCount.incrementAndGet();
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            removeCount.incrementAndGet();
            return null;
        }).when(driverContext).removeAsyncAction();

        AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(connector, baseRequest, 10, Runnable::run);

        SourceOperator operator = factory.get(driverContext);
        drainOperator(operator);
        operator.close();

        assertEquals("addAsyncAction should be called exactly once", 1, addCount.get());
        assertEquals("removeAsyncAction should be called exactly once", 1, removeCount.get());
    }

    /**
     * Slice-queue connector path: removeAsyncAction fires exactly once for all splits.
     */
    public void testSliceQueueRemoveAsyncActionExactlyOnce() throws Exception {
        List<ExternalSplit> receivedSplits = new CopyOnWriteArrayList<>();
        SplitCapturingConnector connector = new SplitCapturingConnector(receivedSplits);
        QueryRequest baseRequest = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);

        StubExternalSplit splitA = new StubExternalSplit("a");
        StubExternalSplit splitB = new StubExternalSplit("b");
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(List.of(splitA, splitB));

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        doAnswer(inv -> {
            addCount.incrementAndGet();
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            removeCount.incrementAndGet();
            return null;
        }).when(driverContext).removeAsyncAction();

        AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(
            connector,
            baseRequest,
            10,
            Runnable::run,
            sliceQueue
        );

        SourceOperator operator = factory.get(driverContext);
        drainOperator(operator);
        operator.close();

        assertEquals("addAsyncAction should be called exactly once", 1, addCount.get());
        assertEquals("removeAsyncAction should be called exactly once after all splits", 1, removeCount.get());
        assertEquals(2, receivedSplits.size());
    }

    /**
     * Connector with real thread pool: exercises backpressure.
     */
    public void testConnectorBackpressureWithRealThreadPool() throws Exception {
        ExecutorService realExec = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "conn-test"));
        try {
            List<ExternalSplit> receivedSplits = new CopyOnWriteArrayList<>();
            MultiPageSplitConnector connector = new MultiPageSplitConnector(receivedSplits, 10);
            QueryRequest baseRequest = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);

            StubExternalSplit splitA = new StubExternalSplit("a");
            StubExternalSplit splitB = new StubExternalSplit("b");
            ExternalSliceQueue sliceQueue = new ExternalSliceQueue(List.of(splitA, splitB));

            DriverContext driverContext = mockDriverContext();

            AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(
                connector,
                baseRequest,
                2,
                realExec,
                sliceQueue
            );

            SourceOperator operator = factory.get(driverContext);
            List<Page> pages = new ArrayList<>();

            long deadline = System.currentTimeMillis() + 30_000;
            while (operator.isFinished() == false && System.currentTimeMillis() < deadline) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                } else {
                    Thread.sleep(10);
                }
            }
            assertTrue("Operator should complete within timeout", operator.isFinished());
            assertEquals(2, receivedSplits.size());
            assertEquals(20, pages.size());

            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        } finally {
            realExec.shutdown();
            assertTrue(realExec.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    // ===== Helpers =====

    private static DriverContext mockDriverContext() {
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
        return driverContext;
    }

    private static void drainOperator(SourceOperator operator) {
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }
        for (Page p : pages) {
            p.releaseBlocks();
        }
    }

    private static Page createTestPage() {
        IntBlock block = TEST_BLOCK_FACTORY.newIntBlockBuilder(1).appendInt(42).build();
        return new Page(block);
    }

    private static ResultCursor singlePageResultCursor() {
        return new ResultCursor() {
            private boolean consumed = false;

            @Override
            public boolean hasNext() {
                return consumed == false;
            }

            @Override
            public Page next() {
                if (consumed) {
                    throw new NoSuchElementException();
                }
                consumed = true;
                return createTestPage();
            }

            @Override
            public void close() {}
        };
    }

    private static ResultCursor emptyResultCursor() {
        return new ResultCursor() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };
    }

    /**
     * Connector that overrides the ExternalSplit overload and records received splits.
     */
    private static class SplitCapturingConnector implements Connector {
        private final List<ExternalSplit> receivedSplits;

        SplitCapturingConnector(List<ExternalSplit> receivedSplits) {
            this.receivedSplits = receivedSplits;
        }

        @Override
        public ResultCursor execute(QueryRequest request, Split split) {
            return singlePageResultCursor();
        }

        @Override
        public ResultCursor execute(QueryRequest request, ExternalSplit split) {
            receivedSplits.add(split);
            return singlePageResultCursor();
        }

        @Override
        public void close() {}
    }

    /**
     * Connector that does NOT override the ExternalSplit overload — uses the
     * default delegation to execute(QueryRequest, Split). Records Split instances.
     */
    private static class LegacySplitCapturingConnector implements Connector {
        private final List<Split> receivedSplits;

        LegacySplitCapturingConnector(List<Split> receivedSplits) {
            this.receivedSplits = receivedSplits;
        }

        @Override
        public ResultCursor execute(QueryRequest request, Split split) {
            receivedSplits.add(split);
            return emptyResultCursor();
        }

        @Override
        public void close() {}
    }

    /**
     * Connector that returns multi-page results for ExternalSplit queries.
     */
    private static class MultiPageSplitConnector implements Connector {
        private final List<ExternalSplit> receivedSplits;
        private final int pagesPerSplit;

        MultiPageSplitConnector(List<ExternalSplit> receivedSplits, int pagesPerSplit) {
            this.receivedSplits = receivedSplits;
            this.pagesPerSplit = pagesPerSplit;
        }

        @Override
        public ResultCursor execute(QueryRequest request, Split split) {
            return multiPageResultCursor(pagesPerSplit);
        }

        @Override
        public ResultCursor execute(QueryRequest request, ExternalSplit split) {
            receivedSplits.add(split);
            return multiPageResultCursor(pagesPerSplit);
        }

        @Override
        public void close() {}

        private static ResultCursor multiPageResultCursor(int pageCount) {
            return new ResultCursor() {
                private int remaining = pageCount;

                @Override
                public boolean hasNext() {
                    return remaining > 0;
                }

                @Override
                public Page next() {
                    if (remaining <= 0) {
                        throw new NoSuchElementException();
                    }
                    remaining--;
                    return createTestPage();
                }

                @Override
                public void close() {}
            };
        }
    }

    /**
     * Minimal ExternalSplit implementation for testing.
     */
    private static class StubExternalSplit implements ExternalSplit {
        private final String id;

        StubExternalSplit(String id) {
            this.id = id;
        }

        @Override
        public String sourceType() {
            return "test";
        }

        @Override
        public String getWriteableName() {
            return "stub_split";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        @Override
        public String toString() {
            return "StubExternalSplit[" + id + "]";
        }
    }
}
