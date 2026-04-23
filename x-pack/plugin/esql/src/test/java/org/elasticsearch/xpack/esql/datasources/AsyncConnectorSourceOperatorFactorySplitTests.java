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

    /**
     * State-machine guard test for the flattened producer loop.
     *
     * Drives the slice-queue producer end-to-end with 4 splits (1 page per split) on a
     * single-thread executor and asserts:
     * <ul>
     *   <li>all 4 splits are read once (state machine visits every split);</li>
     *   <li>all 4 pages arrive at the operator;</li>
     *   <li>every opened {@link ResultCursor} is closed exactly once;</li>
     *   <li>{@code removeAsyncAction()} fires exactly once (success path);</li>
     *   <li>the connector is closed exactly once.</li>
     * </ul>
     * Followed by a second scenario that forces early cancellation via {@link SourceOperator#finish()}
     * partway through, to verify the active cursor gets closed and no further splits are opened.
     * <p>
     * BLOCKED-path coverage (buffer-full backpressure and wakeup correctness) is not exercised here;
     * it lives in {@code AsyncExternalSourceBufferTests#testNoLostWakeupUnderConcurrentAddAndPoll}.
     */
    public void testProducerLoopStateMachine() throws Exception {
        // --- scenario 1: run to completion across 4 splits ---
        runStateMachineScenario(false);

        // --- scenario 2: force early cancellation after first page ---
        runStateMachineScenario(true);
    }

    private void runStateMachineScenario(boolean forceEarlyCancellation) throws Exception {
        AtomicInteger readCalls = new AtomicInteger();
        AtomicInteger closeCalls = new AtomicInteger();
        AtomicInteger connectorCloseCalls = new AtomicInteger();
        TrackingConnector connector = new TrackingConnector(readCalls, closeCalls, connectorCloseCalls, 1);

        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            splits.add(new StubExternalSplit("s" + i));
        }
        ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        AtomicInteger addAsync = new AtomicInteger();
        AtomicInteger removeAsync = new AtomicInteger();
        doAnswer(inv -> {
            addAsync.incrementAndGet();
            return null;
        }).when(driverContext).addAsyncAction();
        doAnswer(inv -> {
            removeAsync.incrementAndGet();
            return null;
        }).when(driverContext).removeAsyncAction();

        QueryRequest baseRequest = new QueryRequest("target", List.of("col"), List.of(), Map.of(), 100, TEST_BLOCK_FACTORY);

        // Use a single-thread executor so producer ordering is deterministic.
        ExecutorService executor = Executors.newSingleThreadExecutor(EsExecutors.daemonThreadFactory("test", "sm-test"));
        try {
            AsyncConnectorSourceOperatorFactory factory = new AsyncConnectorSourceOperatorFactory(
                connector,
                baseRequest,
                10,
                executor,
                sliceQueue
            );

            SourceOperator operator = factory.get(driverContext);

            if (forceEarlyCancellation) {
                // Consume at least one page to ensure a cursor is open, then cancel.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                int received = 0;
                while (received < 1 && System.nanoTime() < deadline) {
                    Page p = operator.getOutput();
                    if (p != null) {
                        received++;
                        p.releaseBlocks();
                    }
                }
                operator.finish();
                while (operator.isFinished() == false) {
                    Page p = operator.getOutput();
                    if (p != null) p.releaseBlocks();
                }
            } else {
                List<Page> pages = new ArrayList<>();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (operator.isFinished() == false && System.nanoTime() < deadline) {
                    Page p = operator.getOutput();
                    if (p != null) pages.add(p);
                }
                assertTrue("operator did not finish within timeout", operator.isFinished());
                assertEquals("all 4 splits produced a page", 4, pages.size());
                for (Page p : pages) {
                    p.releaseBlocks();
                }
            }

            operator.close();

            // Wait for the producer thread to run its completion listener (which closes the
            // connector and calls removeAsyncAction) before asserting on counters.
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

            // Every cursor that was opened must have been closed exactly once.
            assertEquals("read/close counts differ - leaked cursor", readCalls.get(), closeCalls.get());
            // addAsync/removeAsync are paired: exactly once, on the happy path and cancelled path.
            assertEquals(1, addAsync.get());
            assertEquals(1, removeAsync.get());
            // Connector.close() is invoked exactly once by the completion listener.
            assertEquals("connector must be closed exactly once", 1, connectorCloseCalls.get());
            if (forceEarlyCancellation == false) {
                assertEquals(4, readCalls.get());
            } else {
                assertTrue("expected at least one split to be read before cancellation", readCalls.get() >= 1);
            }
        } finally {
            if (executor.isShutdown() == false) {
                executor.shutdown();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
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
     * Connector for the state-machine test. Every {@code execute(request, split)} returns a new
     * cursor that emits {@code pagesPerCursor} pages and increments {@code closeCount} on
     * {@link ResultCursor#close()}, so the test can assert that every opened cursor is closed
     * exactly once. Tracks {@link #close()} separately to verify one-shot connector cleanup.
     */
    private static class TrackingConnector implements Connector {
        private final AtomicInteger readCount;
        private final AtomicInteger closeCount;
        private final AtomicInteger connectorCloseCount;
        private final int pagesPerCursor;

        TrackingConnector(AtomicInteger readCount, AtomicInteger closeCount, AtomicInteger connectorCloseCount, int pagesPerCursor) {
            this.readCount = readCount;
            this.closeCount = closeCount;
            this.connectorCloseCount = connectorCloseCount;
            this.pagesPerCursor = pagesPerCursor;
        }

        @Override
        public ResultCursor execute(QueryRequest request, Split split) {
            return newCursor();
        }

        @Override
        public ResultCursor execute(QueryRequest request, ExternalSplit split) {
            return newCursor();
        }

        @Override
        public void close() {
            connectorCloseCount.incrementAndGet();
        }

        private ResultCursor newCursor() {
            readCount.incrementAndGet();
            return new ResultCursor() {
                private int remaining = pagesPerCursor;

                @Override
                public boolean hasNext() {
                    return remaining > 0;
                }

                @Override
                public Page next() {
                    if (remaining <= 0) throw new NoSuchElementException();
                    remaining--;
                    return createTestPage();
                }

                @Override
                public void close() {
                    closeCount.incrementAndGet();
                }
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
