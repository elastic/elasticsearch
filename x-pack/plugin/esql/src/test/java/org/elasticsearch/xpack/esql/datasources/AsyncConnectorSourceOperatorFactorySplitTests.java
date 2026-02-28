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

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("test"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

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
