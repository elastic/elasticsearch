/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchSourceExec;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteFetchOperatorTests extends ESTestCase {
    private final List<BlockFactory> blockFactories = new ArrayList<>();

    public void testFetchesAcrossNodesAndReassemblesInInputOrder() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(
            new RemoteFetchService.FetchField("salary", DataType.INTEGER),
            new RemoteFetchService.FetchField("name", DataType.KEYWORD)
        );
        List<Attribute> outputFields = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER),
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD)
        );
        AtomicInteger requests = new AtomicInteger();
        RecordingClient client = new RecordingClient(driverContext) {
            @Override
            void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {
                requests.incrementAndGet();
                switch (nodeId) {
                    case "node-a" -> {
                        assertEquals("session-a", sessionId);
                        assertEquals(List.of(11, 33), handles.stream().map(RemoteFetchHandle::doc).toList());
                        enqueue(batchId, page(driverContext, 10, "a"), false);
                        enqueue(batchId, page(driverContext, 30, "c"), true);
                    }
                    case "node-b" -> {
                        assertEquals("session-b", sessionId);
                        assertEquals(List.of(22), handles.stream().map(RemoteFetchHandle::doc).toList());
                        enqueue(batchId, page(driverContext, 20, "b"), true);
                    }
                    default -> throw new IllegalStateException("unexpected node [" + nodeId + "]");
                }
            }
        };

        Page input = null;
        Page output = null;
        try (
            RemoteFetchOperator operator = new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                outputFields,
                null,
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        ) {
            input = new Page(handles(driverContext), carry(driverContext));
            operator.addInput(input);
            input = null;
            output = operator.getOutput();

            assertNotNull(output);
            assertEquals(4, output.getBlockCount());
            assertEquals(2, requests.get());

            IntBlock carryValues = output.getBlock(1);
            assertEquals(100, carryValues.getInt(0));
            assertEquals(200, carryValues.getInt(1));
            assertEquals(300, carryValues.getInt(2));

            IntBlock fetchedInts = output.getBlock(2);
            assertEquals(10, fetchedInts.getInt(0));
            assertEquals(20, fetchedInts.getInt(1));
            assertEquals(30, fetchedInts.getInt(2));

            BytesRefBlock fetchedStrings = output.getBlock(3);
            assertEquals("a", utf8(fetchedStrings, 0));
            assertEquals("b", utf8(fetchedStrings, 1));
            assertEquals("c", utf8(fetchedStrings, 2));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
            if (output != null) {
                output.releaseBlocks();
            }
        }
    }

    public void testFilteredFetchDropsRowsAndRemapsPositions() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(
            new RemoteFetchService.FetchField("salary", DataType.INTEGER),
            new RemoteFetchService.FetchField("name", DataType.KEYWORD)
        );
        List<Attribute> outputFields = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER),
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD)
        );
        RecordingClient client = new RecordingClient(driverContext) {
            @Override
            void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {
                switch (nodeId) {
                    case "node-a" -> {
                        // sent handles [h0, h1], filter keeps only h1 (original offset 1)
                        enqueue(batchId, pageWithPosition(driverContext, 30, "c", 1), true);
                    }
                    case "node-b" -> {
                        // sent handle [h0], filter keeps it (original offset 0)
                        enqueue(batchId, pageWithPosition(driverContext, 20, "b", 0), true);
                    }
                    default -> throw new IllegalStateException("unexpected node [" + nodeId + "]");
                }
            }
        };

        Page input = null;
        Page output = null;
        try (
            RemoteFetchOperator operator = new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                outputFields,
                pushdownPlan(),
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        ) {
            input = new Page(handles(driverContext), carry(driverContext));
            operator.addInput(input);
            input = null;
            output = operator.getOutput();

            assertNotNull(output);
            // row 0 (node-a, offset 0) was filtered out, so only 2 rows survive
            assertEquals(2, output.getPositionCount());
            assertEquals(4, output.getBlockCount());

            // carry column: row 1 (node-b, offset 0) and row 2 (node-a, offset 1) survive
            IntBlock carryValues = output.getBlock(1);
            assertEquals(200, carryValues.getInt(0));
            assertEquals(300, carryValues.getInt(1));

            IntBlock fetchedInts = output.getBlock(2);
            assertEquals(20, fetchedInts.getInt(0));
            assertEquals(30, fetchedInts.getInt(1));

            BytesRefBlock fetchedStrings = output.getBlock(3);
            assertEquals("b", utf8(fetchedStrings, 0));
            assertEquals("c", utf8(fetchedStrings, 1));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
            if (output != null) {
                output.releaseBlocks();
            }
        }
    }

    public void testStreamingFetchWaitsForAllGroupsBeforeConservativeOutput() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(new RemoteFetchService.FetchField("salary", DataType.INTEGER));
        List<Attribute> outputFields = List.of(new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER));
        RecordingClient client = new RecordingClient(driverContext) {
            @Override
            void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {
                if (nodeId.equals("node-a")) {
                    enqueue(batchId, intPage(driverContext, 10, 10), true);
                }
            }
        };

        Page input = null;
        Page output = null;
        try (
            RemoteFetchOperator operator = new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                outputFields,
                null,
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        ) {
            input = new Page(handles(driverContext), carry(driverContext));
            operator.addInput(input);
            input = null;

            assertNull("node-b has not completed yet, so conservative output must wait", operator.getOutput());

            client.enqueueOnly("node-b", intPage(driverContext, 20), true);
            output = operator.getOutput();

            assertNotNull(output);
            assertEquals(3, output.getPositionCount());
            IntBlock fetchedInts = output.getBlock(2);
            assertEquals(10, fetchedInts.getInt(0));
            assertEquals(20, fetchedInts.getInt(1));
            assertEquals(10, fetchedInts.getInt(2));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
            if (output != null) {
                output.releaseBlocks();
            }
        }
    }

    public void testFilteredFetchCanDropAllRowsInGroupAfterLastPageMarker() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(new RemoteFetchService.FetchField("salary", DataType.INTEGER));
        List<Attribute> outputFields = List.of(new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER));
        RecordingClient client = new RecordingClient(driverContext) {
            @Override
            void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {
                switch (nodeId) {
                    case "node-a" -> enqueueMarker(batchId);
                    case "node-b" -> enqueue(batchId, intPageWithPosition(driverContext, 20, 0), true);
                    default -> throw new IllegalStateException("unexpected node [" + nodeId + "]");
                }
            }
        };

        Page input = null;
        Page output = null;
        try (
            RemoteFetchOperator operator = new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                outputFields,
                pushdownPlan(),
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        ) {
            input = new Page(handles(driverContext), carry(driverContext));
            operator.addInput(input);
            input = null;
            output = operator.getOutput();

            assertNotNull(output);
            assertEquals(1, output.getPositionCount());

            IntBlock carryValues = output.getBlock(1);
            assertEquals(200, carryValues.getInt(0));

            IntBlock fetchedInts = output.getBlock(2);
            assertEquals(20, fetchedInts.getInt(0));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
            if (output != null) {
                output.releaseBlocks();
            }
        }
    }

    public void testPushdownRequiresMappedResponsePages() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(new RemoteFetchService.FetchField("salary", DataType.INTEGER));
        List<Attribute> outputFields = List.of(new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER));
        RecordingClient client = new RecordingClient(driverContext) {
            @Override
            void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {
                switch (nodeId) {
                    case "node-a" -> enqueue(batchId, intPage(driverContext, 10, 30), true);
                    case "node-b" -> enqueue(batchId, intPage(driverContext, 20), true);
                    default -> throw new IllegalStateException("unexpected node [" + nodeId + "]");
                }
            }
        };

        Page input = null;
        try (
            RemoteFetchOperator operator = new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                outputFields,
                nonFilteringPushdownPlan(),
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        ) {
            input = new Page(handles(driverContext), carry(driverContext));
            operator.addInput(input);
            input = null;

            IllegalStateException exception = expectThrows(IllegalStateException.class, operator::getOutput);
            assertThat(exception.getMessage(), containsString("remote fetch returned plain response pages for a pushdown fetch"));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
        }
    }

    public void testPlainFetchRejectsMappedResponsePages() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(new RemoteFetchService.FetchField("salary", DataType.INTEGER));
        List<Attribute> outputFields = List.of(new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER));
        RecordingClient client = new RecordingClient(driverContext) {
            @Override
            void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {
                switch (nodeId) {
                    case "node-a" -> enqueue(batchId, intPageWithPosition(driverContext, 10, 0), false);
                    case "node-b" -> enqueue(batchId, intPageWithPosition(driverContext, 20, 0), true);
                    default -> throw new IllegalStateException("unexpected node [" + nodeId + "]");
                }
            }
        };

        Page input = null;
        try (
            RemoteFetchOperator operator = new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                outputFields,
                null,
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        ) {
            input = new Page(handles(driverContext), carry(driverContext));
            operator.addInput(input);
            input = null;

            IllegalStateException exception = expectThrows(IllegalStateException.class, operator::getOutput);
            assertThat(exception.getMessage(), containsString("remote fetch returned mapped response pages for a plain fetch"));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
        }
    }

    public void testInvalidRequestShapesRejected() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(new RemoteFetchService.FetchField("salary", DataType.INTEGER));
        List<Attribute> outputFields = List.of(new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER));
        RecordingClient client = new RecordingClient(driverContext) {
            @Override
            void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {}
        };

        IllegalArgumentException noRequestFields = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                List.of(),
                outputFields,
                null,
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        );
        assertThat(noRequestFields.getMessage(), containsString("remote fetch requires at least one request field"));

        IllegalArgumentException noOutputFields = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                List.of(),
                null,
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        );
        assertThat(noOutputFields.getMessage(), containsString("remote fetch requires at least one output field"));
    }

    public void testUnsupportedPushdownRejectedAtOperatorConstruction() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(new RemoteFetchService.FetchField("salary", DataType.INTEGER));
        ReferenceAttribute outputField = new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER);
        ReferenceAttribute positionAttribute = new ReferenceAttribute(Source.EMPTY, null, "_remote_fetch_position", DataType.INTEGER);
        PhysicalPlan unsupportedPushdown = new LimitExec(
            Source.EMPTY,
            new RemoteFetchSourceExec(Source.EMPTY, List.of(outputField, positionAttribute)),
            new Literal(Source.EMPTY, 10, DataType.INTEGER),
            null
        );
        RecordingClient client = new RecordingClient(driverContext) {
            @Override
            void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {}
        };

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                List.of(outputField),
                unsupportedPushdown,
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        );
        assertThat(exception.getMessage(), containsString("unsupported remote fetch pushdown plan [LimitExec]"));
    }

    public void testMappedFetchRejectsInvalidPositionMappings() {
        record InvalidPositionCase(String name, String message) {}

        for (InvalidPositionCase testCase : List.of(
            new InvalidPositionCase("null", "cannot contain nulls"),
            new InvalidPositionCase("multivalue", "exactly one position"),
            new InvalidPositionCase("negative", "out of range"),
            new InvalidPositionCase("too_large", "out of range")
        )) {
            DriverContext driverContext = driverContext();
            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            List<RemoteFetchService.FetchField> fields = List.of(new RemoteFetchService.FetchField("salary", DataType.INTEGER));
            List<Attribute> outputFields = List.of(new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER));
            RecordingClient client = new RecordingClient(driverContext) {
                @Override
                void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {
                    switch (nodeId) {
                        case "node-a" -> {
                            if (handles.size() == 2) {
                                enqueue(batchId, invalidPositionPage(driverContext, testCase.name), true);
                            } else {
                                throw new IllegalStateException("unexpected handle count [" + handles.size() + "]");
                            }
                        }
                        case "node-b" -> enqueue(batchId, intPageWithPosition(driverContext, 20, 0), true);
                        default -> throw new IllegalStateException("unexpected node [" + nodeId + "]");
                    }
                }
            };

            Page input = null;
            try (
                RemoteFetchOperator operator = new RemoteFetchOperator(
                    driverContext,
                    threadContext,
                    0,
                    fields,
                    outputFields,
                    pushdownPlan(),
                    ConfigurationAware.CONFIGURATION_MARKER,
                    2,
                    client
                )
            ) {
                input = new Page(handles(driverContext), carry(driverContext));
                operator.addInput(input);
                input = null;

                IllegalStateException exception = expectThrows(IllegalStateException.class, operator::getOutput);
                assertThat(exception.getMessage(), containsString(testCase.message));
            } finally {
                if (input != null) {
                    input.releaseBlocks();
                }
            }
        }
    }

    public void testAddInputFailureKeepsOperatorUnfinishedUntilFailureIsThrown() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(new RemoteFetchService.FetchField("salary", DataType.INTEGER));
        List<Attribute> outputFields = List.of(new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER));
        RecordingClient client = new RecordingClient(driverContext) {
            @Override
            void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {
                throw new AssertionError("input decoding should fail before a batch is sent");
            }
        };

        Page input = null;
        try (
            RemoteFetchOperator operator = new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                outputFields,
                null,
                ConfigurationAware.CONFIGURATION_MARKER,
                2,
                client
            )
        ) {
            input = new Page(handlesWithNull(driverContext));
            operator.addInput(input);
            input = null;

            assertFalse(operator.isFinished());
            expectThrows(IllegalStateException.class, operator::getOutput);
            assertFalse(operator.isFinished());
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
        }
    }

    public void testIsBlockedWaitsForRemoteFetchEvenWhenMoreInputCanBeAccepted() {
        DriverContext driverContext = driverContext();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        List<RemoteFetchService.FetchField> fields = List.of(new RemoteFetchService.FetchField("salary", DataType.INTEGER));
        List<Attribute> outputFields = List.of(new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER));

        try (
            RemoteFetchOperator operator = new RemoteFetchOperator(
                driverContext,
                threadContext,
                0,
                fields,
                outputFields,
                pushdownPlan(),
                ConfigurationTestUtils.randomConfiguration(),
                2,
                new RecordingClient(driverContext) {
                    @Override
                    void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles) {}
                }
            )
        ) {
            operator.addInput(new Page(handles(driverContext), carry(driverContext)));
            assertTrue(operator.needsInput());

            IsBlockedResult blocked = operator.isBlocked();
            assertFalse(blocked.listener().isDone());
            assertThat(blocked.reason(), containsString("remote fetch"));
        }
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(4)).withCircuitBreaking();
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).build();
        blockFactories.add(blockFactory);
        return new DriverContext(bigArrays, blockFactory, null);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            for (BlockFactory blockFactory : blockFactories) {
                assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
                assertThat(blockFactory.breaker().getTrippedCount(), equalTo(0L));
                assertThat(blockFactory.breaker().getName(), equalTo(CircuitBreaker.REQUEST));
            }
            MockBigArrays.ensureAllArraysAreReleased();
        } finally {
            super.tearDown();
        }
    }

    private static Page page(DriverContext driverContext, int intValue, String stringValue) {
        try (
            IntBlock.Builder intBuilder = driverContext.blockFactory().newIntBlockBuilder(1);
            BytesRefBlock.Builder stringBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(1)
        ) {
            intBuilder.appendInt(intValue);
            stringBuilder.appendBytesRef(new BytesRef(stringValue));
            return new Page(intBuilder.build(), stringBuilder.build());
        }
    }

    private static Page intPage(DriverContext driverContext, int... intValues) {
        try (IntBlock.Builder intBuilder = driverContext.blockFactory().newIntBlockBuilder(intValues.length)) {
            for (int intValue : intValues) {
                intBuilder.appendInt(intValue);
            }
            return new Page(intBuilder.build());
        }
    }

    private static Page intPageWithPosition(DriverContext driverContext, int intValue, int originalPosition) {
        try (
            IntBlock.Builder intBuilder = driverContext.blockFactory().newIntBlockBuilder(1);
            IntBlock.Builder posBuilder = driverContext.blockFactory().newIntBlockBuilder(1)
        ) {
            intBuilder.appendInt(intValue);
            posBuilder.appendInt(originalPosition);
            return new Page(intBuilder.build(), posBuilder.build());
        }
    }

    private static Page invalidPositionPage(DriverContext driverContext, String name) {
        return switch (name) {
            case "null" -> intPageWithPositionNull(driverContext);
            case "multivalue" -> intPageWithMultivaluePosition(driverContext);
            case "negative" -> intPageWithPosition(driverContext, 10, -1);
            case "too_large" -> intPageWithPosition(driverContext, 10, 2);
            default -> throw new IllegalArgumentException("unknown invalid position case [" + name + "]");
        };
    }

    private static Page intPageWithPositionNull(DriverContext driverContext) {
        try (
            IntBlock.Builder intBuilder = driverContext.blockFactory().newIntBlockBuilder(1);
            IntBlock.Builder posBuilder = driverContext.blockFactory().newIntBlockBuilder(1)
        ) {
            intBuilder.appendInt(10);
            posBuilder.appendNull();
            return new Page(intBuilder.build(), posBuilder.build());
        }
    }

    private static Page intPageWithMultivaluePosition(DriverContext driverContext) {
        try (
            IntBlock.Builder intBuilder = driverContext.blockFactory().newIntBlockBuilder(1);
            IntBlock.Builder posBuilder = driverContext.blockFactory().newIntBlockBuilder(1)
        ) {
            intBuilder.appendInt(10);
            posBuilder.beginPositionEntry();
            posBuilder.appendInt(0);
            posBuilder.appendInt(1);
            posBuilder.endPositionEntry();
            return new Page(intBuilder.build(), posBuilder.build());
        }
    }

    private static Page pageWithPosition(DriverContext driverContext, int intValue, String stringValue, int originalPosition) {
        try (
            IntBlock.Builder intBuilder = driverContext.blockFactory().newIntBlockBuilder(1);
            BytesRefBlock.Builder stringBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(1);
            IntBlock.Builder posBuilder = driverContext.blockFactory().newIntBlockBuilder(1)
        ) {
            intBuilder.appendInt(intValue);
            stringBuilder.appendBytesRef(new BytesRef(stringValue));
            posBuilder.appendInt(originalPosition);
            return new Page(intBuilder.build(), stringBuilder.build(), posBuilder.build());
        }
    }

    private static BytesRefBlock handles(DriverContext driverContext) {
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new RemoteFetchHandle("node-a", "session-a", 1, 0, 11).toBytesRef());
            builder.appendBytesRef(new RemoteFetchHandle("node-b", "session-b", 2, 0, 22).toBytesRef());
            builder.appendBytesRef(new RemoteFetchHandle("node-a", "session-a", 1, 0, 33).toBytesRef());
            return builder.build();
        }
    }

    private static BytesRefBlock handlesWithNull(DriverContext driverContext) {
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(1)) {
            builder.appendNull();
            return builder.build();
        }
    }

    private static IntBlock carry(DriverContext driverContext) {
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(3)) {
            builder.appendInt(100);
            builder.appendInt(200);
            builder.appendInt(300);
            return builder.build();
        }
    }

    private static String utf8(BytesRefBlock block, int position) {
        return block.getBytesRef(block.getFirstValueIndex(position), new BytesRef()).utf8ToString();
    }

    private static PhysicalPlan pushdownPlan() {
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER);
        ReferenceAttribute positionAttribute = new ReferenceAttribute(Source.EMPTY, null, "_remote_fetch_position", DataType.INTEGER);
        return new FilterExec(
            Source.EMPTY,
            new RemoteFetchSourceExec(Source.EMPTY, List.of(fetchedAttribute, positionAttribute)),
            new GreaterThan(Source.EMPTY, fetchedAttribute, new Literal(Source.EMPTY, 0, DataType.INTEGER))
        );
    }

    private static PhysicalPlan nonFilteringPushdownPlan() {
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "salary", DataType.INTEGER);
        ReferenceAttribute positionAttribute = new ReferenceAttribute(Source.EMPTY, null, "_remote_fetch_position", DataType.INTEGER);
        return new ProjectExec(
            Source.EMPTY,
            new RemoteFetchSourceExec(Source.EMPTY, List.of(fetchedAttribute, positionAttribute)),
            List.of(fetchedAttribute)
        );
    }

    private abstract static class RecordingClient implements RemoteFetchService.Client {
        private final Map<String, RecordingExchange> exchangesByNode = new HashMap<>();
        private RecordingExchange currentExchange;

        RecordingClient(DriverContext driverContext) {}

        @Override
        public RemoteFetchService.Exchange openExchange(
            String nodeId,
            String sessionId,
            List<RemoteFetchService.FetchField> fields,
            org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan pushdownPlan,
            Configuration configuration
        ) {
            RecordingExchange exchange = new RecordingExchange(nodeId, sessionId);
            exchangesByNode.put(nodeId, exchange);
            return exchange;
        }

        void enqueue(long batchId, Page page, boolean lastPage) {
            currentExchange.enqueue(batchId, page, lastPage);
        }

        void enqueueMarker(long batchId) {
            currentExchange.enqueueMarker(batchId);
        }

        void enqueueOnly(String nodeId, Page page, boolean lastPage) {
            RecordingExchange exchange = exchangesByNode.get(nodeId);
            assertNotNull(exchange);
            exchange.enqueue(exchange.batchId, page, lastPage);
        }

        abstract void onBatch(String nodeId, String sessionId, long batchId, List<RemoteFetchHandle> handles);

        @Override
        public void close() {}

        private class RecordingExchange implements RemoteFetchService.Exchange {
            private final String nodeId;
            private final String sessionId;
            private final Queue<Page> pages = new ArrayDeque<>();
            private final SubscribableListener<Void> pageReady = new SubscribableListener<>();
            private long batchId;

            RecordingExchange(String nodeId, String sessionId) {
                this.nodeId = nodeId;
                this.sessionId = sessionId;
            }

            @Override
            public void sendBatch(long batchId, List<RemoteFetchHandle> handles) {
                this.batchId = batchId;
                currentExchange = this;
                try {
                    onBatch(nodeId, sessionId, batchId, new ArrayList<>(handles));
                } finally {
                    currentExchange = null;
                }
            }

            void enqueue(long batchId, Page page, boolean lastPage) {
                Block[] blocks = new Block[page.getBlockCount()];
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = page.getBlock(i);
                    blocks[i].incRef();
                }
                page.releaseBlocks();
                pages.add(new Page(new BatchMetadata(batchId, 0, lastPage), blocks));
            }

            void enqueueMarker(long batchId) {
                pages.add(Page.createBatchMarkerPage(batchId, 0));
            }

            @Override
            public Page pollPage() {
                return pages.poll();
            }

            @Override
            public IsBlockedResult isBlocked() {
                if (pages.isEmpty()) {
                    return new IsBlockedResult(pageReady, "remote fetch response");
                }
                return Operator.NOT_BLOCKED;
            }

            @Override
            public Exception getFailure() {
                return null;
            }

            @Override
            public void markBatchCompleted(long batchId) {}

            @Override
            public void finish() {}

            @Override
            public boolean isFinished() {
                return true;
            }

            @Override
            public IsBlockedResult waitForCompletion() {
                return Operator.NOT_BLOCKED;
            }

            @Override
            public void close() {
                for (Page page : pages) {
                    page.releaseBlocks();
                }
                pages.clear();
            }
        }
    }
}
