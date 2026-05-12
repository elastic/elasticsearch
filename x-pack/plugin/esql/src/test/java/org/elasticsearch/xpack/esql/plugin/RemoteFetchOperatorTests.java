/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteFetchOperatorTests extends ESTestCase {
    public void testFetchesAcrossNodesAndReassemblesInInputOrder() {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
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
        RemoteFetchOperator.Client client = (nodeId, request, listener) -> {
            requests.incrementAndGet();
            switch (nodeId) {
                case "node-a" -> {
                    assertEquals("session-a", request.sessionId());
                    assertEquals(List.of(11, 33), request.handles().stream().map(RemoteFetchHandle::doc).toList());
                    listener.onResponse(List.of(page(driverContext, 10, "a"), page(driverContext, 30, "c")));
                }
                case "node-b" -> {
                    assertEquals("session-b", request.sessionId());
                    assertEquals(List.of(22), request.handles().stream().map(RemoteFetchHandle::doc).toList());
                    listener.onResponse(List.of(page(driverContext, 20, "b")));
                }
                default -> listener.onFailure(new IllegalStateException("unexpected node [" + nodeId + "]"));
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

    private static BytesRefBlock handles(DriverContext driverContext) {
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new RemoteFetchHandle("node-a", "session-a", 1, 0, 11).toBytesRef());
            builder.appendBytesRef(new RemoteFetchHandle("node-b", "session-b", 2, 0, 22).toBytesRef());
            builder.appendBytesRef(new RemoteFetchHandle("node-a", "session-a", 1, 0, 33).toBytesRef());
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
}
