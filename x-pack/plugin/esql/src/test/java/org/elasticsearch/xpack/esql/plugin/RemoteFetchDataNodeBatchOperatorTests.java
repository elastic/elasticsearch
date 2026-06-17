/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.SequenceBytesRefBlockSourceOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

public class RemoteFetchDataNodeBatchOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceBytesRefBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(i -> handle("node-1", "session-1", i).toBytesRef())
        );
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new Operator.OperatorFactory() {
            @Override
            public Operator get(DriverContext driverContext) {
                return new RemoteFetchDataNodeBatchOperator(handles -> List.of(docsPage(driverContext, handles)));
            }

            @Override
            public String describe() {
                return "RemoteFetchDataNodeBatchOperator[]";
            }
        };
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<Integer> expectedDocs = docsFromHandlePages(input);
        List<Integer> actualDocs = docsFromOutputPages(results);
        assertThat(actualDocs, equalTo(expectedDocs));
    }

    @Override
    protected org.hamcrest.Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("RemoteFetchDataNodeBatchOperator[]");
    }

    @Override
    protected org.hamcrest.Matcher<String> expectedToStringOfSimple() {
        return matchesPattern(".*RemoteFetchDataNodeBatchOperator.*");
    }

    public void testFetchedPagesArePlainPagesForBatchDriverSinkWrapping() {
        DriverContext driverContext = driverContext();
        try (
            RemoteFetchDataNodeBatchOperator operator = new RemoteFetchDataNodeBatchOperator(
                handles -> List.of(singleIntPage(driverContext, 10), singleIntPage(driverContext, 20))
            )
        ) {
            operator.addInput(handlesPage(driverContext));
            Page first = operator.getOutput();
            Page second = operator.getOutput();
            try {
                assertNull(first.batchMetadata());
                assertNull(second.batchMetadata());
                assertThat(first.<IntBlock>getBlock(0).getInt(0), equalTo(10));
                assertThat(second.<IntBlock>getBlock(0).getInt(0), equalTo(20));
            } finally {
                first.releaseBlocks();
                second.releaseBlocks();
            }
        }
    }

    public void testEmptyFetchEmitsNoPagesForBatchDriverSinkWrapping() {
        DriverContext driverContext = driverContext();
        try (RemoteFetchDataNodeBatchOperator operator = new RemoteFetchDataNodeBatchOperator(handles -> List.of())) {
            operator.addInput(handlesPage(driverContext));
            assertNull(operator.getOutput());
        }
    }

    public void testInvokesFetcherOncePerInputPageWithAllHandles() {
        DriverContext driverContext = driverContext();
        AtomicInteger calls = new AtomicInteger();
        List<RemoteFetchHandle> captured = new ArrayList<>();
        List<RemoteFetchHandle> expected = List.of(
            handle("node-1", "session-1", 10),
            handle("node-1", "session-1", 20),
            handle("node-1", "session-1", 30)
        );

        try (RemoteFetchDataNodeBatchOperator operator = new RemoteFetchDataNodeBatchOperator(handles -> {
            calls.incrementAndGet();
            captured.addAll(handles);
            return List.of();
        })) {
            operator.addInput(handlesPage(driverContext, expected));
            assertNull(operator.getOutput());
        }

        assertThat(calls.get(), equalTo(1));
        assertThat(captured, equalTo(expected));
    }

    public void testRejectsHandlesFromDifferentTargetSessionsInSingleBatch() {
        DriverContext driverContext = driverContext();
        try (RemoteFetchDataNodeBatchOperator operator = new RemoteFetchDataNodeBatchOperator(handles -> List.of())) {
            operator.addInput(handlesPage(driverContext, List.of(handle("node-1", "session-1", 1), handle("node-2", "session-2", 2))));
            IllegalStateException e = expectThrows(IllegalStateException.class, operator::getOutput);
            assertThat(
                e.getMessage(),
                equalTo(
                    "remote fetch batch must contain handles from a single target session but saw [node-1/session-1] and [node-2/session-2]"
                )
            );
        }
    }

    private static Page handlesPage(DriverContext driverContext) {
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(1)) {
            builder.appendBytesRef(handle("node-1", "session-1", 1).toBytesRef());
            return new Page(builder.build());
        }
    }

    private static Page handlesPage(DriverContext driverContext, List<RemoteFetchHandle> handles) {
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(handles.size())) {
            for (RemoteFetchHandle handle : handles) {
                builder.appendBytesRef(handle.toBytesRef());
            }
            return new Page(builder.build());
        }
    }

    private static Page singleIntPage(DriverContext driverContext, int value) {
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(1)) {
            builder.appendInt(value);
            return new Page(builder.build());
        }
    }

    private static Page docsPage(DriverContext driverContext, List<RemoteFetchHandle> handles) {
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(handles.size())) {
            for (RemoteFetchHandle handle : handles) {
                builder.appendInt(Math.toIntExact(handle.doc()));
            }
            return new Page(builder.build());
        }
    }

    private static List<Integer> docsFromHandlePages(List<Page> pages) {
        List<Integer> docs = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            BytesRefBlock block = page.getBlock(0);
            for (int position = 0; position < page.getPositionCount(); position++) {
                assertThat(block.getValueCount(position), equalTo(1));
                RemoteFetchHandle handle = RemoteFetchHandle.fromBytesRef(block.getBytesRef(block.getFirstValueIndex(position), scratch));
                docs.add(Math.toIntExact(handle.doc()));
            }
        }
        return docs;
    }

    private static List<Integer> docsFromOutputPages(List<Page> pages) {
        List<Integer> docs = new ArrayList<>();
        for (Page page : pages) {
            IntBlock block = page.getBlock(0);
            for (int position = 0; position < page.getPositionCount(); position++) {
                assertThat(block.getValueCount(position), equalTo(1));
                docs.add(block.getInt(block.getFirstValueIndex(position)));
            }
        }
        return docs;
    }

    private static RemoteFetchHandle handle(String nodeId, String retainedSessionId, int doc) {
        return new RemoteFetchHandle(nodeId, retainedSessionId, 0, 0, doc);
    }

}
