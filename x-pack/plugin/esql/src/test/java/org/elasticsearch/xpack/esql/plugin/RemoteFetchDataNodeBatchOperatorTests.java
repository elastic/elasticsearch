/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class RemoteFetchDataNodeBatchOperatorTests extends ESTestCase {
    private final List<BlockFactory> blockFactories = new ArrayList<>();

    public void testFetchedPagesArePlainPagesForBatchDriverSinkWrapping() {
        DriverContext driverContext = driverContext();
        try (
            RemoteFetchDataNodeBatchOperator operator = new RemoteFetchDataNodeBatchOperator(
                handles -> List.of(intPage(driverContext, 10), intPage(driverContext, 20))
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
            new RemoteFetchHandle("node-1", "session-1", 0, 0, 10),
            new RemoteFetchHandle("node-1", "session-1", 0, 1, 20),
            new RemoteFetchHandle("node-1", "session-1", 1, 0, 30)
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
            operator.addInput(
                handlesPage(
                    driverContext,
                    List.of(new RemoteFetchHandle("node-1", "session-1", 0, 0, 1), new RemoteFetchHandle("node-2", "session-2", 0, 0, 2))
                )
            );
            IllegalStateException e = expectThrows(IllegalStateException.class, operator::getOutput);
            assertThat(
                e.getMessage(),
                equalTo(
                    "remote fetch batch must contain handles from a single target session but saw [node-1/session-1] and [node-2/session-2]"
                )
            );
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

    private static Page handlesPage(DriverContext driverContext) {
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(1)) {
            builder.appendBytesRef(new RemoteFetchHandle("node-1", "session-1", 0, 0, 1).toBytesRef());
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

    private static Page intPage(DriverContext driverContext, int value) {
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(1)) {
            builder.appendInt(value);
            return new Page(builder.build());
        }
    }

}
