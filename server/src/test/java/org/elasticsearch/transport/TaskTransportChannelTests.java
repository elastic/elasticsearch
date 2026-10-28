/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class TaskTransportChannelTests extends ESTestCase {

    public void testClosesTaskAfterChannelHandoff() throws IOException {
        runCompletionOrderTest(c -> c.sendResponse(new ElasticsearchException("simulated")));
        runCompletionOrderTest(c -> c.sendResponse(ActionResponse.Empty.INSTANCE));
    }

    public void testPassesResponseBreakerToTcpChannel() {
        final var threadPool = new TestThreadPool(getTestName());
        try {
            final var tcpChannel = new FakeTcpChannel();
            final var breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
            final var channel = new TaskTransportChannel(
                1,
                TestTransportChannels.newFakeTcpTransportChannel(
                    "node",
                    tcpChannel,
                    threadPool,
                    "action",
                    randomNonNegativeLong(),
                    TransportVersion.current()
                ),
                () -> {}
            );
            channel.sendResponse(ActionResponse.Empty.INSTANCE, breaker);
            // The serialized response fits in one page, which stays charged until the write completes.
            assertThat(breaker.getUsed(), equalTo((long) PageCacheRecycler.BYTE_PAGE_SIZE));
            tcpChannel.getListenerCaptor().get().onResponse(null);
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            terminate(threadPool);
        }
    }

    private void runCompletionOrderTest(CheckedConsumer<TransportChannel, IOException> channelConsumer) throws IOException {
        final var stage = new AtomicInteger();
        var channel = new TaskTransportChannel(
            1,
            new TestTransportChannel(ActionListener.running(() -> assertTrue(stage.compareAndSet(0, 1)))),
            () -> assertTrue(stage.compareAndSet(1, 2))
        );
        channelConsumer.accept(channel);
        assertEquals(2, stage.get());
    }
}
