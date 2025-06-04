/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskTransportChannelTests extends ESTestCase {

    public void testClosesTaskAfterChannelHandoff() throws IOException {
        runCompletionOrderTest(c -> c.sendResponse(new ElasticsearchException("simulated")));
        runCompletionOrderTest(c -> c.sendResponse(ActionResponse.Empty.INSTANCE));
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
