/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.Consumer;

public class TestTransportChannels {

    public static TaskTransportChannel newTaskTransportChannel(TransportChannel channel, Releasable onTaskFinished) {
        return new TaskTransportChannel(1, channel, onTaskFinished);
    }

    public static TcpTransportChannel newFakeTcpTransportChannel(
        String nodeName,
        TcpChannel channel,
        ThreadPool threadPool,
        String action,
        long requestId,
        TransportVersion version
    ) {
        BytesRefRecycler recycler = new BytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE);
        return new TcpTransportChannel(
            new OutboundHandler(nodeName, version, new StatsTracker(), threadPool, recycler, new HandlingTimeTracker(), false),
            channel,
            action,
            requestId,
            version,
            null,
            ResponseStatsConsumer.NONE,
            false,
            () -> {}
        );
    }

    /**
     * Wraps {@code channel} so that {@code breakerConsumer} gets the breaker a response is sent with, before the response is sent.
     * A test can then read it as soon as the response arrives. The consumer isn't called for a response sent without a breaker.
     */
    public static TransportChannel capturingResponseBreaker(TransportChannel channel, Consumer<CircuitBreaker> breakerConsumer) {
        return new TransportChannel() {
            @Override
            public String getProfileName() {
                return channel.getProfileName();
            }

            @Override
            public void sendResponse(TransportResponse response) {
                channel.sendResponse(response);
            }

            @Override
            public void sendResponse(TransportResponse response, CircuitBreaker circuitBreaker) {
                breakerConsumer.accept(circuitBreaker);
                channel.sendResponse(response, circuitBreaker);
            }

            @Override
            public void sendResponse(Exception exception) {
                channel.sendResponse(exception);
            }

            @Override
            public TransportVersion getVersion() {
                return channel.getVersion();
            }
        };
    }
}
