/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.threadpool.ThreadPool;

public class TestTransportChannels {

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
}
