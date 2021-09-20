/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;

public class TestTransportChannels {

    public static TcpTransportChannel newFakeTcpTransportChannel(String nodeName, TcpChannel channel, ThreadPool threadPool,
                                                                 String action, long requestId, Version version) {
        return new TcpTransportChannel(
            new OutboundHandler(nodeName, version, new String[0], new StatsTracker(), threadPool, BigArrays.NON_RECYCLING_INSTANCE),
            channel, action, requestId, version, Collections.emptySet(), null, false, () -> {});
    }
}
