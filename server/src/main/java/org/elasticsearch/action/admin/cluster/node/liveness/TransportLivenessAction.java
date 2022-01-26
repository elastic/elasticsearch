/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.liveness;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

public final class TransportLivenessAction implements TransportRequestHandler<LivenessRequest> {

    private final ClusterService clusterService;
    public static final String NAME = "cluster:monitor/nodes/liveness";

    @Inject
    public TransportLivenessAction(ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        transportService.registerRequestHandler(
            NAME,
            ThreadPool.Names.SAME,
            false,
            false /*can not trip circuit breaker*/,
            LivenessRequest::new,
            this
        );
    }

    @Override
    public void messageReceived(LivenessRequest request, TransportChannel channel, Task task) throws Exception {
        channel.sendResponse(new LivenessResponse(clusterService.getClusterName(), clusterService.localNode()));
    }
}
