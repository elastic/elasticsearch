/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.RateLimitingFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportDeprecationCacheResetAction
    extends TransportNodesAction<DeprecationCacheResetAction.Request,
    DeprecationCacheResetAction.Response,
    DeprecationCacheResetAction.NodeRequest,
    DeprecationCacheResetAction.NodeResponse> {

    private static final DeprecationLogger logger = DeprecationLogger.getLogger(TransportDeprecationCacheResetAction.class);

    private final RateLimitingFilter rateLimitingFilterForIndexing;

    @Inject
    protected TransportDeprecationCacheResetAction(String actionName,
                                                   ThreadPool threadPool,
                                                   ClusterService clusterService,
                                                   TransportService transportService,
                                                   ActionFilters actionFilters,
                                                   Writeable.Reader<DeprecationCacheResetAction.Request> request,
                                                   Writeable.Reader<DeprecationCacheResetAction.NodeRequest> nodeRequest,
                                                   String nodeExecutor, String finalExecutor,
                                                   Class<DeprecationCacheResetAction.NodeResponse> nodeResponseClass,
                                                   RateLimitingFilter rateLimitingFilterForIndexing) {
        super(actionName, threadPool, clusterService, transportService, actionFilters, request,
            nodeRequest, nodeExecutor, finalExecutor, nodeResponseClass);
        this.rateLimitingFilterForIndexing = rateLimitingFilterForIndexing;
    }

    @Override
    protected DeprecationCacheResetAction.Response newResponse(DeprecationCacheResetAction.Request request,
                                                               List<DeprecationCacheResetAction.NodeResponse> nodeResponses,
                                                               List<FailedNodeException> failures) {
        return new DeprecationCacheResetAction.Response(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected DeprecationCacheResetAction.NodeRequest newNodeRequest(DeprecationCacheResetAction.Request request) {
        return new DeprecationCacheResetAction.NodeRequest(request);
    }

    @Override
    protected DeprecationCacheResetAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new DeprecationCacheResetAction.NodeResponse(in);
    }

    @Override
    protected DeprecationCacheResetAction.NodeResponse nodeOperation(DeprecationCacheResetAction.NodeRequest request, Task task) {
        rateLimitingFilterForIndexing.reset();
        logger.warn(DeprecationCategory.CACHE_RESET, "cache_reset", "Deprecation cache was reset");
        return new DeprecationCacheResetAction.NodeResponse(transportService.getLocalNode());
    }
}
