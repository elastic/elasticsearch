/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;

import java.io.IOException;
import java.util.List;

/**
 * Clears a security cache by name (with optional keys).
 * @see CacheInvalidatorRegistry
 */
public class TransportClearSecurityCacheAction extends TransportNodesAction<
    ClearSecurityCacheRequest,
    ClearSecurityCacheResponse,
    ClearSecurityCacheRequest.Node,
    ClearSecurityCacheResponse.Node> {

    private final CacheInvalidatorRegistry cacheInvalidatorRegistry;

    @Inject
    public TransportClearSecurityCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        CacheInvalidatorRegistry cacheInvalidatorRegistry
    ) {
        super(
            ClearSecurityCacheAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ClearSecurityCacheRequest::new,
            ClearSecurityCacheRequest.Node::new,
            ThreadPool.Names.MANAGEMENT,
            ClearSecurityCacheResponse.Node.class
        );
        this.cacheInvalidatorRegistry = cacheInvalidatorRegistry;
    }

    @Override
    protected ClearSecurityCacheResponse newResponse(
        ClearSecurityCacheRequest request,
        List<ClearSecurityCacheResponse.Node> nodes,
        List<FailedNodeException> failures
    ) {
        return new ClearSecurityCacheResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected ClearSecurityCacheRequest.Node newNodeRequest(ClearSecurityCacheRequest request) {
        return new ClearSecurityCacheRequest.Node(request);
    }

    @Override
    protected ClearSecurityCacheResponse.Node newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new ClearSecurityCacheResponse.Node(in);
    }

    @Override
    protected ClearSecurityCacheResponse.Node nodeOperation(ClearSecurityCacheRequest.Node request, Task task) {
        if (request.getKeys() == null || request.getKeys().length == 0) {
            cacheInvalidatorRegistry.invalidateCache(request.getCacheName());
        } else {
            cacheInvalidatorRegistry.invalidateByKey(request.getCacheName(), List.of(request.getKeys()));
        }
        return new ClearSecurityCacheResponse.Node(clusterService.localNode());
    }
}
