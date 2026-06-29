/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.cache.CacheSnapshotService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Node-targeted transport action that executes the cache snapshot sequence on the local node.
 * Invoked from {@link RestCacheSnapshotAction} with a specific target node ID.
 */
public class TransportCacheSnapshotAction extends TransportNodesAction<
    CacheSnapshotRequest,
    CacheSnapshotResponse,
    CacheSnapshotNodeRequest,
    CacheSnapshotNodeResponse,
    Void> {

    public static final String NAME = "cluster:admin/stateless/cache/snapshot";
    public static final ActionType<CacheSnapshotResponse> TYPE = new ActionType<>(NAME);

    private final StatelessSharedBlobCacheService cacheService;
    private final NodeEnvironment nodeEnvironment;

    @Inject
    public TransportCacheSnapshotAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        StatelessSharedBlobCacheService cacheService,
        NodeEnvironment nodeEnvironment
    ) {
        super(
            NAME,
            clusterService,
            transportService,
            actionFilters,
            CacheSnapshotNodeRequest::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.cacheService = cacheService;
        this.nodeEnvironment = nodeEnvironment;
    }

    @Override
    protected CacheSnapshotResponse newResponse(
        CacheSnapshotRequest request,
        List<CacheSnapshotNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new CacheSnapshotResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected CacheSnapshotNodeRequest newNodeRequest(CacheSnapshotRequest request) {
        return new CacheSnapshotNodeRequest();
    }

    @Override
    protected CacheSnapshotNodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new CacheSnapshotNodeResponse(in);
    }

    @Override
    protected CacheSnapshotNodeResponse nodeOperation(CacheSnapshotNodeRequest request, Task task) {
        CacheSnapshotService snapshotService = cacheService.getSnapshotService();
        if (snapshotService == null) {
            throw new IllegalStateException("cache snapshot is not enabled on this node (set stateless.cache_snapshot.enabled = true)");
        }
        try {
            String nodeId = nodeEnvironment.nodeId();
            String snapshotId = snapshotService.snapshot(cacheService, nodeId);
            return new CacheSnapshotNodeResponse(clusterService.localNode(), snapshotId);
        } catch (IOException e) {
            throw new UncheckedIOException("cache snapshot failed", e);
        }
    }
}
