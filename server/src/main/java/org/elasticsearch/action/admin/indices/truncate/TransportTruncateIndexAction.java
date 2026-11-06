/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.truncate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportBroadcastReplicationAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Discards all documents of the resolved indices by truncating every shard copy. Unlike deleting and recreating an index this does not
 * touch the cluster state: no index is created or removed and no shard is allocated, so it is cheap to do repeatedly.
 */
public class TransportTruncateIndexAction extends TransportBroadcastReplicationAction<
    TruncateIndexRequest,
    BroadcastResponse,
    ShardTruncateRequest,
    ReplicationResponse> {

    /**
     * Every node has to understand the {@link org.elasticsearch.index.translog.Translog.Truncate} operation, because it is written to
     * translogs and streamed to recovering copies.
     */
    private static final TransportVersion TRUNCATE_INDEX = TransportVersion.fromName("truncate_index");

    private final ClusterService clusterService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportTruncateIndexAction(
        ClusterService clusterService,
        TransportService transportService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectResolver projectResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            TruncateIndexAction.NAME,
            TruncateIndexRequest::new,
            clusterService,
            transportService,
            client,
            actionFilters,
            indexNameExpressionResolver,
            TransportShardTruncateAction.TYPE,
            transportService.getThreadPool().executor(ThreadPool.Names.FLUSH),
            projectResolver
        );
        this.clusterService = clusterService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected void doExecute(Task task, TruncateIndexRequest request, ActionListener<BroadcastResponse> listener) {
        if (clusterService.state().getMinTransportVersion().supports(TRUNCATE_INDEX) == false) {
            listener.onFailure(new IllegalStateException("cannot truncate indices until all nodes in the cluster have been upgraded"));
            return;
        }
        // truncating is as destructive as deleting, so honour action.destructive_requires_name
        try {
            destructiveOperations.failDestructive(request.indices());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected ShardTruncateRequest newShardRequest(TruncateIndexRequest request, ShardId shardId, SplitShardCountSummary summary) {
        return new ShardTruncateRequest(shardId, summary);
    }

    @Override
    protected BroadcastResponse newResponse(
        int successfulShards,
        int failedShards,
        int totalNumCopies,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        return new BroadcastResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
