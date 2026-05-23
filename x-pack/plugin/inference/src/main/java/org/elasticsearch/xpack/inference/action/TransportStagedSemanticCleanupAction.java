/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for {@code POST /{index}/_semantic_cleanup}.
 *
 * <p>This is a stub implementation: the shard-level operation currently returns
 * zero cleared/failed counts. The actual scan-and-remove logic for expired staged
 * semantic_text data will be added in a subsequent task.
 */
public class TransportStagedSemanticCleanupAction extends TransportBroadcastByNodeAction<
    StagedSemanticCleanupRequest,
    StagedSemanticCleanupResponse,
    TransportStagedSemanticCleanupAction.ShardResult,
    Void> {

    /** Per-shard result carrying the counts of cleared and failed staged fields. */
    public static final class ShardResult implements Writeable {

        private final int cleared;
        private final int failed;

        public ShardResult(int cleared, int failed) {
            this.cleared = cleared;
            this.failed = failed;
        }

        public ShardResult(StreamInput in) throws IOException {
            this.cleared = in.readVInt();
            this.failed = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(cleared);
            out.writeVInt(failed);
        }

        public int cleared() {
            return cleared;
        }

        public int failed() {
            return failed;
        }
    }

    @SuppressWarnings("unused")
    private final IndicesService indicesService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportStagedSemanticCleanupAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            StagedSemanticCleanupAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            StagedSemanticCleanupRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected ShardResult readShardResult(StreamInput in) throws IOException {
        return new ShardResult(in);
    }

    @Override
    protected ResponseFactory<StagedSemanticCleanupResponse, ShardResult> getResponseFactory(
        StagedSemanticCleanupRequest request,
        ClusterState clusterState
    ) {
        return (totalShards, successfulShards, failedShards, shardResults, shardFailures) -> {
            int totalCleared = 0;
            int totalFailed = 0;
            for (ShardResult result : shardResults) {
                totalCleared += result.cleared();
                totalFailed += result.failed();
            }
            return new StagedSemanticCleanupResponse(totalShards, successfulShards, failedShards, shardFailures, totalCleared, totalFailed);
        };
    }

    @Override
    protected StagedSemanticCleanupRequest readRequestFrom(StreamInput in) throws IOException {
        return new StagedSemanticCleanupRequest(in);
    }

    /**
     * Stub shard operation — returns zero cleared/failed.
     * The actual scan-and-remove logic will be added in a subsequent task.
     */
    @Override
    protected void shardOperation(
        StagedSemanticCleanupRequest request,
        ShardRouting shardRouting,
        Task task,
        Void nodeContext,
        ActionListener<ShardResult> listener
    ) {
        listener.onResponse(new ShardResult(0, 0));
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, StagedSemanticCleanupRequest request, String[] concreteIndices) {
        return clusterState.routingTable(projectResolver.getProjectId()).allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, StagedSemanticCleanupRequest request) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, StagedSemanticCleanupRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
