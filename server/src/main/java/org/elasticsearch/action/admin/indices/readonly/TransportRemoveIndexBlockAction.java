/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.List;

/**
 * Removes a single index level block from a given set of indices. This action removes the block setting
 * and updates the cluster state to reflect the change.
 */
public class TransportRemoveIndexBlockAction extends TransportMasterNodeAction<RemoveIndexBlockRequest, RemoveIndexBlockResponse> {

    public static final ActionType<RemoveIndexBlockResponse> TYPE = new ActionType<>("indices:admin/block/remove");

    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final DestructiveOperations destructiveOperations;
    private final MasterServiceTaskQueue<AckedClusterStateUpdateTask> removeBlocksQueue;

    @Inject
    public TransportRemoveIndexBlockAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RemoveIndexBlockRequest::new,
            RemoveIndexBlockResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.destructiveOperations = destructiveOperations;

        removeBlocksQueue = clusterService.createTaskQueue("remove-blocks", Priority.URGENT, new SimpleBatchedAckListenerTaskExecutor<>() {
            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(AckedClusterStateUpdateTask task, ClusterState clusterState)
                throws Exception {
                return Tuple.tuple(task.execute(clusterState), task);
            }
        });
    }

    @Override
    protected void doExecute(Task task, RemoveIndexBlockRequest request, ActionListener<RemoveIndexBlockResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(RemoveIndexBlockRequest request, ClusterState state) {
        if (request.getBlock().getBlock().levels().contains(ClusterBlockLevel.METADATA_WRITE)
            && state.blocks().global(ClusterBlockLevel.METADATA_WRITE).isEmpty()) {
            return null;
        }
        final ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(state);
        return state.blocks()
            .indicesBlockedException(
                projectMetadata.id(),
                ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(projectMetadata, request)
            );
    }

    @Override
    protected void masterOperation(
        final Task task,
        final RemoveIndexBlockRequest request,
        final ClusterState state,
        final ActionListener<RemoveIndexBlockResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(RemoveIndexBlockResponse.EMPTY);
            return;
        }

        final var projectId = projectResolver.getProjectId();
        removeBlocksQueue.submitTask(
            "remove-index-block-[" + request.getBlock().toString() + "]-" + Arrays.toString(concreteIndices),
            new AckedClusterStateUpdateTask(request, listener) {
                private List<RemoveIndexBlockResponse.RemoveBlockResult> results;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    final var tuple = MetadataIndexStateService.removeIndexBlock(
                        currentState.projectState(projectId),
                        concreteIndices,
                        request.getBlock()
                    );
                    results = tuple.v2();
                    return tuple.v1();
                }

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new RemoveIndexBlockResponse(acknowledged, results);
                }
            },
            request.masterNodeTimeout()
        );
    }

}
