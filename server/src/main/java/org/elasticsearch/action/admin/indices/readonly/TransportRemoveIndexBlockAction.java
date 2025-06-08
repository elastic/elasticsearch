/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.readonly;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;

/**
 * Removes a single index level block from a given set of indices. This action removes the block setting
 * and updates the cluster state to reflect the change.
 */
public class TransportRemoveIndexBlockAction extends TransportMasterNodeAction<RemoveIndexBlockRequest, RemoveIndexBlockResponse> {

    public static final ActionType<RemoveIndexBlockResponse> TYPE = new ActionType<>("indices:admin/block/remove");
    private static final Logger logger = LogManager.getLogger(TransportRemoveIndexBlockAction.class);

    private final MetadataIndexStateService indexStateService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportRemoveIndexBlockAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexStateService indexStateService,
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
        this.indexStateService = indexStateService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.destructiveOperations = destructiveOperations;
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

        indexStateService.removeIndexBlock(
            new RemoveIndexBlockClusterStateUpdateRequest(
                request.masterNodeTimeout(),
                request.ackTimeout(),
                projectResolver.getProjectId(),
                request.getBlock(),
                task.getId(),
                concreteIndices
            ),
            listener.delegateResponse((delegatedListener, t) -> {
                logger.debug(() -> "failed to remove block from indices [" + Arrays.toString(concreteIndices) + "]", t);
                delegatedListener.onFailure(t);
            })
        );
    }
}
