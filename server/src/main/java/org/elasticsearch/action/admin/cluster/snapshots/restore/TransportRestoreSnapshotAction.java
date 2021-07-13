/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for restore snapshot operation
 */
public class TransportRestoreSnapshotAction extends TransportMasterNodeAction<RestoreSnapshotRequest, RestoreSnapshotResponse> {
    private final RestoreService restoreService;

    @Inject
    public TransportRestoreSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RestoreService restoreService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            RestoreSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RestoreSnapshotRequest::new,
            indexNameExpressionResolver,
            RestoreSnapshotResponse::new,
            ThreadPool.Names.SAME
        );
        this.restoreService = restoreService;
    }

    @Override
    protected ClusterBlockException checkBlock(RestoreSnapshotRequest request, ClusterState state) {
        // Restoring a snapshot might change the global state and create/change an index,
        // so we need to check for METADATA_WRITE and WRITE blocks
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (blockException != null) {
            return blockException;
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);

    }

    @Override
    protected void masterOperation(
        Task task,
        final RestoreSnapshotRequest request,
        final ClusterState state,
        final ActionListener<RestoreSnapshotResponse> listener
    ) {
        restoreService.restoreSnapshot(request, listener.delegateFailure((delegatedListener, restoreCompletionResponse) -> {
            if (restoreCompletionResponse.getRestoreInfo() == null && request.waitForCompletion()) {
                RestoreClusterStateListener.createAndRegisterListener(clusterService, restoreCompletionResponse, delegatedListener);
            } else {
                delegatedListener.onResponse(new RestoreSnapshotResponse(restoreCompletionResponse.getRestoreInfo()));
            }
        }));
    }
}
