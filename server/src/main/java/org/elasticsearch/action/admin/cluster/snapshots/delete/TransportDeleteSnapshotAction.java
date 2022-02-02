/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for delete snapshot operation
 */
public class TransportDeleteSnapshotAction extends AcknowledgedTransportMasterNodeAction<DeleteSnapshotRequest> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportDeleteSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SnapshotsService snapshotsService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DeleteSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteSnapshotRequest::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteSnapshotRequest request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final DeleteSnapshotRequest request,
        ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        snapshotsService.deleteSnapshots(request, listener.map(v -> AcknowledgedResponse.TRUE));
    }
}
