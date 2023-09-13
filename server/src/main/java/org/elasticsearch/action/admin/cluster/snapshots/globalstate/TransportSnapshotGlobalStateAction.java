/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.globalstate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportSnapshotGlobalStateAction extends TransportMasterNodeAction<SnapshotGlobalStateRequest, SnapshotGlobalStateResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportSnapshotGlobalStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SnapshotsService snapshotsService
    ) {
        super(
            SnapshotGlobalStateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SnapshotGlobalStateRequest::new,
            indexNameExpressionResolver,
            SnapshotGlobalStateResponse::new,
            ThreadPool.Names.SAME
        );
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        final SnapshotGlobalStateRequest request,
        final ClusterState state,
        final ActionListener<SnapshotGlobalStateResponse> listener
    ) throws Exception {
        snapshotsService.getSnapshotGlobalMetadata(
            request.repository(),
            request.snapshot(),
            listener.map(SnapshotGlobalStateResponse::new)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(SnapshotGlobalStateRequest request, ClusterState state) {
        return null;
    }
}
