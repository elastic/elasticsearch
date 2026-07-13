/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public final class TransportUpdateSnapshotStatusAction extends TransportMasterNodeAction<
    UpdateIndexShardSnapshotStatusRequest,
    ActionResponse.Empty> {
    public static final String NAME = "internal:cluster/snapshot/update_snapshot_status";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(NAME);

    private final SnapshotsService snapshotsService;

    @Inject
    public TransportUpdateSnapshotStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SnapshotsService snapshotsService,
        ActionFilters actionFilters
    ) {
        super(
            NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateIndexShardSnapshotStatusRequest::new,
            in -> ActionResponse.Empty.INSTANCE,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateIndexShardSnapshotStatusRequest request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        snapshotsService.createAndSubmitRequestToUpdateSnapshotState(
            request.snapshot(),
            request.shardId(),
            null,
            request.status(),
            listener.map(v -> ActionResponse.Empty.INSTANCE)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateIndexShardSnapshotStatusRequest request, ClusterState state) {
        return null;
    }
}
