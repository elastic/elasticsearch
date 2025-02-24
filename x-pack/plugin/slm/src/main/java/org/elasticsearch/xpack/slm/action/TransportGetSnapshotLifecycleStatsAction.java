/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleStatsAction;

public class TransportGetSnapshotLifecycleStatsAction extends TransportMasterNodeAction<
    AcknowledgedRequest.Plain,
    GetSnapshotLifecycleStatsAction.Response> {

    @Inject
    public TransportGetSnapshotLifecycleStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            GetSnapshotLifecycleStatsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AcknowledgedRequest.Plain::new,
            GetSnapshotLifecycleStatsAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        AcknowledgedRequest.Plain request,
        ClusterState state,
        ActionListener<GetSnapshotLifecycleStatsAction.Response> listener
    ) {
        SnapshotLifecycleMetadata slmMeta = state.metadata().getProject().custom(SnapshotLifecycleMetadata.TYPE);
        if (slmMeta == null) {
            listener.onResponse(new GetSnapshotLifecycleStatsAction.Response(new SnapshotLifecycleStats()));
        } else {
            listener.onResponse(new GetSnapshotLifecycleStatsAction.Response(slmMeta.getStats()));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(AcknowledgedRequest.Plain request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
