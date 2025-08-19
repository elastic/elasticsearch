/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleStatsAction;

public class TransportGetSnapshotLifecycleStatsAction extends TransportLocalProjectMetadataAction<
    GetSnapshotLifecycleStatsAction.Request,
    GetSnapshotLifecycleStatsAction.Response> {

    /**
     * This was a TransportMasterNodeAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @Inject
    @SuppressWarnings("this-escape")
    public TransportGetSnapshotLifecycleStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            GetSnapshotLifecycleStatsAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );

        transportService.registerRequestHandler(
            GetSnapshotLifecycleStatsAction.NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            GetSnapshotLifecycleStatsAction.Request::read,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetSnapshotLifecycleStatsAction.Request request,
        ProjectState projectState,
        ActionListener<GetSnapshotLifecycleStatsAction.Response> listener
    ) {
        SnapshotLifecycleMetadata slmMeta = projectState.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        if (slmMeta == null) {
            listener.onResponse(new GetSnapshotLifecycleStatsAction.Response(new SnapshotLifecycleStats()));
        } else {
            listener.onResponse(new GetSnapshotLifecycleStatsAction.Response(slmMeta.getStats()));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotLifecycleStatsAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(state.projectId(), ClusterBlockLevel.METADATA_READ);
    }
}
