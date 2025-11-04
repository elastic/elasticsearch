/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalClusterStateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportGetBasicStatusAction extends TransportLocalClusterStateAction<GetBasicStatusRequest, GetBasicStatusResponse> {
    public static final ActionType<GetBasicStatusResponse> TYPE = new ActionType<>("cluster:admin/xpack/license/basic_status");

    /**
     * Prior to 9.3 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetBasicStatusAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), clusterService, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetBasicStatusRequest::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetBasicStatusRequest request,
        ClusterState state,
        ActionListener<GetBasicStatusResponse> listener
    ) throws Exception {
        LicensesMetadata licensesMetadata = state.metadata().custom(LicensesMetadata.TYPE);
        if (licensesMetadata == null) {
            listener.onResponse(new GetBasicStatusResponse(true));
        } else {
            License license = licensesMetadata.getLicense();
            listener.onResponse(new GetBasicStatusResponse(license == null || License.LicenseType.isBasic(license.type()) == false));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetBasicStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
