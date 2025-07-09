/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalClusterStateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public abstract class XPackUsageFeatureTransportAction extends TransportLocalClusterStateAction<
    XPackUsageRequest,
    XPackUsageFeatureResponse> {

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    public XPackUsageFeatureTransportAction(
        String name,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(name, actionFilters, transportService.getTaskManager(), clusterService, threadPool.executor(ThreadPool.Names.MANAGEMENT));

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            XPackUsageRequest::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(XPackUsageRequest request, ClusterState state) {
        return null;
    }
}
