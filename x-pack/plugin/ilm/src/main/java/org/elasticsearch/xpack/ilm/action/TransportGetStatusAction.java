/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction.Response;

import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;

public class TransportGetStatusAction extends TransportLocalProjectMetadataAction<GetStatusAction.Request, Response> {

    /**
     * NB prior to 9.1 this was a TransportMasterNodeAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            GetStatusAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetStatusAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );

    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetStatusAction.Request request,
        ProjectState state,
        ActionListener<Response> listener
    ) {
        listener.onResponse(new Response(currentILMMode(state.metadata())));
    }

    @Override
    protected ClusterBlockException checkBlock(GetStatusAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
