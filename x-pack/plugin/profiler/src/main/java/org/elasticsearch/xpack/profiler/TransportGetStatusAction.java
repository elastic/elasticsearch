/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;

public class TransportGetStatusAction extends TransportMasterNodeAction<GetStatusAction.Request, GetStatusAction.Response> {

    @Inject
    public TransportGetStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetStatusAction.Request::new,
            indexNameExpressionResolver,
            GetStatusAction.Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        GetStatusAction.Request request,
        ClusterState state,
        ActionListener<GetStatusAction.Response> listener
    ) {
        boolean pluginEnabled = XPackSettings.PROFILING_ENABLED.get(state.getMetadata().settings());
        boolean resourceManagementEnabled = ProfilingPlugin.PROFILING_TEMPLATES_ENABLED.get(state.getMetadata().settings());
        boolean resourcesCreated = ProfilingIndexTemplateRegistry.areAllTemplatesCreated(state);
        listener.onResponse(new GetStatusAction.Response(pluginEnabled, resourceManagementEnabled, resourcesCreated));
    }

    @Override
    protected ClusterBlockException checkBlock(GetStatusAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
