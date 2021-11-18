/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;

public class TransportUpdateDatafeedAction extends TransportMasterNodeAction<UpdateDatafeedAction.Request, PutDatafeedAction.Response> {

    private final DatafeedManager datafeedManager;
    private final SecurityContext securityContext;

    @Inject
    public TransportUpdateDatafeedAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DatafeedManager datafeedManager
    ) {
        super(
            UpdateDatafeedAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateDatafeedAction.Request::new,
            indexNameExpressionResolver,
            PutDatafeedAction.Response::new,
            ThreadPool.Names.SAME
        );

        this.datafeedManager = datafeedManager;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateDatafeedAction.Request request,
        ClusterState state,
        ActionListener<PutDatafeedAction.Response> listener
    ) {

        datafeedManager.updateDatafeed(request, state, securityContext, threadPool, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
