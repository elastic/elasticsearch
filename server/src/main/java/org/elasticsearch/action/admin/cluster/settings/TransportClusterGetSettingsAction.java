/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportClusterGetSettingsAction extends TransportMasterNodeReadAction<
    ClusterGetSettingsAction.Request,
    ClusterGetSettingsAction.Response> {

    @Inject
    public TransportClusterGetSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterGetSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterGetSettingsAction.Request::new,
            indexNameExpressionResolver,
            ClusterGetSettingsAction.Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        ClusterGetSettingsAction.Request request,
        ClusterState state,
        ActionListener<ClusterGetSettingsAction.Response> listener
    ) throws Exception {
        Metadata metadata = state.metadata();
        listener.onResponse(new ClusterGetSettingsAction.Response(metadata.persistentSettings(), metadata.transientSettings()));
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterGetSettingsAction.Request request, ClusterState state) {
        return null;
    }
}
