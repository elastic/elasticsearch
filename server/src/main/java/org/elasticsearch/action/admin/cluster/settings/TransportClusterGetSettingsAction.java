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
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportClusterGetSettingsAction extends TransportMasterNodeReadAction<
    ClusterGetSettingsAction.Request,
    ClusterGetSettingsAction.Response> {

    private final SettingsFilter settingsFilter;

    @Inject
    public TransportClusterGetSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterGetSettingsAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterGetSettingsAction.Request::new,
            indexNameExpressionResolver,
            ClusterGetSettingsAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.settingsFilter = settingsFilter;
    }

    @Override
    protected void masterOperation(
        Task task,
        ClusterGetSettingsAction.Request request,
        ClusterState state,
        ActionListener<ClusterGetSettingsAction.Response> listener
    ) throws Exception {
        Metadata metadata = state.metadata();
        listener.onResponse(
            new ClusterGetSettingsAction.Response(
                settingsFilter.filter(metadata.persistentSettings()),
                settingsFilter.filter(metadata.transientSettings()),
                settingsFilter.filter(metadata.settings())
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterGetSettingsAction.Request request, ClusterState state) {
        return null;
    }
}
