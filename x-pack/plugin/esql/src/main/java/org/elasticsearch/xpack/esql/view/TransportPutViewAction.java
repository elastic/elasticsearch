/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;

public class TransportPutViewAction extends AcknowledgedTransportMasterNodeAction<PutViewAction.Request> {
    private final ViewService viewService;

    @Inject
    public TransportPutViewAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ViewService viewService
    ) {
        super(
            PutViewAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutViewAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.viewService = viewService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutViewAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        Configuration configuration = new Configuration(
            ZoneOffset.UTC,
            Locale.US,
            // TODO: plug-in security
            null,
            clusterService.getClusterName().value(),
            null,
            clusterService.getClusterSettings().get(EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE),
            clusterService.getClusterSettings().get(EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE),
            request.view().query(),
            false,
            Map.of(),
            System.nanoTime(),
            false,
            EsqlPlugin.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            EsqlPlugin.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY)
        );
        viewService.put(request.name(), request.view(), listener.map(v -> AcknowledgedResponse.TRUE), configuration);
    }

    @Override
    protected ClusterBlockException checkBlock(PutViewAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
