/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.datastreams.lifecycle.GetDataStreamLifecycleAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Collects the data streams from the cluster state, filters the ones that do not have a data stream lifecycle configured and then returns
 * a list of the data stream name and respective lifecycle configuration.
 */
public class TransportGetDataStreamLifecycleAction extends TransportMasterNodeReadAction<
    GetDataStreamLifecycleAction.Request,
    GetDataStreamLifecycleAction.Response> {
    private final ClusterSettings clusterSettings;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;

    @Inject
    public TransportGetDataStreamLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        super(
            GetDataStreamLifecycleAction.INSTANCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDataStreamLifecycleAction.Request::new,
            indexNameExpressionResolver,
            GetDataStreamLifecycleAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        clusterSettings = clusterService.getClusterSettings();
        this.globalRetentionSettings = globalRetentionSettings;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDataStreamLifecycleAction.Request request,
        ClusterState state,
        ActionListener<GetDataStreamLifecycleAction.Response> listener
    ) {
        List<String> results = DataStreamsActionUtil.getDataStreamNames(
            indexNameExpressionResolver,
            state,
            request.getNames(),
            request.indicesOptions()
        );
        Map<String, DataStream> dataStreams = state.metadata().dataStreams();

        listener.onResponse(
            new GetDataStreamLifecycleAction.Response(
                results.stream()
                    .map(dataStreams::get)
                    .filter(Objects::nonNull)
                    .map(
                        dataStream -> new GetDataStreamLifecycleAction.Response.DataStreamLifecycle(
                            dataStream.getName(),
                            dataStream.getLifecycle(),
                            dataStream.isSystem()
                        )
                    )
                    .sorted(Comparator.comparing(GetDataStreamLifecycleAction.Response.DataStreamLifecycle::dataStreamName))
                    .toList(),
                request.includeDefaults() ? clusterSettings.get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING) : null,
                globalRetentionSettings.get()
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamLifecycleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
