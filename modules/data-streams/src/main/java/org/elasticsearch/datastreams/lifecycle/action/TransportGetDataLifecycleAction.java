/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.datastreams.action.DataStreamsActionUtil;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Collects the data streams from the cluster state, filters the ones that do not have a lifecycle configured and then returns
 * a list of the data stream name and respective lifecycle configuration.
 */
public class TransportGetDataLifecycleAction extends TransportMasterNodeReadAction<
    GetDataLifecycleAction.Request,
    GetDataLifecycleAction.Response> {
    private final ClusterSettings clusterSettings;

    @Inject
    public TransportGetDataLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetDataLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDataLifecycleAction.Request::new,
            indexNameExpressionResolver,
            GetDataLifecycleAction.Response::new,
            ThreadPool.Names.SAME
        );
        clusterSettings = clusterService.getClusterSettings();
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDataLifecycleAction.Request request,
        ClusterState state,
        ActionListener<GetDataLifecycleAction.Response> listener
    ) {
        List<String> results = DataStreamsActionUtil.getDataStreamNames(
            indexNameExpressionResolver,
            state,
            request.getNames(),
            request.indicesOptions()
        );
        Map<String, DataStream> dataStreams = state.metadata().dataStreams();

        listener.onResponse(
            new GetDataLifecycleAction.Response(
                results.stream()
                    .map(dataStreams::get)
                    .filter(Objects::nonNull)
                    .map(
                        dataStream -> new GetDataLifecycleAction.Response.DataStreamLifecycle(
                            dataStream.getName(),
                            dataStream.getLifecycle()
                        )
                    )
                    .sorted(Comparator.comparing(GetDataLifecycleAction.Response.DataStreamLifecycle::dataStreamName))
                    .toList(),
                request.includeDefaults() && DataLifecycle.isEnabled()
                    ? clusterSettings.get(DataLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING)
                    : null
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataLifecycleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
