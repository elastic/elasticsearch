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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Exposes stats about the latest lifecycle run and the error store.
 */
public class TransportGetDataStreamLifecycleStatsAction extends TransportMasterNodeReadAction<
    GetDataStreamLifecycleStatsAction.Request,
    GetDataStreamLifecycleStatsAction.Response> {

    private final DataStreamLifecycleService lifecycleService;

    @Inject
    public TransportGetDataStreamLifecycleStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DataStreamLifecycleService lifecycleService
    ) {
        super(
            GetDataStreamLifecycleStatsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDataStreamLifecycleStatsAction.Request::new,
            indexNameExpressionResolver,
            GetDataStreamLifecycleStatsAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.lifecycleService = lifecycleService;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDataStreamLifecycleStatsAction.Request request,
        ClusterState state,
        ActionListener<GetDataStreamLifecycleStatsAction.Response> listener
    ) throws Exception {
        listener.onResponse(collectStats(state));
    }

    // Visible for testing
    GetDataStreamLifecycleStatsAction.Response collectStats(ClusterState state) {
        Metadata metadata = state.metadata();
        Set<String> indicesInErrorStore = lifecycleService.getErrorStore().getAllIndices();
        List<GetDataStreamLifecycleStatsAction.Response.DataStreamStats> dataStreamStats = new ArrayList<>();
        for (DataStream dataStream : state.metadata().dataStreams().values()) {
            if (dataStream.getLifecycle() != null && dataStream.getLifecycle().isEnabled()) {
                int total = 0;
                int inError = 0;
                for (Index index : dataStream.getIndices()) {
                    if (dataStream.isIndexManagedByDataStreamLifecycle(index, metadata::index)) {
                        total++;
                        if (indicesInErrorStore.contains(index.getName())) {
                            inError++;
                        }
                    }
                }
                dataStreamStats.add(new GetDataStreamLifecycleStatsAction.Response.DataStreamStats(dataStream.getName(), total, inError));
            }
        }
        return new GetDataStreamLifecycleStatsAction.Response(
            lifecycleService.getLastRunDuration(),
            lifecycleService.getTimeBetweenStarts(),
            dataStreamStats.isEmpty()
                ? dataStreamStats
                : dataStreamStats.stream()
                    .sorted(Comparator.comparing(GetDataStreamLifecycleStatsAction.Response.DataStreamStats::dataStreamName))
                    .toList()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamLifecycleStatsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
