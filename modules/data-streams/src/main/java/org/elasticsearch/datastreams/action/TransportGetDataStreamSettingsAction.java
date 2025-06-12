/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.GetDataStreamSettingsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransportGetDataStreamSettingsAction extends TransportMasterNodeReadAction<
    GetDataStreamSettingsAction.Request,
    GetDataStreamSettingsAction.Response> {
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportGetDataStreamSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetDataStreamSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDataStreamSettingsAction.Request::localOnly,
            GetDataStreamSettingsAction.Response::localOnly,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamSettingsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDataStreamSettingsAction.Request request,
        ClusterState state,
        ActionListener<GetDataStreamSettingsAction.Response> listener
    ) throws Exception {
        List<String> dataStreamNames = indexNameExpressionResolver.dataStreamNames(
            clusterService.state(),
            IndicesOptions.DEFAULT,
            request.indices()
        );
        Map<String, DataStream> dataStreamMap = state.metadata().dataStreams();
        List<GetDataStreamSettingsAction.DataStreamSettingsResponse> responseList = new ArrayList<>(dataStreamNames.size());
        for (String dataStreamName : dataStreamNames) {
            DataStream dataStream = dataStreamMap.get(dataStreamName);
            Settings settings = settingsFilter.filter(dataStream.getSettings());
            Settings effectiveSettings = settingsFilter.filter(dataStream.getEffectiveSettings(state.metadata()));
            responseList.add(new GetDataStreamSettingsAction.DataStreamSettingsResponse(dataStreamName, settings, effectiveSettings));
        }
        listener.onResponse(new GetDataStreamSettingsAction.Response(responseList));
    }
}
