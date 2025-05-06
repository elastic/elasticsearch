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
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.datastreams.GetDataStreamSettingsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
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

public class TransportGetDataStreamSettingsAction extends TransportLocalProjectMetadataAction<
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
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSettingsAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamSettingsAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetDataStreamSettingsAction.Request request,
        ProjectState project,
        ActionListener<GetDataStreamSettingsAction.Response> listener
    ) throws Exception {
        List<String> dataStreamNames = indexNameExpressionResolver.dataStreamNames(
            clusterService.state(),
            IndicesOptions.DEFAULT,
            request.indices()
        );
        Map<String, DataStream> dataStreamMap = project.metadata().dataStreams();
        List<GetDataStreamSettingsAction.DataStreamSettingsResponse> responseList = new ArrayList<>(dataStreamNames.size());
        for (String dataStreamName : dataStreamNames) {
            DataStream dataStream = dataStreamMap.get(dataStreamName);
            Settings settings = settingsFilter.filter(dataStream.getSettings());
            Settings effectiveSettings = settingsFilter.filter(dataStream.getEffectiveSettings(project.metadata()));
            responseList.add(new GetDataStreamSettingsAction.DataStreamSettingsResponse(dataStreamName, settings, effectiveSettings));
        }
        listener.onResponse(new GetDataStreamSettingsAction.Response(responseList));
    }
}
