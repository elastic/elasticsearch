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
import org.elasticsearch.action.datastreams.GetDataStreamMappingsAction;
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
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransportGetDataStreamMappingsAction extends TransportLocalProjectMetadataAction<
    GetDataStreamMappingsAction.Request,
    GetDataStreamMappingsAction.Response> {
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NamedXContentRegistry xContentRegistry;
    private final IndicesService indicesService;

    @Inject
    public TransportGetDataStreamMappingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedXContentRegistry xContentRegistry,
        IndicesService indicesService
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
        this.xContentRegistry = xContentRegistry;
        this.indicesService = indicesService;
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamMappingsAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetDataStreamMappingsAction.Request request,
        ProjectState project,
        ActionListener<GetDataStreamMappingsAction.Response> listener
    ) throws Exception {
        List<String> dataStreamNames = indexNameExpressionResolver.dataStreamNames(
            project.metadata(),
            IndicesOptions.DEFAULT,
            request.indices()
        );
        Map<String, DataStream> dataStreamMap = project.metadata().dataStreams();
        List<GetDataStreamMappingsAction.DataStreamMappingsResponse> responseList = new ArrayList<>(dataStreamNames.size());
        for (String dataStreamName : dataStreamNames) {
            DataStream dataStream = dataStreamMap.get(dataStreamName);
            responseList.add(
                new GetDataStreamMappingsAction.DataStreamMappingsResponse(
                    dataStreamName,
                    dataStream.getMappings(),
                    dataStream.getEffectiveMappings(project.metadata(), xContentRegistry, indicesService)
                )
            );
        }
        listener.onResponse(new GetDataStreamMappingsAction.Response(responseList));
    }
}
