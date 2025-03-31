/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.options.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Collects the data streams from the cluster state and then returns for each data stream its name and its
 * data stream options. Currently, data stream options include only the failure store configuration.
 */
public class TransportGetDataStreamOptionsAction extends TransportLocalProjectMetadataAction<
    GetDataStreamOptionsAction.Request,
    GetDataStreamOptionsAction.Response> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;
    private final ThreadPool threadPool;

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetDataStreamOptionsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            GetDataStreamOptionsAction.INSTANCE.name(),
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.systemIndices = systemIndices;
        this.threadPool = threadPool;

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetDataStreamOptionsAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetDataStreamOptionsAction.Request request,
        ProjectState state,
        ActionListener<GetDataStreamOptionsAction.Response> listener
    ) {
        ((CancellableTask) task).ensureNotCancelled();
        List<String> requestedDataStreams = DataStreamsActionUtil.getDataStreamNames(
            indexNameExpressionResolver,
            state.metadata(),
            request.getNames(),
            request.indicesOptions()
        );
        Map<String, DataStream> dataStreams = state.metadata().dataStreams();
        for (String name : requestedDataStreams) {
            systemIndices.validateDataStreamAccess(name, threadPool.getThreadContext());
        }
        listener.onResponse(
            new GetDataStreamOptionsAction.Response(
                requestedDataStreams.stream()
                    .map(dataStreams::get)
                    .filter(Objects::nonNull)
                    .map(
                        dataStream -> new GetDataStreamOptionsAction.Response.DataStreamEntry(
                            dataStream.getName(),
                            dataStream.getDataStreamOptions()
                        )
                    )
                    .sorted(Comparator.comparing(GetDataStreamOptionsAction.Response.DataStreamEntry::dataStreamName))
                    .toList()
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamOptionsAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
