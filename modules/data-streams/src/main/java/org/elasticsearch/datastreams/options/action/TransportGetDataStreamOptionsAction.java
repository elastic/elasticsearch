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
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
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
public class TransportGetDataStreamOptionsAction extends TransportMasterNodeReadAction<
    GetDataStreamOptionsAction.Request,
    GetDataStreamOptionsAction.Response> {

    private final SystemIndices systemIndices;

    @Inject
    public TransportGetDataStreamOptionsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            GetDataStreamOptionsAction.INSTANCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDataStreamOptionsAction.Request::new,
            indexNameExpressionResolver,
            GetDataStreamOptionsAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDataStreamOptionsAction.Request request,
        ClusterState state,
        ActionListener<GetDataStreamOptionsAction.Response> listener
    ) {
        List<String> requestedDataStreams = DataStreamsActionUtil.getDataStreamNames(
            indexNameExpressionResolver,
            state,
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
    protected ClusterBlockException checkBlock(GetDataStreamOptionsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
