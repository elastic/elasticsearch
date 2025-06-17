/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.streams.logs;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.streams.StreamsMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action to retrieve the status of logs streams in a project / cluster.
 * Results are broken down by stream type. Currently only logs streams are implemented.
 */
public class TransportStreamsStatusAction extends TransportLocalProjectMetadataAction<
    StreamsStatusAction.Request,
    StreamsStatusAction.Response> {

    @Inject
    public TransportStreamsStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            StreamsStatusAction.INSTANCE.name(),
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
    }

    @Override
    protected ClusterBlockException checkBlock(StreamsStatusAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        StreamsStatusAction.Request request,
        ProjectState state,
        ActionListener<StreamsStatusAction.Response> listener
    ) {
        StreamsMetadata streamsState = state.metadata().custom(StreamsMetadata.TYPE, StreamsMetadata.EMPTY);
        boolean logsEnabled = streamsState.isLogsEnabled();
        StreamsStatusAction.Response response = new StreamsStatusAction.Response(logsEnabled);
        listener.onResponse(response);
    }
}
