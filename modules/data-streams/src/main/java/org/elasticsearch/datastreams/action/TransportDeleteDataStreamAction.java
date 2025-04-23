/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.action.datastreams.DataStreamsActionUtil.getDataStreamNames;

public class TransportDeleteDataStreamAction extends AcknowledgedTransportMasterNodeAction<DeleteDataStreamAction.Request> {

    private static final Logger LOGGER = LogManager.getLogger(TransportDeleteDataStreamAction.class);

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportDeleteDataStreamAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices,
        ProjectResolver projectResolver
    ) {
        super(
            DeleteDataStreamAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDataStreamAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.systemIndices = systemIndices;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDataStreamAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        final var projectState = projectResolver.getProjectState(state);
        // resolve the names in the request early
        List<String> names = getDataStreamNames(
            indexNameExpressionResolver,
            projectState.metadata(),
            request.getNames(),
            request.indicesOptions()
        );
        for (String name : names) {
            systemIndices.validateDataStreamAccess(name, threadPool.getThreadContext());
        }

        submitUnbatchedTask(
            "remove-data-stream [" + Strings.arrayToCommaDelimitedString(request.getNames()) + "]",
            new ClusterStateUpdateTask(Priority.HIGH, request.masterNodeTimeout()) {

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return removeDataStream(
                        indexNameExpressionResolver,
                        currentState.projectState(projectState.projectId()),
                        request,
                        ds -> systemIndices.validateDataStreamAccess(ds, threadPool.getThreadContext()),
                        clusterService.getSettings()
                    );
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }
            }
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static ClusterState removeDataStream(
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectState projectState,
        DeleteDataStreamAction.Request request,
        Consumer<String> systemDataStreamAccessValidator,
        Settings settings
    ) {
        final ProjectMetadata project = projectState.metadata();
        List<String> names = getDataStreamNames(indexNameExpressionResolver, project, request.getNames(), request.indicesOptions());
        Set<String> dataStreams = new HashSet<>(names);
        for (String dataStreamName : dataStreams) {
            systemDataStreamAccessValidator.accept(dataStreamName);
        }

        if (dataStreams.isEmpty()) {
            if (request.isWildcardExpressionsOriginallySpecified()) {
                return projectState.cluster();
            } else {
                throw new ResourceNotFoundException("data streams " + Arrays.toString(request.getNames()) + " not found");
            }
        }

        return MetadataDataStreamsService.deleteDataStreams(
            projectState,
            dataStreams.stream().map(project.dataStreams()::get).collect(Collectors.toSet()),
            settings
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
