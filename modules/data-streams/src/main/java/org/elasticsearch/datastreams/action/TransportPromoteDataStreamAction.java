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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.PromoteDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.core.Strings.format;

public class TransportPromoteDataStreamAction extends AcknowledgedTransportMasterNodeAction<PromoteDataStreamAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportPromoteDataStreamAction.class);

    private final SystemIndices systemIndices;

    @Inject
    public TransportPromoteDataStreamAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        SystemIndices systemIndices
    ) {
        super(
            PromoteDataStreamAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PromoteDataStreamAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(
        Task task,
        PromoteDataStreamAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        systemIndices.validateDataStreamAccess(request.getName(), threadPool.getThreadContext());
        submitUnbatchedTask(
            "promote-data-stream [" + request.getName() + "]",
            new ClusterStateUpdateTask(Priority.HIGH, request.masterNodeTimeout()) {

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return promoteDataStream(currentState, request);
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

    static ClusterState promoteDataStream(ClusterState currentState, PromoteDataStreamAction.Request request) {
        DataStream dataStream = currentState.getMetadata().getProject().dataStreams().get(request.getName());

        if (dataStream == null) {
            throw new ResourceNotFoundException("data stream [" + request.getName() + "] does not exist");
        }

        warnIfTemplateMissingForDatastream(dataStream, currentState);

        DataStream promotedDataStream = dataStream.promoteDataStream();
        Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        metadata.put(promotedDataStream);
        return ClusterState.builder(currentState).metadata(metadata).build();
    }

    private static void warnIfTemplateMissingForDatastream(DataStream dataStream, ClusterState currentState) {
        var datastreamName = dataStream.getName();

        var matchingIndex = currentState.metadata()
            .getProject()
            .templatesV2()
            .values()
            .stream()
            .filter(cit -> cit.getDataStreamTemplate() != null)
            .flatMap(cit -> cit.indexPatterns().stream())
            .anyMatch(pattern -> Regex.simpleMatch(pattern, datastreamName));

        if (matchingIndex == false) {
            String warningMessage = format(
                "Data stream [%s] does not have a matching index template. This will cause rollover to fail until a matching index "
                    + "template is created",
                datastreamName
            );
            logger.warn(() -> warningMessage);
            HeaderWarning.addWarning(warningMessage);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PromoteDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
