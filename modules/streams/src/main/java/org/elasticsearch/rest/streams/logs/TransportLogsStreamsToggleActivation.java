/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.streams.logs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SequentialAckingBatchedTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.StreamsMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;

/**
 * Transport action to toggle the activation state of logs streams in a project / cluster.
 */
public class TransportLogsStreamsToggleActivation extends AcknowledgedTransportMasterNodeAction<LogsStreamsActivationToggleAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportLogsStreamsToggleActivation.class);

    private final ProjectResolver projectResolver;
    private final MasterServiceTaskQueue<StreamsMetadataUpdateTask> taskQueue;

    @Inject
    public TransportLogsStreamsToggleActivation(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            LogsStreamsActivationToggleAction.INSTANCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            LogsStreamsActivationToggleAction.Request::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.taskQueue = clusterService.createTaskQueue(
            "streams-update-state-queue",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        LogsStreamsActivationToggleAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        ProjectId projectId = projectResolver.getProjectId();
        StreamsMetadata streamsState = state.metadata().getProject(projectId).custom(StreamsMetadata.TYPE, StreamsMetadata.EMPTY);
        boolean currentlyEnabled = streamsState.isLogsEnabled();
        boolean shouldEnable = request.shouldEnable();
        if (shouldEnable != currentlyEnabled) {
            StreamsMetadataUpdateTask updateTask = new StreamsMetadataUpdateTask(request, listener, projectId, shouldEnable);
            String taskName = String.format(Locale.ROOT, "enable-streams-logs-[%s]", shouldEnable ? "enable" : "disable");
            taskQueue.submitTask(taskName, updateTask, updateTask.timeout());
        } else {
            logger.debug("Logs streams are already in the requested state: {}", shouldEnable);
            listener.onResponse(AcknowledgedResponse.TRUE);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(LogsStreamsActivationToggleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    static class StreamsMetadataUpdateTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final boolean enabled;

        StreamsMetadataUpdateTask(
            AcknowledgedRequest<?> request,
            ActionListener<? extends AcknowledgedResponse> listener,
            ProjectId projectId,
            boolean enabled
        ) {
            super(request, listener);
            this.projectId = projectId;
            this.enabled = enabled;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            return currentState.copyAndUpdateProject(
                projectId,
                builder -> builder.putCustom(StreamsMetadata.TYPE, new StreamsMetadata(enabled))
            );
        }
    }
}
