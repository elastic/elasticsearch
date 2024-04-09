/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.TaskContext;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelCacheMetadata;

public class TrainedModelCacheMetadataService implements ClusterStateListener {
    private static final Logger LOGGER = LogManager.getLogger(TrainedModelCacheMetadataService.class);
    private final MasterServiceTaskQueue<TrainedModelCacheMetadataUpdateTask> modelCacheMetadataUpdateTaskQueue;
    private volatile boolean isMasterNode = false;

    public TrainedModelCacheMetadataService(ClusterService clusterService) {
        this.modelCacheMetadataUpdateTaskQueue = clusterService.createTaskQueue(
            "trained-models-cache-metadata-management",
            Priority.IMMEDIATE,
            new TrainedModelCacheMetadataTaskExecutor()
        );
        clusterService.addListener(this);
    }

    public void refreshCacheVersion(ActionListener<AcknowledgedResponse> listener) {
        if (this.isMasterNode == false) {
            // TODO: Use an internal action to update the cache version
            listener.onResponse(AcknowledgedResponse.FALSE);
            return;
        }
        TrainedModelCacheMetadataUpdateTask updateMetadataTask = new RefreshTrainedModeCacheMetadataVersionTask(listener);
        this.modelCacheMetadataUpdateTaskQueue.submitTask(updateMetadataTask.getDescription(), updateMetadataTask, null);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().clusterRecovered() == false || event.state().nodes().getMasterNode() == null) {
            return;
        }
        this.isMasterNode = event.localNodeMaster();
    }

    private abstract static class TrainedModelCacheMetadataUpdateTask implements ClusterStateTaskListener {
        protected final ActionListener<AcknowledgedResponse> listener;

        TrainedModelCacheMetadataUpdateTask(ActionListener<AcknowledgedResponse> listener) {
            this.listener = listener;
        }

        protected abstract TrainedModelCacheMetadata execute(
            TrainedModelCacheMetadata currentCacheMetadata,
            TaskContext<TrainedModelCacheMetadataUpdateTask> taskContext
        );

        protected abstract String getDescription();

        @Override
        public void onFailure(@Nullable Exception e) {
            LOGGER.error(() -> "unexpected failure during [" + getDescription() + "]", e);
            listener.onFailure(e);
        }
    }

    private static class RefreshTrainedModeCacheMetadataVersionTask extends TrainedModelCacheMetadataUpdateTask {
        RefreshTrainedModeCacheMetadataVersionTask(ActionListener<AcknowledgedResponse> listener) {
            super(listener);
        }

        @Override
        protected TrainedModelCacheMetadata execute(
            TrainedModelCacheMetadata currentCacheMetadata,
            TaskContext<TrainedModelCacheMetadataUpdateTask> taskContext
        ) {
            long newVersion = currentCacheMetadata.version() < Long.MAX_VALUE ? currentCacheMetadata.version() + 1 : 1L;
            taskContext.success(() -> listener.onResponse(AcknowledgedResponse.TRUE));
            return new TrainedModelCacheMetadata(newVersion);
        }

        @Override
        protected String getDescription() {
            return "refresh trained model cache version";
        }
    }

    private static class TrainedModelCacheMetadataTaskExecutor implements ClusterStateTaskExecutor<TrainedModelCacheMetadataUpdateTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<TrainedModelCacheMetadataUpdateTask> batchExecutionContext) {
            final var initialState = batchExecutionContext.initialState();
            XPackPlugin.checkReadyForXPackCustomMetadata(initialState);

            final TrainedModelCacheMetadata originalCacheMetadata = TrainedModelCacheMetadata.fromState(initialState);
            TrainedModelCacheMetadata currentCacheMetadata = originalCacheMetadata;

            for (final var taskContext : batchExecutionContext.taskContexts()) {
                try (var ignored = taskContext.captureResponseHeaders()) {
                    currentCacheMetadata = taskContext.getTask().execute(currentCacheMetadata, taskContext);
                }
            }

            if (currentCacheMetadata == originalCacheMetadata) {
                return initialState;
            }

            return ClusterState.builder(initialState)
                .metadata(Metadata.builder(initialState.metadata()).putCustom(TrainedModelCacheMetadata.NAME, currentCacheMetadata))
                .build();
        }
    }
}
