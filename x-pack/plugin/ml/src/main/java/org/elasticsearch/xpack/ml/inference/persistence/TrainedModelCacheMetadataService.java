/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.util.HashMap;

public class TrainedModelCacheMetadataService {
    private static final Logger LOGGER = LogManager.getLogger(TrainedModelCacheMetadataService.class);
    private final MasterServiceTaskQueue<ModelCacheMetadataManagementTask> modelCacheMetadataManagementTaskQueue;

    public TrainedModelCacheMetadataService(ClusterService clusterService) {
        this.modelCacheMetadataManagementTaskQueue = clusterService.createTaskQueue(
            "trained-models-cache-metadata",
            Priority.IMMEDIATE,
            new ModelCacheMetadataManagementTaskExecutor()
        );
    }

    public void deleteCacheMetadataEntry(String modelId, ActionListener<AcknowledgedResponse> listener) {
        ModelCacheMetadataManagementTask deleteModelCacheMetadataTask = new DeleteModelCacheMetadataTask(modelId, listener);
        this.modelCacheMetadataManagementTaskQueue.submitTask(deleteModelCacheMetadataTask.getDescription(), deleteModelCacheMetadataTask, null);
    }

    public void saveCacheMetadataEntry(TrainedModelConfig modelConfig, ActionListener<AcknowledgedResponse> listener) {
        ModelCacheMetadataManagementTask putModelCacheMetadataTask = new PutModelCacheMetadataTask(modelConfig.getModelId(), listener);
        this.modelCacheMetadataManagementTaskQueue.submitTask(putModelCacheMetadataTask.getDescription(), putModelCacheMetadataTask, null);
    }

    private abstract static class ModelCacheMetadataManagementTask implements ClusterStateTaskListener {
        protected final ActionListener<AcknowledgedResponse> listener;

        ModelCacheMetadataManagementTask(ActionListener<AcknowledgedResponse> listener) {
            this.listener = listener;
        }

        protected abstract TrainedModelCacheMetadata execute(TrainedModelCacheMetadata currentCacheMetadata, TaskContext<ModelCacheMetadataManagementTask> taskContext);

        protected abstract String getDescription();

        @Override
        public void onFailure(@Nullable Exception e) {
            LOGGER.error(() -> "unexpected failure during [" + getDescription() + "]", e);
            listener.onFailure(e);
        }
    }

    private static class PutModelCacheMetadataTask extends ModelCacheMetadataManagementTask {
        private final String modelId;

        PutModelCacheMetadataTask(String modelId, ActionListener<AcknowledgedResponse> listener) {
            super(listener);
            this.modelId = modelId;
        }

        protected TrainedModelCacheMetadata execute(TrainedModelCacheMetadata currentCacheMetadata, TaskContext<ModelCacheMetadataManagementTask> taskContext) {
            var entries = new HashMap<>(currentCacheMetadata.entries());
            entries.put(modelId, new TrainedModelCacheMetadata.TrainedModelCacheMetadataEntry(modelId));
            taskContext.success(() -> listener.onResponse(AcknowledgedResponse.TRUE));
            return new TrainedModelCacheMetadata(entries);
        }

        @Override
        protected String getDescription() {
            return "saving cache metadata for model [" + modelId + "]";
        }
    }

    private static class DeleteModelCacheMetadataTask extends ModelCacheMetadataManagementTask {
        private final String modelId;

        DeleteModelCacheMetadataTask(String modelId, ActionListener<AcknowledgedResponse> listener) {
            super(listener);
            this.modelId = modelId;
        }

        @Override
        protected TrainedModelCacheMetadata execute(TrainedModelCacheMetadata currentCacheMetadata, TaskContext<ModelCacheMetadataManagementTask> taskContext) {
            final TrainedModelCacheMetadata updatedCacheMetadata;
            if (currentCacheMetadata.entries().containsKey(modelId)) {
                var entries = new HashMap<>(currentCacheMetadata.entries());
                entries.remove(modelId);
                updatedCacheMetadata = new TrainedModelCacheMetadata(entries);
            } else {
                // We do not want to fail here since the model may not have a cache entry yet.
                updatedCacheMetadata = currentCacheMetadata;
            }

            taskContext.success(() -> listener.onResponse(AcknowledgedResponse.TRUE));
            return updatedCacheMetadata;
        }

        @Override
        protected String getDescription() {
            return "deleting cache metadata for model [" + modelId + "]";
        }
    }

    private static class ModelCacheMetadataManagementTaskExecutor implements ClusterStateTaskExecutor<ModelCacheMetadataManagementTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<ModelCacheMetadataManagementTask> batchExecutionContext) {
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
