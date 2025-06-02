/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.TaskContext;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.action.FlushTrainedModelCacheAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelCacheMetadata;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TrainedModelCacheMetadataService implements ClusterStateListener {
    private static final Logger LOGGER = LogManager.getLogger(TrainedModelCacheMetadataService.class);
    static final String TASK_QUEUE_NAME = "trained-models-cache-metadata-management";
    private final MasterServiceTaskQueue<CacheMetadataUpdateTask> metadataUpdateTaskQueue;
    private final Client client;
    private volatile boolean isMasterNode = false;

    public TrainedModelCacheMetadataService(ClusterService clusterService, Client client) {
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        CacheMetadataUpdateTaskExecutor metadataUpdateTaskExecutor = new CacheMetadataUpdateTaskExecutor();
        this.metadataUpdateTaskQueue = clusterService.createTaskQueue(TASK_QUEUE_NAME, Priority.IMMEDIATE, metadataUpdateTaskExecutor);
        clusterService.addListener(this);
    }

    public void updateCacheVersion(ActionListener<AcknowledgedResponse> listener) {
        if (this.isMasterNode == false) {
            client.execute(FlushTrainedModelCacheAction.INSTANCE, new FlushTrainedModelCacheAction.Request(), listener);
            return;
        }

        CacheMetadataUpdateTask updateMetadataTask = new RefreshCacheMetadataVersionTask(listener);
        this.metadataUpdateTaskQueue.submitTask(updateMetadataTask.getDescription(), updateMetadataTask, null);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().clusterRecovered() == false || event.state().nodes().getMasterNode() == null) {
            return;
        }
        this.isMasterNode = event.localNodeMaster();
    }

    abstract static class CacheMetadataUpdateTask implements ClusterStateTaskListener {
        protected final ActionListener<AcknowledgedResponse> listener;

        CacheMetadataUpdateTask(ActionListener<AcknowledgedResponse> listener) {
            this.listener = listener;
        }

        protected abstract TrainedModelCacheMetadata execute(
            TrainedModelCacheMetadata currentCacheMetadata,
            TaskContext<CacheMetadataUpdateTask> taskContext
        );

        protected abstract String getDescription();

        @Override
        public void onFailure(@Nullable Exception e) {
            LOGGER.error("unexpected failure during [" + getDescription() + "]", e);
            listener.onFailure(e);
        }
    }

    static class RefreshCacheMetadataVersionTask extends CacheMetadataUpdateTask {
        RefreshCacheMetadataVersionTask(ActionListener<AcknowledgedResponse> listener) {
            super(listener);
        }

        @Override
        protected TrainedModelCacheMetadata execute(
            TrainedModelCacheMetadata currentCacheMetadata,
            TaskContext<CacheMetadataUpdateTask> taskContext
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

    static class CacheMetadataUpdateTaskExecutor implements ClusterStateTaskExecutor<CacheMetadataUpdateTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<CacheMetadataUpdateTask> batchExecutionContext) {
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

            final var finalMetadata = currentCacheMetadata;
            return initialState.copyAndUpdateProject(
                initialState.metadata().getProject().id(),
                builder -> builder.putCustom(TrainedModelCacheMetadata.NAME, finalMetadata)
            );
        }
    }
}
