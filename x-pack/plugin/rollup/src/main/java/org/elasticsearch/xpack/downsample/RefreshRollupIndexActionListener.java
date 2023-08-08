/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.TaskId;

/**
 * Updates the downsample target index metadata (task status)
 */
public class RefreshRollupIndexActionListener implements ActionListener<RefreshResponse> {

    private final ActionListener<AcknowledgedResponse> failureDelegate;
    private final Client client;
    private final TaskId parentTask;
    private final String rollupIndexName;
    private final TimeValue timeout;

    private final MasterServiceTaskQueue<TransportDownsampleAction.RollupClusterStateUpdateTask> taskQueue;

    public RefreshRollupIndexActionListener(
        final ActionListener<AcknowledgedResponse> failureDelegate,
        final Client client,
        TaskId parentTask,
        final String rollupIndexName,
        final TimeValue timeout,
        final MasterServiceTaskQueue<TransportDownsampleAction.RollupClusterStateUpdateTask> taskQueue
    ) {
        this.failureDelegate = failureDelegate;
        this.client = client;
        this.parentTask = parentTask;
        this.rollupIndexName = rollupIndexName;
        this.timeout = timeout;
        this.taskQueue = taskQueue;
    }

    @Override
    public void onResponse(final RefreshResponse response) {
        if (response.getFailedShards() == 0) {
            // Mark rollup index as "completed successfully" ("index.rollup.status": "success")
            taskQueue.submitTask(
                "update-rollup-metadata [" + rollupIndexName + "]",
                new TransportDownsampleAction.RollupClusterStateUpdateTask(
                    new ForceMergeActionListener(parentTask, rollupIndexName, client, failureDelegate)
                ) {

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final Metadata metadata = currentState.metadata();
                        final Metadata.Builder metadataBuilder = Metadata.builder(metadata);
                        final Index rollupIndex = metadata.index(rollupIndexName).getIndex();
                        final IndexMetadata rollupIndexMetadata = metadata.index(rollupIndex);

                        metadataBuilder.updateSettings(
                            Settings.builder()
                                .put(rollupIndexMetadata.getSettings())
                                .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), IndexMetadata.DownsampleTaskStatus.SUCCESS)
                                .build(),
                            rollupIndexName
                        );
                        return ClusterState.builder(currentState).metadata(metadataBuilder.build()).build();
                    }
                },
                timeout
            );
        }
    }

    @Override
    public void onFailure(Exception e) {
        failureDelegate.onFailure(e);
    }

}
