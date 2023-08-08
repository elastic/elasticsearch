/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;

/**
 * Refreshes the downsample target index
 */
public class UpdateRollupIndexSettingsActionListener implements ActionListener<AcknowledgedResponse> {
    private final ActionListener<AcknowledgedResponse> failureDelegate;
    private final Client client;
    private final TaskId parentTask;
    private final String rollupIndexName;
    private final TimeValue timeout;
    private final MasterServiceTaskQueue<TransportDownsampleAction.RollupClusterStateUpdateTask> taskQueue;

    public UpdateRollupIndexSettingsActionListener(
        final ActionListener<AcknowledgedResponse> failureDelegate,
        final Client client,
        final TaskId parentTask,
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
    public void onResponse(final AcknowledgedResponse response) {
        if (response.isAcknowledged()) {
            final RefreshRequest request = new RefreshRequest(rollupIndexName);
            request.setParentTask(parentTask);
            client.admin()
                .indices()
                .refresh(
                    request,
                    new RefreshRollupIndexActionListener(failureDelegate, client, parentTask, rollupIndexName, timeout, taskQueue)
                );
        }
    }

    @Override
    public void onFailure(Exception e) {
        failureDelegate.onFailure(e);
    }

}
