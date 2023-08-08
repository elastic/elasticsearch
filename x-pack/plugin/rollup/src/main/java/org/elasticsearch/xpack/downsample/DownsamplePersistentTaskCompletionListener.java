/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles operations to be carried out once the downsample persistent task completes.
 */
public class DownsamplePersistentTaskCompletionListener
    implements
        PersistentTasksService.WaitForPersistentTaskListener<RollupShardTaskParams> {

    private static final Logger logger = LogManager.getLogger(DownsamplePersistentTaskCompletionListener.class);

    private final ActionListener<AcknowledgedResponse> failureDelegate;
    private final Client client;
    private final TaskId parentTask;
    private final String rollupIndexName;
    private final TimeValue timeout;
    private final MasterServiceTaskQueue<TransportDownsampleAction.RollupClusterStateUpdateTask> taskQueue;
    private final AtomicInteger countDown;
    private final IndexMetadata sourceIndexMetadata;

    public DownsamplePersistentTaskCompletionListener(
        final int numberOfTasks,
        final IndexMetadata sourceIndexMetadata,
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
        this.countDown = new AtomicInteger(numberOfTasks);
        this.sourceIndexMetadata = sourceIndexMetadata;
    }

    @Override
    public void onResponse(final PersistentTasksCustomMetadata.PersistentTask<RollupShardTaskParams> persistentTask) {
        final RollupShardTaskParams params = persistentTask.getParams();
        logger.info("Downsampling task [" + persistentTask + " completed for shard " + params.shardId());
        if (countDown.decrementAndGet() != 0) {
            return;
        }
        logger.info("All downsampling tasks completed");

        final Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true);
        // Number of replicas had been previously set to 0 to speed up index population
        if (sourceIndexMetadata.getNumberOfReplicas() > 0) {
            settings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, sourceIndexMetadata.getNumberOfReplicas());
        }
        // Setting index.hidden has been initially set to true. We revert this to the value of the
        // source index
        if (sourceIndexMetadata.isHidden() == false) {
            if (sourceIndexMetadata.getSettings().keySet().contains(IndexMetadata.SETTING_INDEX_HIDDEN)) {
                settings.put(IndexMetadata.SETTING_INDEX_HIDDEN, false);
            } else {
                settings.putNull(IndexMetadata.SETTING_INDEX_HIDDEN);
            }
        }
        final UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(settings.build(), rollupIndexName);
        updateSettingsReq.setParentTask(parentTask);
        client.admin()
            .indices()
            .updateSettings(
                updateSettingsReq,
                new UpdateRollupIndexSettingsActionListener(failureDelegate, client, parentTask, rollupIndexName, timeout, taskQueue)
            );
    }

    @Override
    public void onFailure(Exception e) {
        failureDelegate.onFailure(e);
    }
}
