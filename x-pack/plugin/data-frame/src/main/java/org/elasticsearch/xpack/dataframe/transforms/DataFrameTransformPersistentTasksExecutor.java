/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.notifications.Auditor;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.notifications.DataFrameAuditMessage;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.dataframe.DataFrame;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.util.Map;

public class DataFrameTransformPersistentTasksExecutor extends PersistentTasksExecutor<DataFrameTransform> {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformPersistentTasksExecutor.class);

    private final Client client;
    private final DataFrameTransformsConfigManager transformsConfigManager;
    private final DataFrameTransformsCheckpointService dataFrameTransformsCheckpointService;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;
    private final Auditor<DataFrameAuditMessage> auditor;

    public DataFrameTransformPersistentTasksExecutor(Client client,
                                                     DataFrameTransformsConfigManager transformsConfigManager,
                                                     DataFrameTransformsCheckpointService dataFrameTransformsCheckpointService,
                                                     SchedulerEngine schedulerEngine,
                                                     Auditor<DataFrameAuditMessage> auditor,
                                                     ThreadPool threadPool) {
        super(DataFrameField.TASK_NAME, DataFrame.TASK_THREAD_POOL_NAME);
        this.client = client;
        this.transformsConfigManager = transformsConfigManager;
        this.dataFrameTransformsCheckpointService = dataFrameTransformsCheckpointService;
        this.schedulerEngine = schedulerEngine;
        this.auditor = auditor;
        this.threadPool = threadPool;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, @Nullable DataFrameTransform params, PersistentTaskState state) {
        DataFrameTransformTask buildTask = (DataFrameTransformTask) task;
        SchedulerEngine.Job schedulerJob = new SchedulerEngine.Job(
                DataFrameTransformTask.SCHEDULE_NAME + "_" + params.getId(), next());
        DataFrameTransformState transformState = (DataFrameTransformState) state;
        if (transformState != null && transformState.getTaskState() == DataFrameTransformTaskState.FAILED) {
            logger.warn("Tried to start failed transform [" + params.getId() + "] failure reason: " + transformState.getReason());
            return;
        }
        transformsConfigManager.getTransformStats(params.getId(), ActionListener.wrap(
            stats -> {
                // Initialize with the previously recorded stats
                buildTask.initializePreviousStats(stats);
                scheduleTask(buildTask, schedulerJob, params.getId());
            },
            error -> {
                if (error instanceof ResourceNotFoundException == false) {
                    logger.error("Unable to load previously persisted statistics for transform [" + params.getId() + "]", error);
                }
                scheduleTask(buildTask, schedulerJob, params.getId());
            }
        ));
    }

    private void scheduleTask(DataFrameTransformTask buildTask, SchedulerEngine.Job schedulerJob, String id) {
        // Note that while the task is added to the scheduler here, the internal state will prevent
        // it from doing any work until the task is "started" via the StartTransform api
        schedulerEngine.register(buildTask);
        schedulerEngine.add(schedulerJob);

        logger.info("Data frame transform [" + id + "] created.");
    }

    static SchedulerEngine.Schedule next() {
        return (startTime, now) -> {
            return now + 1000; // to be fixed, hardcode something
        };
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
            PersistentTasksCustomMetaData.PersistentTask<DataFrameTransform> persistentTask, Map<String, String> headers) {
        return new DataFrameTransformTask(id, type, action, parentTaskId, persistentTask.getParams(),
            (DataFrameTransformState) persistentTask.getState(), client, transformsConfigManager,
            dataFrameTransformsCheckpointService, schedulerEngine, auditor, threadPool, headers);
    }
}
