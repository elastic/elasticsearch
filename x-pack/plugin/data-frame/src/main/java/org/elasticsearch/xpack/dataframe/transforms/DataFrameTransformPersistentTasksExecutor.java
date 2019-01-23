/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameTransformState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.dataframe.DataFrame;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.util.Map;

public class DataFrameTransformPersistentTasksExecutor extends PersistentTasksExecutor<DataFrameTransform> {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformPersistentTasksExecutor.class);

    private final Client client;
    private final DataFrameTransformsConfigManager transformsConfigManager;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;

    public DataFrameTransformPersistentTasksExecutor(Client client, DataFrameTransformsConfigManager transformsConfigManager,
            SchedulerEngine schedulerEngine, ThreadPool threadPool) {
        super(DataFrameField.TASK_NAME, DataFrame.TASK_THREAD_POOL_NAME);
        this.client = client;
        this.transformsConfigManager = transformsConfigManager;
        this.schedulerEngine = schedulerEngine;
        this.threadPool = threadPool;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, @Nullable DataFrameTransform params, PersistentTaskState state) {
        DataFrameTransformTask buildTask = (DataFrameTransformTask) task;
        SchedulerEngine.Job schedulerJob = new SchedulerEngine.Job(
                DataFrameTransformTask.SCHEDULE_NAME + "_" + params.getId(), next());

        // Note that while the task is added to the scheduler here, the internal state will prevent
        // it from doing any work until the task is "started" via the StartTransform api
        schedulerEngine.register(buildTask);
        schedulerEngine.add(schedulerJob);

        logger.info("Data frame transform [" + params.getId() + "] created.");
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
                (DataFrameTransformState) persistentTask.getState(), client, transformsConfigManager, schedulerEngine, threadPool, headers);
    }
}