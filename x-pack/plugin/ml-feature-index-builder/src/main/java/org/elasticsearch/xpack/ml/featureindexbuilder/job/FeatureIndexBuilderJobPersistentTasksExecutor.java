/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.ml.featureindexbuilder.FeatureIndexBuilder;

import java.util.Map;

public class FeatureIndexBuilderJobPersistentTasksExecutor extends PersistentTasksExecutor<FeatureIndexBuilderJob> {
    private final Client client;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;

    public FeatureIndexBuilderJobPersistentTasksExecutor(Settings settings, Client client, SchedulerEngine schedulerEngine,
            ThreadPool threadPool) {
        super(settings, "xpack/feature_index_builder/job", FeatureIndexBuilder.TASK_THREAD_POOL_NAME);
        this.client = client;
        this.schedulerEngine = schedulerEngine;
        this.threadPool = threadPool;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, @Nullable FeatureIndexBuilderJob params, PersistentTaskState state) {
        FeatureIndexBuilderJobTask buildTask = (FeatureIndexBuilderJobTask) task;
        SchedulerEngine.Job schedulerJob = new SchedulerEngine.Job(FeatureIndexBuilderJobTask.SCHEDULE_NAME + "_" + params.getConfig().getId(), next());

        // Note that while the task is added to the scheduler here, the internal state
        // will prevent
        // it from doing any work until the task is "started" via the StartJob api
        schedulerEngine.register(buildTask);
        schedulerEngine.add(schedulerJob);

        logger.info("FeatureIndexBuilder job [" + params.getConfig().getId() + "] created.");
    }

    static SchedulerEngine.Schedule next() {
        return (startTime, now) -> {
            return now + 1000; // to be fixed, hardcode something
        };
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
            PersistentTasksCustomMetaData.PersistentTask<FeatureIndexBuilderJob> persistentTask, Map<String, String> headers) {
        return new FeatureIndexBuilderJobTask(id, type, action, parentTaskId, persistentTask.getParams(),
                (FeatureIndexBuilderJobState) persistentTask.getState(), client, schedulerEngine, threadPool, headers);
    }
}