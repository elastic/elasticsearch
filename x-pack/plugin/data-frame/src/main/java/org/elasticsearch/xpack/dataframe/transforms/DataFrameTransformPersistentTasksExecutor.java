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
import org.elasticsearch.action.LatchedActionListener;
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
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction;
import org.elasticsearch.xpack.core.dataframe.notifications.DataFrameAuditMessage;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.dataframe.DataFrame;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.pivot.SchemaUtil;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.dataframe.transforms.TransformProgressGatherer.getProgress;

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
        final String transformId = params.getId();
        final DataFrameTransformTask buildTask = (DataFrameTransformTask) task;
        final SchedulerEngine.Job schedulerJob = new SchedulerEngine.Job(DataFrameTransformTask.SCHEDULE_NAME + "_" + transformId,
            next());
        final DataFrameTransformState transformState = (DataFrameTransformState) state;

        final DataFrameTransformTask.ClientDataFrameIndexerBuilder indexerBuilder =
            new DataFrameTransformTask.ClientDataFrameIndexerBuilder()
                .setAuditor(auditor)
                .setClient(client)
                .setIndexerState(transformState == null ? IndexerState.STOPPED : transformState.getIndexerState())
                .setInitialPosition(transformState == null ? null : transformState.getPosition())
                .setTransformsCheckpointService(dataFrameTransformsCheckpointService)
                .setTransformsConfigManager(transformsConfigManager)
                .setTransformId(transformId);

        ActionListener<StartDataFrameTransformTaskAction.Response> startTaskListener = ActionListener.wrap(
            response -> logger.info("Successfully completed and scheduled task in node operation"),
            failure -> logger.error("Failed to start task ["+ transformId +"] in node operation", failure)
        );

        // <4> Set the progress in the indexer (if it exists), set the task's indexer, and attempt to start the task (if it is STOPPED)
        // Since we don't create the task until `_start` is called, if we see that the task state is stopped, attempt to start
        // Schedule execution regardless
        ActionListener<DataFrameTransformProgress> progressActionListener = ActionListener.wrap(
            progress -> {
                indexerBuilder.setProgress(progress);
                buildTask.initializeIndexer(indexerBuilder);
                scheduleAndStartTask(buildTask, schedulerJob, params.getId(), startTaskListener);
            },
            error -> {
                logger.error("Unable to gather transform progress[" + transformId + "]", error);
                buildTask.initializeIndexer(indexerBuilder);
                scheduleAndStartTask(buildTask, schedulerJob, transformId, startTaskListener);
            }
        );

        // <3> Set the previous stats (if they exist), attempt to get the progress of the task
        ActionListener<DataFrameIndexerTransformStats> transformStatsActionListener = ActionListener.wrap(
            stats -> {
                indexerBuilder.setInitialStats(stats);
                getProgress(client, indexerBuilder.getTransformConfig(), indexerBuilder.getInitialPosition(), progressActionListener);
            },
            error -> {
                if (error instanceof ResourceNotFoundException == false) {
                    logger.error("Unable to load previously persisted statistics for transform [" + params.getId() + "]", error);
                }
                indexerBuilder.setInitialStats(new DataFrameIndexerTransformStats(transformId));
                getProgress(client, indexerBuilder.getTransformConfig(), indexerBuilder.getInitialPosition(), progressActionListener);
            }
        );

        // <2> set fieldmappings for the indexer, get the previous stats (if they exist)
        ActionListener<Map<String, String>> getFieldMappingsListener = ActionListener.wrap(
            fieldMappings -> {
                indexerBuilder.setFieldMappings(fieldMappings);
                transformsConfigManager.getTransformStats(transformId, transformStatsActionListener);
            },
            error -> {
                String msg = DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_UNABLE_TO_GATHER_FIELD_MAPPINGS,
                    indexerBuilder.getTransformConfig().getDestination().getIndex());
                logger.error(msg, error);
                markAsFailed(buildTask, msg);
            }
        );

        // <1> Validate the transform, assigning it to the indexer, and get the field mappings
        ActionListener<DataFrameTransformConfig> getTransformConfigListener = ActionListener.wrap(
            config -> {
                if (config.isValid()) {
                    indexerBuilder.setTransformConfig(config);
                    SchemaUtil.getDestinationFieldMappings(client, config.getDestination().getIndex(), getFieldMappingsListener);
                } else {
                    markAsFailed(buildTask,
                        DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_TRANSFORM_CONFIGURATION_INVALID, transformId));
                }
            },
            error -> {
                String msg = DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_LOAD_TRANSFORM_CONFIGURATION, transformId);
                logger.error(msg, error);
                markAsFailed(buildTask, msg);
            }
        );
        // <0> Get the transform config
        transformsConfigManager.getTransformConfiguration(transformId, getTransformConfigListener);
    }

    private void markAsFailed(DataFrameTransformTask task, String reason) {
        CountDownLatch latch = new CountDownLatch(1);

        task.markAsFailed(reason, new LatchedActionListener<>(ActionListener.wrap(
            nil -> {},
            failure -> logger.error("Failed to set task [" + task.getTransformId() +"] to failed", failure)
        ), latch));
        try {
            latch.await(90, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("timeout waiting for task [" + task.getTransformId() + "] to be marked as failed in cluster state", e);
        }
    }

    private void scheduleAndStartTask(DataFrameTransformTask buildTask,
                                      SchedulerEngine.Job schedulerJob,
                                      String id,
                                      ActionListener<StartDataFrameTransformTaskAction.Response> listener) {
        // Note that while the task is added to the scheduler here, the internal state will prevent
        // it from doing any work until the task is "started" via the StartTransform api
        schedulerEngine.register(buildTask);
        schedulerEngine.add(schedulerJob);
        logger.info("Data frame transform [" + id + "] created.");
        // If we are stopped, and it is an initial run, this means we have never been started,
        // attempt to start the task
        if (buildTask.getState().getTaskState().equals(DataFrameTransformTaskState.STOPPED) && buildTask.isInitialRun()) {
            buildTask.start(listener);
        } else {
            logger.info("No need to start task. Its current state is: " + buildTask.getState().getIndexerState());
            listener.onResponse(new StartDataFrameTransformTaskAction.Response(true));
        }
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
            (DataFrameTransformState) persistentTask.getState(), schedulerEngine, auditor, threadPool, headers);
    }
}
