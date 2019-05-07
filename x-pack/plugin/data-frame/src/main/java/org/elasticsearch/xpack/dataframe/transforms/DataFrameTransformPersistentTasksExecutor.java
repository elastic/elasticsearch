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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.dataframe.DataFrame;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.pivot.SchemaUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DataFrameTransformPersistentTasksExecutor extends PersistentTasksExecutor<DataFrameTransform> {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformPersistentTasksExecutor.class);

    // The amount of time we wait for the cluster state to respond when being marked as failed
    private static final int MARK_AS_FAILED_TIMEOUT_SEC = 90;
    private final Client client;
    private final DataFrameTransformsConfigManager transformsConfigManager;
    private final DataFrameTransformsCheckpointService dataFrameTransformsCheckpointService;
    private final SchedulerEngine schedulerEngine;
    private final ThreadPool threadPool;
    private final DataFrameAuditor auditor;

    public DataFrameTransformPersistentTasksExecutor(Client client,
                                                     DataFrameTransformsConfigManager transformsConfigManager,
                                                     DataFrameTransformsCheckpointService dataFrameTransformsCheckpointService,
                                                     SchedulerEngine schedulerEngine,
                                                     DataFrameAuditor auditor,
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
    public PersistentTasksCustomMetaData.Assignment getAssignment(DataFrameTransform params, ClusterState clusterState) {
        List<String> unavailableIndices = verifyIndicesPrimaryShardsAreActive(clusterState);
        if (unavailableIndices.size() != 0) {
            String reason = "Not starting data frame transform [" + params.getId() + "], " +
                "because not all primary shards are active for the following indices [" +
                String.join(",", unavailableIndices) + "]";
            logger.debug(reason);
            return new PersistentTasksCustomMetaData.Assignment(null, reason);
        }
        return super.getAssignment(params, clusterState);
    }

    static List<String> verifyIndicesPrimaryShardsAreActive(ClusterState clusterState) {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver();
        String[] indices = resolver.concreteIndexNames(clusterState,
            IndicesOptions.lenientExpandOpen(),
            DataFrameInternalIndex.INDEX_TEMPLATE_PATTERN + "*");
        List<String> unavailableIndices = new ArrayList<>(indices.length);
        for (String index : indices) {
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                unavailableIndices.add(index);
            }
        }
        return unavailableIndices;
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
                .setIndexerState(currentIndexerState(transformState))
                .setInitialPosition(transformState == null ? null : transformState.getPosition())
                // If the state is `null` that means this is a "first run". We can safely assume the
                // task will attempt to gather the initial progress information
                // if we have state, this may indicate the previous execution node crashed, so we should attempt to retrieve
                // the progress from state to keep an accurate measurement of our progress
                .setProgress(transformState == null ? null : transformState.getProgress())
                .setTransformsCheckpointService(dataFrameTransformsCheckpointService)
                .setTransformsConfigManager(transformsConfigManager)
                .setTransformId(transformId);

        ActionListener<StartDataFrameTransformTaskAction.Response> startTaskListener = ActionListener.wrap(
            response -> logger.info("Successfully completed and scheduled task in node operation"),
            failure -> logger.error("Failed to start task ["+ transformId +"] in node operation", failure)
        );

        // <3> Set the previous stats (if they exist), initialize the indexer, start the task (If it is STOPPED)
        // Since we don't create the task until `_start` is called, if we see that the task state is stopped, attempt to start
        // Schedule execution regardless
        ActionListener<DataFrameIndexerTransformStats> transformStatsActionListener = ActionListener.wrap(
            stats -> {
                indexerBuilder.setInitialStats(stats);
                buildTask.initializeIndexer(indexerBuilder);
                scheduleAndStartTask(buildTask, schedulerJob, startTaskListener);
            },
            error -> {
                if (error instanceof ResourceNotFoundException == false) {
                    logger.error("Unable to load previously persisted statistics for transform [" + params.getId() + "]", error);
                }
                indexerBuilder.setInitialStats(new DataFrameIndexerTransformStats(transformId));
                buildTask.initializeIndexer(indexerBuilder);
                scheduleAndStartTask(buildTask, schedulerJob, startTaskListener);
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

    private static IndexerState currentIndexerState(DataFrameTransformState previousState) {
        if (previousState == null) {
            return IndexerState.STOPPED;
        }
        switch(previousState.getIndexerState()){
            // If it is STARTED or INDEXING we want to make sure we revert to started
            // Otherwise, the internal indexer will never get scheduled and execute
            case STARTED:
            case INDEXING:
                return IndexerState.STARTED;
            // If we are STOPPED, STOPPING, or ABORTING and just started executing on this node,
            //  then it is safe to say we should be STOPPED
            case STOPPED:
            case STOPPING:
            case ABORTING:
            default:
                return IndexerState.STOPPED;
        }
    }

    private void markAsFailed(DataFrameTransformTask task, String reason) {
        CountDownLatch latch = new CountDownLatch(1);

        task.markAsFailed(reason, new LatchedActionListener<>(ActionListener.wrap(
            nil -> {},
            failure -> logger.error("Failed to set task [" + task.getTransformId() +"] to failed", failure)
        ), latch));
        try {
            latch.await(MARK_AS_FAILED_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Timeout waiting for task [" + task.getTransformId() + "] to be marked as failed in cluster state", e);
        }
    }

    private void scheduleAndStartTask(DataFrameTransformTask buildTask,
                                      SchedulerEngine.Job schedulerJob,
                                      ActionListener<StartDataFrameTransformTaskAction.Response> listener) {
        // Note that while the task is added to the scheduler here, the internal state will prevent
        // it from doing any work until the task is "started" via the StartTransform api
        schedulerEngine.register(buildTask);
        schedulerEngine.add(schedulerJob);
        logger.info("Data frame transform [{}] created.", buildTask.getTransformId());
        // If we are stopped, and it is an initial run, this means we have never been started,
        // attempt to start the task
        if (buildTask.getState().getTaskState().equals(DataFrameTransformTaskState.STOPPED) && buildTask.isInitialRun()) {
            buildTask.start(listener);
        } else {
            logger.debug("No need to start task. Its current state is: {}", buildTask.getState().getIndexerState());
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
