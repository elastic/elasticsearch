/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStoredDoc;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.dataframe.DataFrame;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.persistence.SeqNoPrimaryTermAndIndex;
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
    private volatile int numFailureRetries;

    public DataFrameTransformPersistentTasksExecutor(Client client,
                                                     DataFrameTransformsConfigManager transformsConfigManager,
                                                     DataFrameTransformsCheckpointService dataFrameTransformsCheckpointService,
                                                     SchedulerEngine schedulerEngine,
                                                     DataFrameAuditor auditor,
                                                     ThreadPool threadPool,
                                                     ClusterService clusterService,
                                                     Settings settings) {
        super(DataFrameField.TASK_NAME, DataFrame.TASK_THREAD_POOL_NAME);
        this.client = client;
        this.transformsConfigManager = transformsConfigManager;
        this.dataFrameTransformsCheckpointService = dataFrameTransformsCheckpointService;
        this.schedulerEngine = schedulerEngine;
        this.auditor = auditor;
        this.threadPool = threadPool;
        this.numFailureRetries = DataFrameTransformTask.NUM_FAILURE_RETRIES_SETTING.get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DataFrameTransformTask.NUM_FAILURE_RETRIES_SETTING, this::setNumFailureRetries);
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
        DiscoveryNode discoveryNode = selectLeastLoadedNode(clusterState, (node) ->
            node.isDataNode() && node.getVersion().onOrAfter(params.getVersion())
        );
        return discoveryNode == null ? NO_NODE_FOUND : new PersistentTasksCustomMetaData.Assignment(discoveryNode.getId(), "");
    }

    static List<String> verifyIndicesPrimaryShardsAreActive(ClusterState clusterState) {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver();
        String[] indices = resolver.concreteIndexNames(clusterState,
            IndicesOptions.lenientExpandOpen(),
            DataFrameInternalIndex.INDEX_NAME_PATTERN);
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
        // NOTE: DataFrameTransformPersistentTasksExecutor#createTask pulls in the stored task state from the ClusterState when the object
        // is created. DataFrameTransformTask#ctor takes into account setting the task as failed if that is passed in with the
        // persisted state.
        // DataFrameTransformPersistentTasksExecutor#startTask will fail as DataFrameTransformTask#start, when force == false, will return
        // a failure indicating that a failed task cannot be started.
        //
        // We want the rest of the state to be populated in the task when it is loaded on the node so that users can force start it again
        // later if they want.

        final ClientDataFrameIndexerBuilder indexerBuilder =
            new ClientDataFrameIndexerBuilder()
                .setAuditor(auditor)
                .setClient(client)
                .setTransformsCheckpointService(dataFrameTransformsCheckpointService)
                .setTransformsConfigManager(transformsConfigManager);

        final SetOnce<DataFrameTransformState> stateHolder = new SetOnce<>();

        ActionListener<StartDataFrameTransformTaskAction.Response> startTaskListener = ActionListener.wrap(
            response -> logger.info("Successfully completed and scheduled task in node operation"),
            failure -> logger.error("Failed to start task ["+ transformId +"] in node operation", failure)
        );

        // <5> load next checkpoint
        ActionListener<DataFrameTransformCheckpoint> getTransformNextCheckpointListener = ActionListener.wrap(
                nextCheckpoint -> {

                    if (nextCheckpoint.isEmpty()) {
                        // extra safety: reset position and progress if next checkpoint is empty
                        // prevents a failure if for some reason the next checkpoint has been deleted
                        indexerBuilder.setInitialPosition(null);
                        indexerBuilder.setProgress(null);
                    } else {
                        logger.trace("[{}] Loaded next checkpoint [{}] found, starting the task", transformId,
                                nextCheckpoint.getCheckpoint());
                        indexerBuilder.setNextCheckpoint(nextCheckpoint);
                    }

                    final long lastCheckpoint = stateHolder.get().getCheckpoint();

                    startTask(buildTask, indexerBuilder, lastCheckpoint, startTaskListener);
                },
                error -> {
                    // TODO: do not use the same error message as for loading the last checkpoint
                    String msg = DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_LOAD_TRANSFORM_CHECKPOINT, transformId);
                    logger.error(msg, error);
                    markAsFailed(buildTask, msg);
                }
        );

        // <4> load last checkpoint
        ActionListener<DataFrameTransformCheckpoint> getTransformLastCheckpointListener = ActionListener.wrap(
                lastCheckpoint -> {
                    indexerBuilder.setLastCheckpoint(lastCheckpoint);

                    logger.trace("[{}] Loaded last checkpoint [{}], looking for next checkpoint", transformId,
                            lastCheckpoint.getCheckpoint());
                    transformsConfigManager.getTransformCheckpoint(transformId, lastCheckpoint.getCheckpoint() + 1,
                            getTransformNextCheckpointListener);
                },
                error -> {
                    String msg = DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_LOAD_TRANSFORM_CHECKPOINT, transformId);
                    logger.error(msg, error);
                    markAsFailed(buildTask, msg);
                }
        );

        // <3> Set the previous stats (if they exist), initialize the indexer, start the task (If it is STOPPED)
        // Since we don't create the task until `_start` is called, if we see that the task state is stopped, attempt to start
        // Schedule execution regardless
        ActionListener<Tuple<DataFrameTransformStoredDoc, SeqNoPrimaryTermAndIndex>> transformStatsActionListener = ActionListener.wrap(
            stateAndStatsAndSeqNoPrimaryTermAndIndex -> {
                DataFrameTransformStoredDoc stateAndStats = stateAndStatsAndSeqNoPrimaryTermAndIndex.v1();
                SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex = stateAndStatsAndSeqNoPrimaryTermAndIndex.v2();
                // Since we have not set the value for this yet, it SHOULD be null
                buildTask.updateSeqNoPrimaryTermAndIndex(null, seqNoPrimaryTermAndIndex);
                logger.trace("[{}] initializing state and stats: [{}]", transformId, stateAndStats.toString());
                indexerBuilder.setInitialStats(stateAndStats.getTransformStats())
                    .setInitialPosition(stateAndStats.getTransformState().getPosition())
                    .setProgress(stateAndStats.getTransformState().getProgress())
                    .setIndexerState(currentIndexerState(stateAndStats.getTransformState()));
                logger.debug("[{}] Loading existing state: [{}], position [{}]",
                    transformId,
                    stateAndStats.getTransformState(),
                    stateAndStats.getTransformState().getPosition());

                stateHolder.set(stateAndStats.getTransformState());
                final long lastCheckpoint = stateHolder.get().getCheckpoint();

                if (lastCheckpoint == 0) {
                    logger.trace("[{}] No last checkpoint found, looking for next checkpoint", transformId);
                    transformsConfigManager.getTransformCheckpoint(transformId, lastCheckpoint + 1, getTransformNextCheckpointListener);
                } else {
                    logger.trace ("[{}] Restore last checkpoint: [{}]", transformId, lastCheckpoint);
                    transformsConfigManager.getTransformCheckpoint(transformId, lastCheckpoint, getTransformLastCheckpointListener);
                }
            },
            error -> {
                if (error instanceof ResourceNotFoundException == false) {
                    String msg = DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_LOAD_TRANSFORM_STATE, transformId);
                    logger.error(msg, error);
                    markAsFailed(buildTask, msg);
                } else {
                    logger.trace("[{}] No stats found (new transform), starting the task", transformId);
                    startTask(buildTask, indexerBuilder, null, startTaskListener);
                }
            }
        );

        // <2> set fieldmappings for the indexer, get the previous stats (if they exist)
        ActionListener<Map<String, String>> getFieldMappingsListener = ActionListener.wrap(
            fieldMappings -> {
                indexerBuilder.setFieldMappings(fieldMappings);
                transformsConfigManager.getTransformStoredDoc(transformId, transformStatsActionListener);
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

    private void startTask(DataFrameTransformTask buildTask,
                           ClientDataFrameIndexerBuilder indexerBuilder,
                           Long previousCheckpoint,
                           ActionListener<StartDataFrameTransformTaskAction.Response> listener) {
        buildTask.initializeIndexer(indexerBuilder);
        // DataFrameTransformTask#start will fail if the task state is FAILED
        // Will continue to attempt to start the indexer, even if the state is STARTED
        buildTask.setNumFailureRetries(numFailureRetries).start(previousCheckpoint, false, false, listener);
    }

    private void setNumFailureRetries(int numFailureRetries) {
        this.numFailureRetries = numFailureRetries;
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
            PersistentTasksCustomMetaData.PersistentTask<DataFrameTransform> persistentTask, Map<String, String> headers) {
        return new DataFrameTransformTask(id, type, action, parentTaskId, persistentTask.getParams(),
            (DataFrameTransformState) persistentTask.getState(), schedulerEngine, auditor, threadPool, headers);
    }
}
