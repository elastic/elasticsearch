/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.time.Clock;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class DataFrameAnalyticsManager {

    private static final Logger LOGGER = LogManager.getLogger(DataFrameAnalyticsManager.class);

    /**
     * We need a {@link NodeClient} to get the reindexing task and be able to report progress
     */
    private final NodeClient client;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final AnalyticsProcessManager processManager;
    private final DataFrameAnalyticsAuditor auditor;

    public DataFrameAnalyticsManager(NodeClient client, DataFrameAnalyticsConfigProvider configProvider,
                                     AnalyticsProcessManager processManager, DataFrameAnalyticsAuditor auditor) {
        this.client = Objects.requireNonNull(client);
        this.configProvider = Objects.requireNonNull(configProvider);
        this.processManager = Objects.requireNonNull(processManager);
        this.auditor = Objects.requireNonNull(auditor);
    }

    public void execute(DataFrameAnalyticsTask task, DataFrameAnalyticsState currentState, ClusterState clusterState) {
        // With config in hand, determine action to take
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(
            config -> {
                switch(currentState) {
                    // If we are STARTED, it means the job was started because the start API was called.
                    // We should determine the job's starting state based on its previous progress.
                    case STARTED:
                        executeStartingJob(task, config);
                        break;
                    // The task has fully reindexed the documents and we should continue on with our analyses
                    case ANALYZING:
                        LOGGER.debug("[{}] Reassigning job that was analyzing", config.getId());
                        startAnalytics(task, config, true);
                        break;
                    // If we are already at REINDEXING, we are not 100% sure if we reindexed ALL the docs.
                    // We will delete the destination index, recreate, reindex
                    case REINDEXING:
                        LOGGER.debug("[{}] Reassigning job that was reindexing", config.getId());
                        executeJobInMiddleOfReindexing(task, config);
                        break;
                    default:
                        task.updateState(DataFrameAnalyticsState.FAILED, "Cannot execute analytics task [" + config.getId() +
                            "] as it is in unknown state [" + currentState + "]. Must be one of [STARTED, REINDEXING, ANALYZING]");
                }

            },
            error -> task.updateState(DataFrameAnalyticsState.FAILED, error.getMessage())
        );

        // Retrieve configuration
        ActionListener<Boolean> stateAliasListener = ActionListener.wrap(
            aBoolean -> configProvider.get(task.getParams().getId(), configListener),
            configListener::onFailure
        );

        // Make sure the state index and alias exist
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client, clusterState, stateAliasListener);
    }

    private void executeStartingJob(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        DataFrameAnalyticsTaskState reindexingState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.REINDEXING,
            task.getAllocationId(), null);
        DataFrameAnalyticsTask.StartingState startingState = DataFrameAnalyticsTask.determineStartingState(
            config.getId(), task.getParams().getProgressOnStart());

        LOGGER.debug("[{}] Starting job from state [{}]", config.getId(), startingState);
        switch (startingState) {
            case FIRST_TIME:
                task.updatePersistentTaskState(reindexingState, ActionListener.wrap(
                    updatedTask -> reindexDataframeAndStartAnalysis(task, config),
                    error -> task.updateState(DataFrameAnalyticsState.FAILED, error.getMessage())
                ));
                break;
            case RESUMING_REINDEXING:
                task.updatePersistentTaskState(reindexingState, ActionListener.wrap(
                    updatedTask -> executeJobInMiddleOfReindexing(task, config),
                    error -> task.updateState(DataFrameAnalyticsState.FAILED, error.getMessage())
                ));
                break;
            case RESUMING_ANALYZING:
                startAnalytics(task, config, true);
                break;
            case FINISHED:
            default:
                task.updateState(DataFrameAnalyticsState.FAILED, "Unexpected starting state [" + startingState + "]");
        }
    }

    private void executeJobInMiddleOfReindexing(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        ClientHelper.executeAsyncWithOrigin(client,
            ML_ORIGIN,
            DeleteIndexAction.INSTANCE,
            new DeleteIndexRequest(config.getDest().getIndex()),
            ActionListener.wrap(
                r-> reindexDataframeAndStartAnalysis(task, config),
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                        reindexDataframeAndStartAnalysis(task, config);
                    } else {
                        task.updateState(DataFrameAnalyticsState.FAILED, e.getMessage());
                    }
                }
            ));
    }

    private void reindexDataframeAndStartAnalysis(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        if (task.isStopping()) {
            // The task was requested to stop before we started reindexing
            task.markAsCompleted();
            return;
        }

        // Reindexing is complete; start analytics
        ActionListener<BulkByScrollResponse> reindexCompletedListener = ActionListener.wrap(
            refreshResponse -> {
                if (task.isStopping()) {
                    LOGGER.debug("[{}] Stopping before starting analytics process", config.getId());
                    return;
                }
                task.setReindexingTaskId(null);
                task.setReindexingFinished();
                auditor.info(
                    config.getId(),
                    Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_FINISHED_REINDEXING, config.getDest().getIndex()));
                startAnalytics(task, config, false);
            },
            error -> task.updateState(DataFrameAnalyticsState.FAILED, error.getMessage())
        );

        // Reindex
        ActionListener<CreateIndexResponse> copyIndexCreatedListener = ActionListener.wrap(
            createIndexResponse -> {
                ReindexRequest reindexRequest = new ReindexRequest();
                reindexRequest.setSourceIndices(config.getSource().getIndex());
                reindexRequest.setSourceQuery(config.getSource().getParsedQuery());
                reindexRequest.getSearchRequest().source().fetchSource(config.getSource().getSourceFiltering());
                reindexRequest.setDestIndex(config.getDest().getIndex());
                reindexRequest.setScript(new Script("ctx._source." + DataFrameAnalyticsIndex.ID_COPY + " = ctx._id"));

                final ThreadContext threadContext = client.threadPool().getThreadContext();
                final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
                try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(ML_ORIGIN)) {
                    Task reindexTask = client.executeLocally(ReindexAction.INSTANCE, reindexRequest,
                        new ContextPreservingActionListener<>(supplier, reindexCompletedListener));
                    task.setReindexingTaskId(reindexTask.getId());
                }
            },
            reindexCompletedListener::onFailure
        );

        // Create destination index if it does not exist
        ActionListener<GetIndexResponse> destIndexListener = ActionListener.wrap(
            indexResponse -> {
                auditor.info(
                    config.getId(),
                    Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_REUSING_DEST_INDEX, indexResponse.indices()[0]));
                LOGGER.info("[{}] Using existing destination index [{}]", config.getId(), indexResponse.indices()[0]);
                DataFrameAnalyticsIndex.updateMappingsToDestIndex(client, config, indexResponse, ActionListener.wrap(
                    acknowledgedResponse -> copyIndexCreatedListener.onResponse(null),
                    copyIndexCreatedListener::onFailure
                ));
            },
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                    auditor.info(
                        config.getId(),
                        Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_CREATING_DEST_INDEX, config.getDest().getIndex()));
                    LOGGER.info("[{}] Creating destination index [{}]", config.getId(), config.getDest().getIndex());
                    DataFrameAnalyticsIndex.createDestinationIndex(client, Clock.systemUTC(), config, copyIndexCreatedListener);
                } else {
                    copyIndexCreatedListener.onFailure(e);
                }
            }
        );

        ClientHelper.executeWithHeadersAsync(config.getHeaders(), ML_ORIGIN, client, GetIndexAction.INSTANCE,
                new GetIndexRequest().indices(config.getDest().getIndex()), destIndexListener);
    }

    private void startAnalytics(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config, boolean isTaskRestarting) {
        // Ensure we mark reindexing is finished for the case we are recovering a task that had finished reindexing
        task.setReindexingFinished();

        // Update state to ANALYZING and start process
        ActionListener<DataFrameDataExtractorFactory> dataExtractorFactoryListener = ActionListener.wrap(
            dataExtractorFactory -> {
                DataFrameAnalyticsTaskState analyzingState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.ANALYZING,
                    task.getAllocationId(), null);
                task.updatePersistentTaskState(analyzingState, ActionListener.wrap(
                    updatedTask -> processManager.runJob(task, config, dataExtractorFactory),
                    error -> {
                        if (ExceptionsHelper.unwrapCause(error) instanceof ResourceNotFoundException) {
                            // Task has stopped
                        } else {
                            task.updateState(DataFrameAnalyticsState.FAILED, error.getMessage());
                        }
                    }
                ));
            },
            error -> task.updateState(DataFrameAnalyticsState.FAILED, error.getMessage())
        );

        // TODO This could fail with errors. In that case we get stuck with the copied index.
        // We could delete the index in case of failure or we could try building the factory before reindexing
        // to catch the error early on.
        DataFrameDataExtractorFactory.createForDestinationIndex(client, config, isTaskRestarting, dataExtractorFactoryListener);
    }

    public void stop(DataFrameAnalyticsTask task) {
        processManager.stop(task);
    }
}
