/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.time.Clock;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeWithHeadersAsync;

public class DataFrameAnalyticsManager {

    private static final Logger LOGGER = LogManager.getLogger(DataFrameAnalyticsManager.class);

    /**
     * We need a {@link NodeClient} to get the reindexing task and be able to report progress
     */
    private final NodeClient client;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final AnalyticsProcessManager processManager;
    private final DataFrameAnalyticsAuditor auditor;
    private final IndexNameExpressionResolver expressionResolver;

    public DataFrameAnalyticsManager(NodeClient client, DataFrameAnalyticsConfigProvider configProvider,
                                     AnalyticsProcessManager processManager, DataFrameAnalyticsAuditor auditor,
                                     IndexNameExpressionResolver expressionResolver) {
        this.client = Objects.requireNonNull(client);
        this.configProvider = Objects.requireNonNull(configProvider);
        this.processManager = Objects.requireNonNull(processManager);
        this.auditor = Objects.requireNonNull(auditor);
        this.expressionResolver = Objects.requireNonNull(expressionResolver);
    }

    public void execute(DataFrameAnalyticsTask task, DataFrameAnalyticsState currentState, ClusterState clusterState) {
        // With config in hand, determine action to take
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(
            config -> {
                // At this point we have the config at hand and we can reset the progress tracker
                // to use the analyses phases. We preserve reindexing progress as if reindexing was
                // finished it will not be reset.
                task.getStatsHolder().resetProgressTrackerPreservingReindexingProgress(config.getAnalysis().getProgressPhases());

                switch(currentState) {
                    // If we are STARTED, it means the job was started because the start API was called.
                    // We should determine the job's starting state based on its previous progress.
                    case STARTED:
                        executeStartingJob(task, config);
                        break;
                    // The task has fully reindexed the documents and we should continue on with our analyses
                    case ANALYZING:
                        LOGGER.debug("[{}] Reassigning job that was analyzing", config.getId());
                        startAnalytics(task, config);
                        break;
                    // If we are already at REINDEXING, we are not 100% sure if we reindexed ALL the docs.
                    // We will delete the destination index, recreate, reindex
                    case REINDEXING:
                        LOGGER.debug("[{}] Reassigning job that was reindexing", config.getId());
                        executeJobInMiddleOfReindexing(task, config);
                        break;
                    default:
                        task.setFailed(ExceptionsHelper.serverError("Cannot execute analytics task [" + config.getId() +
                            "] as it is in unknown state [" + currentState + "]. Must be one of [STARTED, REINDEXING, ANALYZING]"));
                }

            },
            task::setFailed
        );

        // Retrieve configuration
        ActionListener<Boolean> statsIndexListener = ActionListener.wrap(
            aBoolean -> configProvider.get(task.getParams().getId(), configListener),
            configListener::onFailure
        );

        // Make sure the stats index and alias exist
        ActionListener<Boolean> stateAliasListener = ActionListener.wrap(
            aBoolean -> createStatsIndexAndUpdateMappingsIfNecessary(new ParentTaskAssigningClient(client, task.getParentTaskId()),
                    clusterState, statsIndexListener), configListener::onFailure
        );

        // Make sure the state index and alias exist
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(new ParentTaskAssigningClient(client, task.getParentTaskId()),
                clusterState, expressionResolver, stateAliasListener);
    }

    private void createStatsIndexAndUpdateMappingsIfNecessary(Client client, ClusterState clusterState, ActionListener<Boolean> listener) {
        ActionListener<Boolean> createIndexListener = ActionListener.wrap(
            aBoolean -> ElasticsearchMappings.addDocMappingIfMissing(
                    MlStatsIndex.writeAlias(),
                    MlStatsIndex::mapping,
                    client,
                    clusterState,
                    listener)
            , listener::onFailure
        );

        MlStatsIndex.createStatsIndexAndAliasIfNecessary(client, clusterState, expressionResolver, createIndexListener);
    }

    private void executeStartingJob(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        if (task.isStopping()) {
            LOGGER.debug("[{}] task is stopping. Marking as complete before starting job.", task.getParams().getId());
            task.markAsCompleted();
            return;
        }
        DataFrameAnalyticsTaskState reindexingState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.REINDEXING,
            task.getAllocationId(), null);
        DataFrameAnalyticsTask.StartingState startingState = DataFrameAnalyticsTask.determineStartingState(
            config.getId(), task.getParams().getProgressOnStart());

        LOGGER.debug("[{}] Starting job from state [{}]", config.getId(), startingState);
        switch (startingState) {
            case FIRST_TIME:
                task.updatePersistentTaskState(reindexingState, ActionListener.wrap(
                    updatedTask -> reindexDataframeAndStartAnalysis(task, config),
                    task::setFailed
                ));
                break;
            case RESUMING_REINDEXING:
                task.updatePersistentTaskState(reindexingState, ActionListener.wrap(
                    updatedTask -> executeJobInMiddleOfReindexing(task, config),
                    task::setFailed
                ));
                break;
            case RESUMING_ANALYZING:
                startAnalytics(task, config);
                break;
            case FINISHED:
            default:
                task.setFailed(ExceptionsHelper.serverError("Unexpected starting state [" + startingState + "]"));
        }
    }

    private void executeJobInMiddleOfReindexing(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        if (task.isStopping()) {
            LOGGER.debug("[{}] task is stopping. Marking as complete before restarting reindexing.", task.getParams().getId());
            task.markAsCompleted();
            return;
        }
        ClientHelper.executeAsyncWithOrigin(new ParentTaskAssigningClient(client, task.getParentTaskId()),
            ML_ORIGIN,
            DeleteIndexAction.INSTANCE,
            new DeleteIndexRequest(config.getDest().getIndex()),
            ActionListener.wrap(
                r-> reindexDataframeAndStartAnalysis(task, config),
                e -> {
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof IndexNotFoundException) {
                        reindexDataframeAndStartAnalysis(task, config);
                    } else {
                        task.setFailed(e);
                    }
                }
            ));
    }

    private void reindexDataframeAndStartAnalysis(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        if (task.isStopping()) {
            LOGGER.debug("[{}] task is stopping. Marking as complete before starting reindexing and analysis.",
                task.getParams().getId());
            task.markAsCompleted();
            return;
        }

        final ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, task.getParentTaskId());

        // Reindexing is complete; start analytics
        ActionListener<BulkByScrollResponse> reindexCompletedListener = ActionListener.wrap(
            reindexResponse -> {
                // If the reindex task is canceled, this listener is called.
                // Consequently, we should not signal reindex completion.
                if (task.isStopping()) {
                    LOGGER.debug("[{}] task is stopping. Marking as complete before marking reindex as finished.",
                        task.getParams().getId());
                    task.markAsCompleted();
                    return;
                }
                task.setReindexingTaskId(null);
                auditor.info(
                    config.getId(),
                    Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_FINISHED_REINDEXING, config.getDest().getIndex(),
                        reindexResponse.getTook()));
                startAnalytics(task, config);
            },
            error -> {
                if (task.isStopping() && isTaskCancelledException(error)) {
                    LOGGER.debug(new ParameterizedMessage("[{}] Caught task cancelled exception while task is stopping",
                        config.getId()), error);
                    task.markAsCompleted();
                } else {
                    task.setFailed(error);
                }
            }
        );

        // Reindex
        ActionListener<CreateIndexResponse> copyIndexCreatedListener = ActionListener.wrap(
            createIndexResponse -> {
                ReindexRequest reindexRequest = new ReindexRequest();
                reindexRequest.setRefresh(true);
                reindexRequest.setSourceIndices(config.getSource().getIndex());
                reindexRequest.setSourceQuery(config.getSource().getParsedQuery());
                reindexRequest.getSearchRequest().source().fetchSource(config.getSource().getSourceFiltering());
                reindexRequest.setDestIndex(config.getDest().getIndex());
                reindexRequest.setScript(new Script("ctx._source." + DestinationIndex.ID_COPY + " = ctx._id"));
                reindexRequest.setParentTask(task.getParentTaskId());

                final ThreadContext threadContext = parentTaskClient.threadPool().getThreadContext();
                final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
                try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(ML_ORIGIN)) {
                    LOGGER.info("[{}] Started reindexing", config.getId());
                    Task reindexTask = client.executeLocally(ReindexAction.INSTANCE, reindexRequest,
                        new ContextPreservingActionListener<>(supplier, reindexCompletedListener));
                    task.setReindexingTaskId(reindexTask.getId());
                    auditor.info(config.getId(),
                        Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_AUDIT_STARTED_REINDEXING, config.getDest().getIndex()));
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
                DestinationIndex.updateMappingsToDestIndex(parentTaskClient, config, indexResponse, ActionListener.wrap(
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
                    DestinationIndex.createDestinationIndex(parentTaskClient, Clock.systemUTC(), config, copyIndexCreatedListener);
                } else {
                    copyIndexCreatedListener.onFailure(e);
                }
            }
        );

        ClientHelper.executeWithHeadersAsync(config.getHeaders(), ML_ORIGIN, parentTaskClient, GetIndexAction.INSTANCE,
                new GetIndexRequest().indices(config.getDest().getIndex()), destIndexListener);
    }

    private static boolean isTaskCancelledException(Exception error) {
        return ExceptionsHelper.unwrapCause(error) instanceof TaskCancelledException
            || ExceptionsHelper.unwrapCause(error.getCause()) instanceof TaskCancelledException;
    }

    private void startAnalytics(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        if (task.isStopping()) {
            LOGGER.debug("[{}] task is stopping. Marking as complete before starting analysis.", task.getParams().getId());
            task.markAsCompleted();
            return;
        }

        final ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, task.getParentTaskId());
        // Update state to ANALYZING and start process
        ActionListener<DataFrameDataExtractorFactory> dataExtractorFactoryListener = ActionListener.wrap(
            dataExtractorFactory -> {
                DataFrameAnalyticsTaskState analyzingState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.ANALYZING,
                    task.getAllocationId(), null);
                task.updatePersistentTaskState(analyzingState, ActionListener.wrap(
                    updatedTask -> {
                        if (task.isStopping()) {
                            LOGGER.debug("[{}] task is stopping. Marking as complete before starting native process.",
                                task.getParams().getId());
                            task.markAsCompleted();
                            return;
                        }
                        processManager.runJob(task, config, dataExtractorFactory);
                    },
                    error -> {
                        Throwable cause = ExceptionsHelper.unwrapCause(error);
                        if (cause instanceof ResourceNotFoundException) {
                            // Task has stopped
                        } else {
                            task.setFailed(error);
                        }
                    }
                ));
            },
            task::setFailed
        );

        ActionListener<RefreshResponse> refreshListener = ActionListener.wrap(
            refreshResponse -> {
                // Now we can ensure reindexing progress is complete
                task.setReindexingFinished();

                // TODO This could fail with errors. In that case we get stuck with the copied index.
                // We could delete the index in case of failure or we could try building the factory before reindexing
                // to catch the error early on.
                DataFrameDataExtractorFactory.createForDestinationIndex(parentTaskClient, config, dataExtractorFactoryListener);
            },
            dataExtractorFactoryListener::onFailure
        );

        // First we need to refresh the dest index to ensure data is searchable in case the job
        // was stopped after reindexing was complete but before the index was refreshed.
        executeWithHeadersAsync(config.getHeaders(), ML_ORIGIN, parentTaskClient, RefreshAction.INSTANCE,
            new RefreshRequest(config.getDest().getIndex()), refreshListener);
    }

    public void stop(DataFrameAnalyticsTask task) {
        processManager.stop(task);
    }
}
