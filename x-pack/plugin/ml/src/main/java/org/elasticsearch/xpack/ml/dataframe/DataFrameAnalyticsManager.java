/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
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
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.action.TransportStartDataFrameAnalyticsAction.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;

import java.time.Clock;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class DataFrameAnalyticsManager {

    private final ClusterService clusterService;
    /**
     * We need a {@link NodeClient} to be get the reindexing task and be able to report progress
     */
    private final NodeClient client;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final AnalyticsProcessManager processManager;

    public DataFrameAnalyticsManager(ClusterService clusterService, NodeClient client, DataFrameAnalyticsConfigProvider configProvider,
                                     AnalyticsProcessManager processManager) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
        this.configProvider = Objects.requireNonNull(configProvider);
        this.processManager = Objects.requireNonNull(processManager);
    }

    public void execute(DataFrameAnalyticsTask task, DataFrameAnalyticsState currentState) {
        ActionListener<DataFrameAnalyticsConfig> reindexingStateListener = ActionListener.wrap(
            config -> reindexDataframeAndStartAnalysis(task, config),
            task::markAsFailed
        );

        // With config in hand, determine action to take
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(
            config -> {
                DataFrameAnalyticsTaskState reindexingState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.REINDEXING,
                    task.getAllocationId());
                switch(currentState) {
                    // If we are STARTED, we are right at the beginning of our task, we should indicate that we are entering the
                    // REINDEX state and start reindexing.
                    case STARTED:
                        task.updatePersistentTaskState(reindexingState, ActionListener.wrap(
                            updatedTask -> reindexingStateListener.onResponse(config),
                            reindexingStateListener::onFailure));
                        break;
                    // The task has fully reindexed the documents and we should continue on with our analyses
                    case ANALYZING:
                        // TODO apply previously stored model state if applicable
                        startAnalytics(task, config, true);
                        break;
                    // If we are already at REINDEXING, we are not 100% sure if we reindexed ALL the docs.
                    // We will delete the destination index, recreate, reindex
                    case REINDEXING:
                        ClientHelper.executeAsyncWithOrigin(client,
                            ML_ORIGIN,
                            DeleteIndexAction.INSTANCE,
                            new DeleteIndexRequest(config.getDest().getIndex()),
                            ActionListener.wrap(
                                r-> reindexingStateListener.onResponse(config),
                                e -> {
                                    if (e instanceof IndexNotFoundException) {
                                        reindexingStateListener.onResponse(config);
                                    } else {
                                        reindexingStateListener.onFailure(e);
                                    }
                                }
                            ));
                        break;
                    default:
                        reindexingStateListener.onFailure(
                            ExceptionsHelper.conflictStatusException(
                                "Cannot execute analytics task [{}] as it is currently in state [{}]. " +
                                "Must be one of [STARTED, REINDEXING, ANALYZING]", config.getId(), currentState));
                }

            },
            reindexingStateListener::onFailure
        );

        // Retrieve configuration
        configProvider.get(task.getParams().getId(), configListener);
    }

    private void reindexDataframeAndStartAnalysis(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        if (task.isStopping()) {
            // The task was requested to stop before we started reindexing
            task.markAsCompleted();
            return;
        }

        // Reindexing is complete; start analytics
        ActionListener<RefreshResponse> refreshListener = ActionListener.wrap(
            refreshResponse -> {
                task.setReindexingTaskId(null);
                startAnalytics(task, config, false);
            },
            task::markAsFailed
        );

        // Refresh to ensure copied index is fully searchable
        ActionListener<BulkByScrollResponse> reindexCompletedListener = ActionListener.wrap(
            bulkResponse ->
                ClientHelper.executeAsyncWithOrigin(client,
                    ClientHelper.ML_ORIGIN,
                    RefreshAction.INSTANCE,
                    new RefreshRequest(config.getDest().getIndex()),
                    refreshListener),
            task::markAsFailed
        );

        // Reindex
        ActionListener<CreateIndexResponse> copyIndexCreatedListener = ActionListener.wrap(
            createIndexResponse -> {
                ReindexRequest reindexRequest = new ReindexRequest();
                reindexRequest.setSourceIndices(config.getSource().getIndex());
                reindexRequest.setSourceQuery(config.getSource().getParsedQuery());
                reindexRequest.setDestIndex(config.getDest().getIndex());
                reindexRequest.setScript(new Script("ctx._source." + DataFrameAnalyticsFields.ID + " = ctx._id"));

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

        DataFrameAnalyticsIndex.createDestinationIndex(client, Clock.systemUTC(), clusterService.state(), config, copyIndexCreatedListener);
    }

    private void startAnalytics(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config, boolean isTaskRestarting) {
        // Update state to ANALYZING and start process
        ActionListener<DataFrameDataExtractorFactory> dataExtractorFactoryListener = ActionListener.wrap(
            dataExtractorFactory -> {
                DataFrameAnalyticsTaskState analyzingState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.ANALYZING,
                    task.getAllocationId());
                task.updatePersistentTaskState(analyzingState, ActionListener.wrap(
                    updatedTask -> processManager.runJob(task, config, dataExtractorFactory,
                        error -> {
                            if (error != null) {
                                task.markAsFailed(error);
                            } else {
                                task.markAsCompleted();
                            }
                        }),
                    task::markAsFailed
                ));
            },
            task::markAsFailed
        );

        // TODO This could fail with errors. In that case we get stuck with the copied index.
        // We could delete the index in case of failure or we could try building the factory before reindexing
        // to catch the error early on.
        DataFrameDataExtractorFactory.create(client, config, isTaskRestarting, dataExtractorFactoryListener);
    }

    public void stop(DataFrameAnalyticsTask task) {
        processManager.stop(task);
    }
}
