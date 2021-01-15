/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;
import org.elasticsearch.xpack.ml.dataframe.steps.AnalysisStep;
import org.elasticsearch.xpack.ml.dataframe.steps.DataFrameAnalyticsStep;
import org.elasticsearch.xpack.ml.dataframe.steps.ReindexingStep;
import org.elasticsearch.xpack.ml.dataframe.steps.StepResponse;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class DataFrameAnalyticsManager {

    private static final Logger LOGGER = LogManager.getLogger(DataFrameAnalyticsManager.class);

    /**
     * We need a {@link NodeClient} to get the reindexing task and be able to report progress
     */
    private final NodeClient client;
    private final ClusterService clusterService;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final AnalyticsProcessManager processManager;
    private final DataFrameAnalyticsAuditor auditor;
    private final IndexNameExpressionResolver expressionResolver;
    /** Indicates whether the node is shutting down. */
    private final AtomicBoolean nodeShuttingDown = new AtomicBoolean();

    public DataFrameAnalyticsManager(NodeClient client, ClusterService clusterService, DataFrameAnalyticsConfigProvider configProvider,
                                     AnalyticsProcessManager processManager, DataFrameAnalyticsAuditor auditor,
                                     IndexNameExpressionResolver expressionResolver) {
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.configProvider = Objects.requireNonNull(configProvider);
        this.processManager = Objects.requireNonNull(processManager);
        this.auditor = Objects.requireNonNull(auditor);
        this.expressionResolver = Objects.requireNonNull(expressionResolver);
    }

    public void execute(DataFrameAnalyticsTask task, ClusterState clusterState) {
        // With config in hand, determine action to take
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(
            config -> {
                // Check if existing destination index is incompatible.
                // If it is, we delete it and start from reindexing.
                IndexMetadata destIndex = clusterState.getMetadata().index(config.getDest().getIndex());
                if (destIndex != null) {
                    MappingMetadata destIndexMapping = clusterState.getMetadata().index(config.getDest().getIndex()).mapping();
                    DestinationIndex.Metadata metadata = DestinationIndex.readMetadata(config.getId(), destIndexMapping);
                    if (metadata.hasMetadata() && (metadata.isCompatible() == false)) {
                        LOGGER.info("[{}] Destination index was created in version [{}] but minimum supported version is [{}]. " +
                            "Deleting index and starting from scratch.", config.getId(), metadata.getVersion(),
                            DestinationIndex.MIN_COMPATIBLE_VERSION);
                        task.getStatsHolder().resetProgressTracker(config.getAnalysis().getProgressPhases(),
                            config.getAnalysis().supportsInference());
                        executeJobInMiddleOfReindexing(task, config);
                        return;
                    }
                }

                task.getStatsHolder().adjustProgressTracker(config.getAnalysis().getProgressPhases(),
                    config.getAnalysis().supportsInference());

                determineProgressAndResume(task, config);

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

    private void determineProgressAndResume(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        DataFrameAnalyticsTask.StartingState startingState = task.determineStartingState();

        LOGGER.debug(() -> new ParameterizedMessage("[{}] Starting job from state [{}]", config.getId(), startingState));
        switch (startingState) {
            case FIRST_TIME:
                executeStep(task, config, new ReindexingStep(clusterService, client, task, auditor, config));
                break;
            case RESUMING_REINDEXING:
                executeJobInMiddleOfReindexing(task, config);
                break;
            case RESUMING_ANALYZING:
                executeStep(task, config, new AnalysisStep(client, task, auditor, config, processManager));
                break;
            case FINISHED:
            default:
                task.setFailed(ExceptionsHelper.serverError("Unexpected starting state [" + startingState + "]"));
        }
    }

    private void executeStep(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config, DataFrameAnalyticsStep step) {
        task.setStep(step);

        ActionListener<StepResponse> stepListener = ActionListener.wrap(
            stepResponse -> {
                if (stepResponse.isTaskComplete()) {
                    LOGGER.info("[{}] Marking task completed", config.getId());
                    task.markAsCompleted();
                    return;
                }
                switch (step.name()) {
                    case REINDEXING:
                        executeStep(task, config, new AnalysisStep(client, task, auditor, config, processManager));
                        break;
                    case ANALYSIS:
                        // This is the last step
                        LOGGER.info("[{}] Marking task completed", config.getId());
                        task.markAsCompleted();
                        break;
                    default:
                        task.markAsFailed(ExceptionsHelper.serverError("Unknown step [{}]", step));
                }
            },
            task::setFailed
        );

        step.execute(stepListener);
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
                r-> executeStep(task, config, new ReindexingStep(clusterService, client, task, auditor, config)),
                e -> {
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof IndexNotFoundException) {
                        executeStep(task, config, new ReindexingStep(clusterService, client, task, auditor, config));
                    } else {
                        task.setFailed(e);
                    }
                }
            ));
    }

    public boolean isNodeShuttingDown() {
        return nodeShuttingDown.get();
    }

    public void markNodeAsShuttingDown() {
        nodeShuttingDown.set(true);
    }
}
