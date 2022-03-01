/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.extractor.ExtractedFieldsDetector;
import org.elasticsearch.xpack.ml.dataframe.extractor.ExtractedFieldsDetectorFactory;
import org.elasticsearch.xpack.ml.dataframe.inference.InferenceRunner;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;
import org.elasticsearch.xpack.ml.dataframe.steps.AnalysisStep;
import org.elasticsearch.xpack.ml.dataframe.steps.DataFrameAnalyticsStep;
import org.elasticsearch.xpack.ml.dataframe.steps.FinalStep;
import org.elasticsearch.xpack.ml.dataframe.steps.InferenceStep;
import org.elasticsearch.xpack.ml.dataframe.steps.ReindexingStep;
import org.elasticsearch.xpack.ml.dataframe.steps.StepResponse;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class DataFrameAnalyticsManager {

    private static final Logger LOGGER = LogManager.getLogger(DataFrameAnalyticsManager.class);

    private final Settings settings;
    /**
     * We need a {@link NodeClient} to get the reindexing task and be able to report progress
     */
    private final NodeClient client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final AnalyticsProcessManager processManager;
    private final DataFrameAnalyticsAuditor auditor;
    private final IndexNameExpressionResolver expressionResolver;
    private final ResultsPersisterService resultsPersisterService;
    private final ModelLoadingService modelLoadingService;
    /** Indicates whether the node is shutting down. */
    private final AtomicBoolean nodeShuttingDown = new AtomicBoolean();

    public DataFrameAnalyticsManager(
        Settings settings,
        NodeClient client,
        ThreadPool threadPool,
        ClusterService clusterService,
        DataFrameAnalyticsConfigProvider configProvider,
        AnalyticsProcessManager processManager,
        DataFrameAnalyticsAuditor auditor,
        IndexNameExpressionResolver expressionResolver,
        ResultsPersisterService resultsPersisterService,
        ModelLoadingService modelLoadingService
    ) {
        this.settings = Objects.requireNonNull(settings);
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.configProvider = Objects.requireNonNull(configProvider);
        this.processManager = Objects.requireNonNull(processManager);
        this.auditor = Objects.requireNonNull(auditor);
        this.expressionResolver = Objects.requireNonNull(expressionResolver);
        this.resultsPersisterService = Objects.requireNonNull(resultsPersisterService);
        this.modelLoadingService = Objects.requireNonNull(modelLoadingService);
    }

    public void execute(DataFrameAnalyticsTask task, ClusterState clusterState, TimeValue masterNodeTimeout) {
        // With config in hand, determine action to take
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(config -> {
            // Check if existing destination index is incompatible.
            // If it is, we delete it and start from reindexing.
            IndexMetadata destIndex = clusterState.getMetadata().index(config.getDest().getIndex());
            if (destIndex != null) {
                MappingMetadata destIndexMapping = clusterState.getMetadata().index(config.getDest().getIndex()).mapping();
                DestinationIndex.Metadata metadata = DestinationIndex.readMetadata(config.getId(), destIndexMapping);
                if (metadata.hasMetadata() && (metadata.isCompatible() == false)) {
                    LOGGER.info(
                        "[{}] Destination index was created in version [{}] but minimum supported version is [{}]. "
                            + "Deleting index and starting from scratch.",
                        config.getId(),
                        metadata.getVersion(),
                        DestinationIndex.MIN_COMPATIBLE_VERSION
                    );
                    task.getStatsHolder()
                        .resetProgressTracker(config.getAnalysis().getProgressPhases(), config.getAnalysis().supportsInference());
                    executeJobInMiddleOfReindexing(task, config);
                    return;
                }
            }

            task.getStatsHolder().adjustProgressTracker(config.getAnalysis().getProgressPhases(), config.getAnalysis().supportsInference());

            determineProgressAndResume(task, config);

        }, task::setFailed);

        // Retrieve configuration
        ActionListener<Boolean> statsIndexListener = ActionListener.wrap(
            aBoolean -> configProvider.get(task.getParams().getId(), configListener),
            configListener::onFailure
        );

        // Make sure the stats index and alias exist
        ActionListener<Boolean> stateAliasListener = ActionListener.wrap(
            aBoolean -> createStatsIndexAndUpdateMappingsIfNecessary(
                new ParentTaskAssigningClient(client, task.getParentTaskId()),
                clusterState,
                masterNodeTimeout,
                statsIndexListener
            ),
            configListener::onFailure
        );

        // Make sure the state index and alias exist
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessaryAndWaitForYellow(
            new ParentTaskAssigningClient(client, task.getParentTaskId()),
            clusterState,
            expressionResolver,
            masterNodeTimeout,
            stateAliasListener
        );
    }

    private void createStatsIndexAndUpdateMappingsIfNecessary(
        Client clientToUse,
        ClusterState clusterState,
        TimeValue masterNodeTimeout,
        ActionListener<Boolean> listener
    ) {
        ActionListener<Boolean> createIndexListener = ActionListener.wrap(
            aBoolean -> ElasticsearchMappings.addDocMappingIfMissing(
                MlStatsIndex.writeAlias(),
                MlStatsIndex::wrappedMapping,
                clientToUse,
                clusterState,
                masterNodeTimeout,
                listener
            ),
            listener::onFailure
        );

        MlStatsIndex.createStatsIndexAndAliasIfNecessary(
            clientToUse,
            clusterState,
            expressionResolver,
            masterNodeTimeout,
            createIndexListener
        );
    }

    private void determineProgressAndResume(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        DataFrameAnalyticsTask.StartingState startingState = task.determineStartingState();

        LOGGER.debug(() -> new ParameterizedMessage("[{}] Starting job from state [{}]", config.getId(), startingState));
        switch (startingState) {
            case FIRST_TIME -> executeStep(task, config, new ReindexingStep(clusterService, client, task, auditor, config));
            case RESUMING_REINDEXING -> executeJobInMiddleOfReindexing(task, config);
            case RESUMING_ANALYZING -> executeStep(task, config, new AnalysisStep(client, task, auditor, config, processManager));
            case RESUMING_INFERENCE -> buildInferenceStep(
                task,
                config,
                ActionListener.wrap(inferenceStep -> executeStep(task, config, inferenceStep), task::setFailed)
            );
            case FINISHED -> task.setFailed(ExceptionsHelper.serverError("Unexpected starting state [" + startingState + "]"));
        }
    }

    private void executeStep(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config, DataFrameAnalyticsStep step) {
        task.setStep(step);

        ActionListener<StepResponse> stepListener = ActionListener.wrap(stepResponse -> {
            if (stepResponse.isTaskComplete()) {
                // We always want to perform the final step as it tidies things up
                executeStep(task, config, new FinalStep(client, task, auditor, config));
                return;
            }
            switch (step.name()) {
                case REINDEXING -> executeStep(task, config, new AnalysisStep(client, task, auditor, config, processManager));
                case ANALYSIS -> buildInferenceStep(
                    task,
                    config,
                    ActionListener.wrap(inferenceStep -> executeStep(task, config, inferenceStep), task::setFailed)
                );
                case INFERENCE -> executeStep(task, config, new FinalStep(client, task, auditor, config));
                case FINAL -> {
                    LOGGER.info("[{}] Marking task completed", config.getId());
                    task.markAsCompleted();
                }
                default -> task.markAsFailed(ExceptionsHelper.serverError("Unknown step [{}]", step));
            }
        }, task::setFailed);

        step.execute(stepListener);
    }

    private void executeJobInMiddleOfReindexing(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        if (task.isStopping()) {
            LOGGER.debug("[{}] task is stopping. Marking as complete before restarting reindexing.", task.getParams().getId());
            task.markAsCompleted();
            return;
        }
        ClientHelper.executeAsyncWithOrigin(
            new ParentTaskAssigningClient(client, task.getParentTaskId()),
            ML_ORIGIN,
            DeleteIndexAction.INSTANCE,
            new DeleteIndexRequest(config.getDest().getIndex()),
            ActionListener.wrap(r -> executeStep(task, config, new ReindexingStep(clusterService, client, task, auditor, config)), e -> {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof IndexNotFoundException) {
                    executeStep(task, config, new ReindexingStep(clusterService, client, task, auditor, config));
                } else {
                    task.setFailed(e);
                }
            })
        );
    }

    private void buildInferenceStep(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config, ActionListener<InferenceStep> listener) {
        ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, task.getParentTaskId());

        ActionListener<ExtractedFieldsDetector> extractedFieldsDetectorListener = ActionListener.wrap(extractedFieldsDetector -> {
            ExtractedFields extractedFields = extractedFieldsDetector.detect().v1();
            InferenceRunner inferenceRunner = new InferenceRunner(
                settings,
                parentTaskClient,
                modelLoadingService,
                resultsPersisterService,
                task.getParentTaskId(),
                config,
                extractedFields,
                task.getStatsHolder().getProgressTracker(),
                task.getStatsHolder().getDataCountsTracker()
            );
            InferenceStep inferenceStep = new InferenceStep(client, task, auditor, config, threadPool, inferenceRunner);
            listener.onResponse(inferenceStep);
        }, listener::onFailure);

        new ExtractedFieldsDetectorFactory(parentTaskClient).createFromDest(config, extractedFieldsDetectorListener);
    }

    public boolean isNodeShuttingDown() {
        return nodeShuttingDown.get();
    }

    public void markNodeAsShuttingDown() {
        nodeShuttingDown.set(true);
    }
}
