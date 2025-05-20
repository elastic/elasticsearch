/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;
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
    private final String[] destIndexAllowedSettings;
    /** Indicates whether the node is shutting down. */
    private final AtomicBoolean nodeShuttingDown = new AtomicBoolean();

    private final Map<String, ByteSizeValue> memoryLimitById;

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
        ModelLoadingService modelLoadingService,
        String[] destIndexAllowedSettings
    ) {
        this(
            settings,
            client,
            threadPool,
            clusterService,
            configProvider,
            processManager,
            auditor,
            expressionResolver,
            resultsPersisterService,
            modelLoadingService,
            destIndexAllowedSettings,
            new ConcurrentHashMap<>()
        );
    }

    // For testing only
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
        ModelLoadingService modelLoadingService,
        String[] destIndexAllowedSettings,
        Map<String, ByteSizeValue> memoryLimitById
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
        this.destIndexAllowedSettings = Objects.requireNonNull(destIndexAllowedSettings);
        this.memoryLimitById = Objects.requireNonNull(memoryLimitById);
    }

    public void execute(DataFrameAnalyticsTask task, ClusterState clusterState, TimeValue masterNodeTimeout) {
        // With config in hand, determine action to take
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(config -> {
            memoryLimitById.put(config.getId(), config.getModelMemoryLimit());
            // Check if existing destination index is incompatible.
            // If it is, we delete it and start from reindexing.
            IndexMetadata destIndex = clusterState.getMetadata().getProject().index(config.getDest().getIndex());
            if (destIndex != null) {
                MappingMetadata destIndexMapping = clusterState.getMetadata().getProject().index(config.getDest().getIndex()).mapping();
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

        // Make sure the state index and alias exist
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessaryAndWaitForYellow(
            new ParentTaskAssigningClient(client, task.getParentTaskId()),
            clusterState,
            expressionResolver,
            masterNodeTimeout,
            configListener.delegateFailureAndWrap(
                (delegate, aBoolean) -> createStatsIndexAndUpdateMappingsIfNecessary(
                    new ParentTaskAssigningClient(client, task.getParentTaskId()),
                    clusterState,
                    masterNodeTimeout,
                    // Retrieve configuration
                    delegate.delegateFailureAndWrap((l, ignored) -> configProvider.get(task.getParams().getId(), l))
                )
            )
        );
    }

    private void createStatsIndexAndUpdateMappingsIfNecessary(
        Client clientToUse,
        ClusterState clusterState,
        TimeValue masterNodeTimeout,
        ActionListener<Boolean> listener
    ) {
        MlStatsIndex.createStatsIndexAndAliasIfNecessary(
            clientToUse,
            clusterState,
            expressionResolver,
            masterNodeTimeout,
            listener.delegateFailureAndWrap(
                (l, aBoolean) -> ElasticsearchMappings.addDocMappingIfMissing(
                    MlStatsIndex.writeAlias(),
                    MlStatsIndex::wrappedMapping,
                    clientToUse,
                    clusterState,
                    masterNodeTimeout,
                    l,
                    MlStatsIndex.STATS_INDEX_MAPPINGS_VERSION
                )
            )
        );
    }

    private void determineProgressAndResume(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config) {
        DataFrameAnalyticsTask.StartingState startingState = task.determineStartingState();

        LOGGER.debug(() -> format("[%s] Starting job from state [%s]", config.getId(), startingState));
        switch (startingState) {
            case FIRST_TIME -> executeStep(
                task,
                config,
                new ReindexingStep(clusterService, client, task, auditor, config, destIndexAllowedSettings)
            );
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
                    memoryLimitById.remove(config.getId());
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
            TransportDeleteIndexAction.TYPE,
            new DeleteIndexRequest(config.getDest().getIndex()),
            ActionListener.wrap(
                r -> executeStep(task, config, new ReindexingStep(clusterService, client, task, auditor, config, destIndexAllowedSettings)),
                e -> {
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof IndexNotFoundException) {
                        executeStep(
                            task,
                            config,
                            new ReindexingStep(clusterService, client, task, auditor, config, destIndexAllowedSettings)
                        );
                    } else {
                        task.setFailed(e);
                    }
                }
            )
        );
    }

    private void buildInferenceStep(DataFrameAnalyticsTask task, DataFrameAnalyticsConfig config, ActionListener<InferenceStep> listener) {
        ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, task.getParentTaskId());
        new ExtractedFieldsDetectorFactory(parentTaskClient).createFromDest(
            config,
            listener.delegateFailureAndWrap((delegate, extractedFieldsDetector) -> {
                ExtractedFields extractedFields = extractedFieldsDetector.detect().v1();
                InferenceRunner inferenceRunner = InferenceRunner.create(
                    settings,
                    parentTaskClient,
                    modelLoadingService,
                    resultsPersisterService,
                    task.getParentTaskId(),
                    config,
                    extractedFields,
                    task.getStatsHolder().getProgressTracker(),
                    task.getStatsHolder().getDataCountsTracker(),
                    threadPool
                );
                InferenceStep inferenceStep = new InferenceStep(client, task, auditor, config, threadPool, inferenceRunner);
                delegate.onResponse(inferenceStep);
            })
        );
    }

    public boolean isNodeShuttingDown() {
        return nodeShuttingDown.get();
    }

    public void markNodeAsShuttingDown() {
        nodeShuttingDown.set(true);
    }

    /**
     * Get the memory limit for a data frame analytics job if known.
     * The memory limit will only be known if it is running on the
     * current node, or has been very recently.
     * @param id Data frame analytics job ID.
     * @return The {@link ByteSizeValue} representing the memory limit, if known, otherwise {@link Optional#empty}.
     */
    public Optional<ByteSizeValue> getMemoryLimitIfKnown(String id) {
        return Optional.ofNullable(memoryLimitById.get(id));
    }

    /**
     * Finds the memory used by data frame analytics jobs that are active on the current node.
     * This includes jobs that are in the reindexing state, even though they don't have a running
     * process, because we want to ensure that when they get as far as needing to run a process
     * there'll be space for it.
     * @param tasks Persistent tasks metadata.
     * @return Memory used by data frame analytics jobs that are active on the current node.
     */
    public ByteSizeValue getActiveTaskMemoryUsage(PersistentTasksCustomMetadata tasks) {
        long memoryUsedBytes = 0;
        for (Map.Entry<String, ByteSizeValue> entry : memoryLimitById.entrySet()) {
            DataFrameAnalyticsState state = MlTasks.getDataFrameAnalyticsState(entry.getKey(), tasks);
            if (state.consumesMemory()) {
                memoryUsedBytes += entry.getValue().getBytes() + DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes();
            }
        }
        return ByteSizeValue.ofBytes(memoryUsedBytes);
    }
}
