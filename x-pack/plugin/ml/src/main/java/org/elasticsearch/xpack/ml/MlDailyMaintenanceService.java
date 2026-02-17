/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.ResetJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * A service that runs once a day and triggers maintenance tasks.
 */
public class MlDailyMaintenanceService implements Releasable {

    private static final Logger logger = LogManager.getLogger(MlDailyMaintenanceService.class);

    // The maximum value of a random offset used to calculate the time the nightly maintenance tasks are triggered on a given cluster.
    // This is added in order to avoid multiple clusters running the maintenance tasks at the same time.
    private static final int MAX_TIME_OFFSET_MINUTES = 120;

    private final ThreadPool threadPool;
    private final Client client;
    private final ClusterService clusterService;
    private final MlAssignmentNotifier mlAssignmentNotifier;

    /**
     * An interface to abstract the calculation of the delay to the next execution.
     * Needed to enable testing.
     */
    private final Supplier<TimeValue> schedulerProvider;

    private final IndexNameExpressionResolver expressionResolver;

    private final boolean isAnomalyDetectionEnabled;
    private final boolean isDataFrameAnalyticsEnabled;
    private final boolean isNlpEnabled;
    private final boolean isIlmEnabled;

    private volatile Scheduler.Cancellable cancellable;
    private volatile float deleteExpiredDataRequestsPerSecond;
    private volatile ByteSizeValue rolloverMaxSize;

    MlDailyMaintenanceService(
        Settings settings,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService,
        MlAssignmentNotifier mlAssignmentNotifier,
        Supplier<TimeValue> schedulerProvider,
        IndexNameExpressionResolver expressionResolver,
        boolean isAnomalyDetectionEnabled,
        boolean isDataFrameAnalyticsEnabled,
        boolean isNlpEnabled,
        boolean isIlmEnabled
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.mlAssignmentNotifier = Objects.requireNonNull(mlAssignmentNotifier);
        this.schedulerProvider = Objects.requireNonNull(schedulerProvider);
        this.expressionResolver = Objects.requireNonNull(expressionResolver);
        this.deleteExpiredDataRequestsPerSecond = MachineLearning.NIGHTLY_MAINTENANCE_REQUESTS_PER_SECOND.get(settings);
        this.rolloverMaxSize = MachineLearning.RESULTS_INDEX_ROLLOVER_MAX_SIZE.get(settings);
        this.isAnomalyDetectionEnabled = isAnomalyDetectionEnabled;
        this.isDataFrameAnalyticsEnabled = isDataFrameAnalyticsEnabled;
        this.isNlpEnabled = isNlpEnabled;
        this.isIlmEnabled = isIlmEnabled;
    }

    public MlDailyMaintenanceService(
        Settings settings,
        ClusterName clusterName,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService,
        MlAssignmentNotifier mlAssignmentNotifier,
        IndexNameExpressionResolver expressionResolver,
        boolean isAnomalyDetectionEnabled,
        boolean isDataFrameAnalyticsEnabled,
        boolean isNlpEnabled,
        boolean isIlmEnabled
    ) {
        this(
            settings,
            threadPool,
            client,
            clusterService,
            mlAssignmentNotifier,
            () -> delayToNextTime(clusterName),
            expressionResolver,
            isAnomalyDetectionEnabled,
            isDataFrameAnalyticsEnabled,
            isNlpEnabled,
            isIlmEnabled
        );
    }

    void setDeleteExpiredDataRequestsPerSecond(float value) {
        this.deleteExpiredDataRequestsPerSecond = value;
    }

    // Public for testing
    /**
     * Set the value of the rollover max size.
     *
     * @param value The value of the rollover max size.
     */
    public void setRolloverMaxSize(ByteSizeValue value) {
        this.rolloverMaxSize = value;
    }

    /**
     * Calculates the delay until the next time the maintenance should be triggered.
     * The next time is 30 minutes past midnight of the following day plus a random
     * offset. The random offset is added in order to avoid multiple clusters
     * running the maintenance tasks at the same time. A cluster with a given name
     * shall have the same offset throughout its life.
     *
     * @param clusterName the cluster name is used to seed the random offset
     * @return the delay to the next time the maintenance should be triggered
     */
    private static TimeValue delayToNextTime(ClusterName clusterName) {
        Random random = new Random(clusterName.hashCode());
        int minutesOffset = random.ints(0, MAX_TIME_OFFSET_MINUTES).findFirst().getAsInt();

        ZonedDateTime now = ZonedDateTime.now(Clock.systemDefaultZone());
        ZonedDateTime next = now.plusDays(1).toLocalDate().atStartOfDay(now.getZone()).plusMinutes(30).plusMinutes(minutesOffset);
        return TimeValue.timeValueMillis(next.toInstant().toEpochMilli() - now.toInstant().toEpochMilli());
    }

    public synchronized void start() {
        logger.debug("Starting ML daily maintenance service");
        scheduleNext();
    }

    public synchronized void stop() {
        logger.debug("Stopping ML daily maintenance service");
        if (cancellable != null && cancellable.isCancelled() == false) {
            cancellable.cancel();
        }
    }

    boolean isStarted() {
        return cancellable != null;
    }

    @Override
    public void close() {
        stop();
    }

    private synchronized void scheduleNext() {
        try {
            cancellable = threadPool.schedule(this::triggerTasks, schedulerProvider.get(), threadPool.generic());
        } catch (EsRejectedExecutionException e) {
            if (e.isExecutorShutdown()) {
                logger.debug("failed to schedule next maintenance task; shutting down", e);
            } else {
                throw e;
            }
        }
    }

    private void triggerTasks() {
        try {
            if (MlMetadata.getMlMetadata(clusterService.state()).isUpgradeMode()) {
                logger.warn("skipping scheduled [ML] maintenance tasks because upgrade mode is enabled");
                return;
            }
            if (MlMetadata.getMlMetadata(clusterService.state()).isResetMode()) {
                logger.warn("skipping scheduled [ML] maintenance tasks because machine learning feature reset is in progress");
                return;
            }
            logger.info("triggering scheduled [ML] maintenance tasks");

            if (isAnomalyDetectionEnabled) {
                triggerAnomalyDetectionMaintenance();
            }
            if (isDataFrameAnalyticsEnabled) {
                triggerDataFrameAnalyticsMaintenance();
            }
            if (isNlpEnabled) {
                triggerNlpMaintenance();
            }
            auditUnassignedMlTasks();
        } finally {
            scheduleNext();
        }
    }

    private void triggerAnomalyDetectionMaintenance() {
        // The maintenance tasks are chained, where each subsequent task is executed regardless of whether the previous one
        // succeeded or failed.

        // Final step: Log completion
        ActionListener<AcknowledgedResponse> finalListener = ActionListener.wrap(
            response -> logger.info("Completed [ML] maintenance tasks"),
            e -> logger.warn("An error occurred during [ML] maintenance tasks execution", e)
        );

        // Step 5: Roll over state indices
        Runnable rollStateIndices = () -> triggerRollStateIndicesIfNecessaryTask(finalListener);

        // Step 4: Roll over results indices
        Runnable rollResultsIndices = () -> triggerRollResultsIndicesIfNecessaryTask(
            continueOnFailureListener("roll-state-indices", rollStateIndices)
        );

        // Step 3: Delete expired data
        Runnable deleteExpiredData = () -> triggerDeleteExpiredDataTask(
            continueOnFailureListener("roll-results-indices", rollResultsIndices)
        );

        // Step 2: Reset jobs that are in resetting state without a task
        Runnable resetJobs = () -> triggerResetJobsInStateResetWithoutResetTask(
            continueOnFailureListener("delete-expired-data", deleteExpiredData)
        );

        // Step 1: Delete jobs that are in deleting state without a task
        triggerDeleteJobsInStateDeletingWithoutDeletionTask(continueOnFailureListener("reset-jobs", resetJobs));
    }

    private ActionListener<AcknowledgedResponse> continueOnFailureListener(String nextTaskName, Runnable next) {
        return ActionListener.wrap(response -> next.run(), e -> {
            logger.warn(() -> "A maintenance task failed, but maintenance will continue. Triggering next task [" + nextTaskName + "].", e);
            next.run();
        });
    }

    private void triggerDataFrameAnalyticsMaintenance() {
        // Currently a NOOP
    }

    private void triggerNlpMaintenance() {
        // Currently a NOOP
    }

    // Helper function to remove an alias from a given index.
    private void removeAlias(
        String index,
        String alias,
        IndicesAliasesRequestBuilder aliasRequestBuilder,
        ActionListener<Boolean> listener
    ) {
        logger.trace("removeRolloverAlias: index: {}, alias: {}", index, alias);
        aliasRequestBuilder.removeAlias(index, alias);
        MlIndexAndAlias.updateAliases(aliasRequestBuilder, listener);
    }

    private void rollover(Client client, String rolloverAlias, @Nullable String newIndexName, ActionListener<String> listener) {
        MlIndexAndAlias.rollover(
            client,
            new RolloverRequestBuilder(client).setRolloverTarget(rolloverAlias)
                .setNewIndexName(newIndexName)
                .setConditions(RolloverConditions.newBuilder().addMaxIndexSizeCondition(rolloverMaxSize).build())
                .request(),
            listener
        );
    }

    private void rollAndUpdateAliases(ClusterState clusterState, String index, List<String> allIndices, ActionListener<Boolean> listener) {
        OriginSettingClient originSettingClient = new OriginSettingClient(client, ML_ORIGIN);

        Tuple<String, String> newIndexNameAndRolloverAlias = MlIndexAndAlias.createRolloverAliasAndNewIndexName(index);
        String rolloverAlias = newIndexNameAndRolloverAlias.v1();
        String newIndexName = newIndexNameAndRolloverAlias.v2();

        IndicesAliasesRequestBuilder aliasRequestBuilder = MlIndexAndAlias.createIndicesAliasesRequestBuilder(client);

        // 4 Clean up any dangling aliases
        ActionListener<Boolean> aliasListener = ActionListener.wrap(listener::onResponse, e -> {
            if (e instanceof IndexNotFoundException) {
                // Removal of the rollover alias may have failed in the case of rollover not occurring, e.g. when the rollover conditions
                // were not satisfied.
                // We must still clean up the temporary alias from the original index.
                var indexName = MlIndexAndAlias.ensureValidResultsIndexName(index);
                // Make sure we use a fresh IndicesAliasesRequestBuilder, the original one may have changed internal state.
                IndicesAliasesRequestBuilder localAliasRequestBuilder = MlIndexAndAlias.createIndicesAliasesRequestBuilder(
                    originSettingClient
                );

                // Execute the cleanup, no need to propagate the original failure.
                removeAlias(indexName, rolloverAlias, localAliasRequestBuilder, listener);
            } else {
                listener.onFailure(e);
            }
        });

        // 3 Update aliases
        ActionListener<String> rolloverListener = ActionListener.wrap(newIndexNameResponse -> {
            if (MlIndexAndAlias.isAnomaliesStateIndex(index)) {
                MlIndexAndAlias.addStateIndexRolloverAliasActions(aliasRequestBuilder, newIndexNameResponse, clusterState, allIndices);
            } else {
                MlIndexAndAlias.addResultsIndexRolloverAliasActions(aliasRequestBuilder, newIndexNameResponse, clusterState, allIndices);
            }
            // On success, the rollover alias may have been moved to the new index, so we attempt to remove it from there.
            // Note that the rollover request is considered "successful" even if it didn't occur due to a condition not being met
            // (no exception will be thrown). In which case the attempt to remove the alias here will fail with an
            // IndexNotFoundException. We handle this case with a secondary listener.
            removeAlias(newIndexNameResponse, rolloverAlias, aliasRequestBuilder, aliasListener);
        }, e -> {
            // If rollover fails, we must still clean up the temporary alias from the original index.
            // The index name is either the original one provided or the original with a suffix appended.
            var targetIndexName = MlIndexAndAlias.ensureValidResultsIndexName(index);
            // Execute the cleanup, no need to propagate the original failure.
            removeAlias(targetIndexName, rolloverAlias, aliasRequestBuilder, aliasListener);
        });

        // 2 rollover the index alias to the new index name
        ActionListener<IndicesAliasesResponse> getIndicesAliasesListener = ActionListener.wrap(getIndicesAliasesResponse -> {
            rollover(originSettingClient, rolloverAlias, newIndexName, rolloverListener);
        }, rolloverListener::onFailure);

        // 1. Create necessary aliases
        MlIndexAndAlias.createAliasForRollover(originSettingClient, index, rolloverAlias, getIndicesAliasesListener);
    }

    private String[] findIndicesMatchingPattern(ClusterState clusterState, String indexPattern) {
        // list all indices matching the given index pattern
        String[] indices = expressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpenHidden(), indexPattern);
        if (logger.isTraceEnabled()) {
            logger.trace("findIndicesMatchingPattern: indices found: {} matching pattern [{}]", Arrays.toString(indices), indexPattern);
        }
        return indices;
    }

    private void rolloverIndexSafely(ClusterState clusterState, String index, List<String> allIndices, List<Exception> failures) {
        PlainActionFuture<Boolean> updated = new PlainActionFuture<>();
        rollAndUpdateAliases(clusterState, index, allIndices, updated);
        try {
            updated.actionGet();
        } catch (Exception ex) {
            String message = Strings.format("Failed to rollover ML index [%s]: %s", index, ex.getMessage());
            logger.warn(message);
            if (ex instanceof ElasticsearchException elasticsearchException) {
                failures.add(new ElasticsearchStatusException(message, elasticsearchException.status(), elasticsearchException));
            } else {
                failures.add(new ElasticsearchStatusException(message, RestStatus.REQUEST_TIMEOUT, ex));
            }
        }
    }

    private void handleRolloverResults(String[] indices, List<Exception> failures, ActionListener<AcknowledgedResponse> finalListener) {
        if (failures.isEmpty()) {
            logger.debug("ML anomalies indices [{}] rolled over and aliases updated", String.join(",", indices));
            finalListener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        logger.warn("failed to roll over ml anomalies results indices: [{}]", failures);
        finalListener.onResponse(AcknowledgedResponse.FALSE);
    }

    // Helper function to check for the "index.lifecycle.name" setting on an index
    // public for testing

    /**
     * Return {@code true} if the index has an ILM policy {@code false} otherwise.
     * @param indexName The index name to check.
     * @return {@code true} if the index has an ILM policy {@code false} otherwise.
     */
    public boolean hasIlm(String indexName) {
        // If ILM is not enabled at all in the machine learning plugin then return false.
        if (isIlmEnabled == false) {
            return false;
        }

        GetIndexRequest request = new GetIndexRequest(TimeValue.THIRTY_SECONDS);
        request.indices(indexName);
        request.includeDefaults(true); // Request index settings, mappings and aliases

        GetIndexResponse response = client.admin().indices().getIndex(request).actionGet();

        Settings settings = response.getSettings().get(indexName);

        if (settings != null) {
            String ilmPolicyName = settings.get("index.lifecycle.name");
            // If the setting is present and not empty, ILM is in force
            return ilmPolicyName != null && ilmPolicyName.isEmpty() == false;
        }

        return false;
    }

    private void triggerRollIndicesIfNecessaryTask(
        String taskName,
        String indexPattern,
        ActionListener<AcknowledgedResponse> finalListener
    ) {
        logger.info("[ML] maintenance task: [{}] for index pattern [{}]", taskName, indexPattern);

        ClusterState clusterState = clusterService.state();

        String[] indices = findIndicesMatchingPattern(clusterState, indexPattern);
        if (rolloverMaxSize == ByteSizeValue.MINUS_ONE || indices.length == 0) {
            // Early bath
            finalListener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        List<Exception> failures = new ArrayList<>();

        // Filter out any indices that have an ILM policy to avoid a potential race when rolling indices.
        Arrays.stream(indices)
            .filter(index -> hasIlm(index) == false)
            .collect(Collectors.groupingBy(MlIndexAndAlias::baseIndexName))
            .forEach((baseIndexName, indicesInGroup) -> {
                rolloverIndexSafely(
                    clusterState,
                    MlIndexAndAlias.latestIndex(indicesInGroup.toArray(new String[0])),
                    indicesInGroup,
                    failures
                );
            });

        handleRolloverResults(indices, failures, finalListener);
    }

    // public for testing
    public void triggerRollResultsIndicesIfNecessaryTask(ActionListener<AcknowledgedResponse> finalListener) {
        triggerRollIndicesIfNecessaryTask("roll-results-indices", AnomalyDetectorsIndex.jobResultsIndexPattern(), finalListener);
    }

    // public for testing
    public void triggerRollStateIndicesIfNecessaryTask(ActionListener<AcknowledgedResponse> finalListener) {
        triggerRollIndicesIfNecessaryTask("roll-state-indices", AnomalyDetectorsIndex.jobStateIndexPattern(), finalListener);
    }

    private void triggerDeleteExpiredDataTask(ActionListener<AcknowledgedResponse> finalListener) {
        ActionListener<DeleteExpiredDataAction.Response> deleteExpiredDataActionListener = finalListener.delegateFailureAndWrap(
            (l, deleteExpiredDataResponse) -> {
                if (deleteExpiredDataResponse.isDeleted()) {
                    logger.info("Successfully completed [ML] maintenance task: triggerDeleteExpiredDataTask");
                } else {
                    logger.info("Halting [ML] maintenance tasks before completion as elapsed time is too great");
                }
                l.onResponse(AcknowledgedResponse.TRUE);
            }
        );

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            DeleteExpiredDataAction.INSTANCE,
            new DeleteExpiredDataAction.Request(deleteExpiredDataRequestsPerSecond, TimeValue.timeValueHours(8)),
            deleteExpiredDataActionListener
        );
    }

    // Visible for testing
    public void triggerDeleteJobsInStateDeletingWithoutDeletionTask(ActionListener<AcknowledgedResponse> finalListener) {
        triggerJobsInStateWithoutMatchingTask(
            "triggerDeleteJobsInStateDeletingWithoutDeletionTask",
            Job::isDeleting,
            DeleteJobAction.NAME,
            taskInfo -> stripPrefixOrNull(taskInfo.description(), DeleteJobAction.DELETION_TASK_DESCRIPTION_PREFIX),
            DeleteJobAction.INSTANCE,
            DeleteJobAction.Request::new,
            finalListener
        );
    }

    public void triggerResetJobsInStateResetWithoutResetTask(ActionListener<AcknowledgedResponse> finalListener) {
        triggerJobsInStateWithoutMatchingTask(
            "triggerResetJobsInStateResetWithoutResetTask",
            Job::isResetting,
            ResetJobAction.NAME,
            taskInfo -> stripPrefixOrNull(taskInfo.description(), MlTasks.JOB_TASK_ID_PREFIX),
            ResetJobAction.INSTANCE,
            ResetJobAction.Request::new,
            finalListener
        );
    }

    /**
     * @return If the string starts with the prefix, this returns the string without the prefix.
     *         Otherwise, this return null.
     */
    private static String stripPrefixOrNull(String str, String prefix) {
        return str == null || str.startsWith(prefix) == false ? null : str.substring(prefix.length());
    }

    /**
     * Executes a request for each job in a state, while missing the corresponding task. This
     * usually indicates the node originally executing the task has died, so retry the request.
     *
     * @param maintenanceTaskName Name of ML maintenance task; used only for logging.
     * @param jobFilter           Predicate for filtering the jobs.
     * @param taskActionName      Action name of the tasks corresponding to the jobs.
     * @param jobIdExtractor      Function to extract the job ID from the task info (in order to match to the job).
     * @param actionType          Action type of the request that should be (re)executed.
     * @param requestCreator      Function to create the request from the job ID.
     * @param finalListener       Listener that captures the final response.
     */
    private void triggerJobsInStateWithoutMatchingTask(
        String maintenanceTaskName,
        Predicate<Job> jobFilter,
        String taskActionName,
        Function<TaskInfo, String> jobIdExtractor,
        ActionType<AcknowledgedResponse> actionType,
        Function<String, AcknowledgedRequest<?>> requestCreator,
        ActionListener<AcknowledgedResponse> finalListener
    ) {
        SetOnce<Set<String>> jobsInStateHolder = new SetOnce<>();

        ActionListener<List<Tuple<String, AcknowledgedResponse>>> jobsActionListener = finalListener.delegateFailureAndWrap(
            (delegate, jobsResponses) -> {
                List<String> jobIds = jobsResponses.stream().filter(t -> t.v2().isAcknowledged() == false).map(Tuple::v1).collect(toList());
                if (jobIds.isEmpty()) {
                    logger.info("Successfully completed [ML] maintenance task: {}", maintenanceTaskName);
                } else {
                    logger.info("[ML] maintenance task {} failed for jobs: {}", maintenanceTaskName, jobIds);
                }
                delegate.onResponse(AcknowledgedResponse.TRUE);
            }
        );

        ActionListener<ListTasksResponse> listTasksActionListener = ActionListener.wrap(listTasksResponse -> {
            Set<String> jobsInState = jobsInStateHolder.get();
            Set<String> jobsWithTask = listTasksResponse.getTasks().stream().map(jobIdExtractor).filter(Objects::nonNull).collect(toSet());
            Set<String> jobsInStateWithoutTask = Sets.difference(jobsInState, jobsWithTask);
            if (jobsInStateWithoutTask.isEmpty()) {
                finalListener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            TypedChainTaskExecutor<Tuple<String, AcknowledgedResponse>> chainTaskExecutor = new TypedChainTaskExecutor<>(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                Predicates.always(),
                Predicates.always()
            );
            for (String jobId : jobsInStateWithoutTask) {
                chainTaskExecutor.add(
                    listener -> executeAsyncWithOrigin(
                        client,
                        ML_ORIGIN,
                        actionType,
                        requestCreator.apply(jobId),
                        listener.delegateFailureAndWrap((l, response) -> l.onResponse(Tuple.tuple(jobId, response)))
                    )
                );
            }
            chainTaskExecutor.execute(jobsActionListener);
        }, finalListener::onFailure);

        ActionListener<GetJobsAction.Response> getJobsActionListener = ActionListener.wrap(getJobsResponse -> {
            Set<String> jobsInState = getJobsResponse.getResponse().results().stream().filter(jobFilter).map(Job::getId).collect(toSet());
            if (jobsInState.isEmpty()) {
                finalListener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            jobsInStateHolder.set(jobsInState);
            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                TransportListTasksAction.TYPE,
                new ListTasksRequest().setActions(taskActionName),
                listTasksActionListener
            );
        }, finalListener::onFailure);

        executeAsyncWithOrigin(client, ML_ORIGIN, GetJobsAction.INSTANCE, new GetJobsAction.Request("*"), getJobsActionListener);
    }

    /**
     * The idea of this is that if tasks are unassigned for days on end then they'll get a duplicate
     * audit warning every day, and that will mean they'll permanently have a yellow triangle next
     * to their entries in the UI jobs list.  (This functionality may need revisiting if the condition
     * for displaying a yellow triangle in the UI jobs list changes.)
     */
    private void auditUnassignedMlTasks() {
        ClusterState state = clusterService.state();
        final ProjectMetadata project = state.getMetadata().getProject();
        PersistentTasksCustomMetadata tasks = project.custom(PersistentTasksCustomMetadata.TYPE);
        if (tasks != null) {
            mlAssignmentNotifier.auditUnassignedMlTasks(project.id(), state.nodes(), tasks);
        }
    }
}
