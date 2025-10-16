/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
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
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * A service that runs once a day and triggers maintenance tasks.
 */
public class MlDailyMaintenanceService implements Releasable {

    private static final org.elasticsearch.logging.Logger logger = LogManager.getLogger(MlDailyMaintenanceService.class);

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

    private RolloverConditions rolloverConditions;

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
        boolean isNlpEnabled
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.mlAssignmentNotifier = Objects.requireNonNull(mlAssignmentNotifier);
        this.schedulerProvider = Objects.requireNonNull(schedulerProvider);
        this.expressionResolver = Objects.requireNonNull(expressionResolver);
        this.deleteExpiredDataRequestsPerSecond = MachineLearning.NIGHTLY_MAINTENANCE_REQUESTS_PER_SECOND.get(settings);
        this.rolloverMaxSize = MachineLearning.NIGHTLY_MAINTENANCE_ROLLOVER_MAX_SIZE.get(settings);
        this.isAnomalyDetectionEnabled = isAnomalyDetectionEnabled;
        this.isDataFrameAnalyticsEnabled = isDataFrameAnalyticsEnabled;
        this.isNlpEnabled = isNlpEnabled;

        this.rolloverConditions = RolloverConditions.newBuilder().addMaxIndexSizeCondition(rolloverMaxSize).build();
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
        boolean isNlpEnabled
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
            isNlpEnabled
        );
    }

    void setDeleteExpiredDataRequestsPerSecond(float value) {
        this.deleteExpiredDataRequestsPerSecond = value;
    }

    void setRolloverMaxSize(ByteSizeValue value) {
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
        logger.info("Starting ML daily maintenance service");
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
        // Step 5: Log any error that could have happened
        ActionListener<AcknowledgedResponse> finalListener = ActionListener.wrap(response -> {
            if (response.isAcknowledged() == false) {
                logger.warn("[ML] maintenance task: triggerRollResultsIndicesIfNecessaryTask failed");
            } else {
                logger.info("[ML] maintenance task: triggerRollResultsIndicesIfNecessaryTask succeeded");
            }
        }, e -> logger.warn("An error occurred during [ML] maintenance tasks execution ", e));

        // Step 4: Roll over results indices if necessary
        ActionListener<AcknowledgedResponse> rollResultsIndicesIfNecessaryListener = ActionListener.wrap(unused -> {
            triggerRollResultsIndicesIfNecessaryTask(finalListener);
        }, e -> {
            // Note: Steps 1-4 are independent, so continue upon errors.
            triggerRollResultsIndicesIfNecessaryTask(finalListener);
        });

        // Step 3: Delete expired data
        ActionListener<AcknowledgedResponse> deleteJobsListener = ActionListener.wrap(unused -> {
            triggerDeleteExpiredDataTask(rollResultsIndicesIfNecessaryListener);
        }, e -> {
            // Note: Steps 1-4 are independent, so continue upon errors.
            triggerDeleteExpiredDataTask(rollResultsIndicesIfNecessaryListener);
        });

        // Step 2: Reset jobs that are in resetting state without task
        ActionListener<AcknowledgedResponse> resetJobsListener = ActionListener.wrap(unused -> {
            triggerResetJobsInStateResetWithoutResetTask(deleteJobsListener);
        }, e -> {
            // Note: Steps 1-4 are independent, so continue upon errors.
            triggerResetJobsInStateResetWithoutResetTask(deleteJobsListener);
        });

        // Step 1: Delete jobs that are in deleting state without task
        triggerDeleteJobsInStateDeletingWithoutDeletionTask(resetJobsListener);
    }

    private void triggerDataFrameAnalyticsMaintenance() {
        // Currently a NOOP
    }

    private void triggerNlpMaintenance() {
        // Currently a NOOP
    }

    void removeRolloverAlias(
        String index,
        String alias,
        IndicesAliasesRequestBuilder aliasRequestBuilder,
        ActionListener<Boolean> listener
    ) {
        aliasRequestBuilder.removeAlias(index, alias);
        MlIndexAndAlias.updateAliases(aliasRequestBuilder, listener);
    }

    private void rollAndUpdateAliases(ClusterState clusterState, String index, ActionListener<Boolean> listener) {
        // Create an alias specifically for rolling over.
        // The ml-anomalies index has aliases for each job, any
        // of which could be used but that means one alias is
        // treated differently.
        // Using a `.` in the alias name avoids any conflicts
        // as AD job Ids cannot start with `.`
        String rolloverAlias = index + ".rollover_alias";

        OriginSettingClient originSettingClient = new OriginSettingClient(client, ML_ORIGIN);

        // If the index does not end in a digit then rollover does not know
        // what to name the new index so it must be specified in the request.
        // Otherwise leave null and rollover will calculate the new name
        String newIndexName = MlIndexAndAlias.has6DigitSuffix(index) ? null : index + MlIndexAndAlias.FIRST_INDEX_SIX_DIGIT_SUFFIX;
        IndicesAliasesRequestBuilder aliasRequestBuilder = originSettingClient.admin()
            .indices()
            .prepareAliases(
                MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT
            );

        // 4 Clean up any dangling aliases
        ActionListener<Boolean> aliasListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
            if (e instanceof IndexNotFoundException) {
                // Removal of the rollover alias may have failed in the case of rollover not occurring, e.g. when the rollover conditions
                // were not satisfied.
                // We must still clean up the temporary alias from the original index.
                // The index name is either the original one provided or the original with a suffix appended.
                var indexName = MlIndexAndAlias.has6DigitSuffix(index) ? index : index + MlIndexAndAlias.FIRST_INDEX_SIX_DIGIT_SUFFIX;

                // Make sure we use a fresh IndicesAliasesRequestBuilder, the original one may have changed internal state.
                IndicesAliasesRequestBuilder localAliasRequestBuilder = originSettingClient.admin()
                    .indices()
                    .prepareAliases(
                        MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                        MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT
                    );

                // Execute the cleanup, no need to propagate the original failure.
                removeRolloverAlias(indexName, rolloverAlias, localAliasRequestBuilder, listener);
            } else {
                listener.onFailure(e);
            }
        });

        // 3 Update aliases
        ActionListener<String> rolloverListener = ActionListener.wrap(newIndexNameResponse -> {
            MlIndexAndAlias.addIndexAliasesRequests(aliasRequestBuilder, index, newIndexNameResponse, clusterState);
            // On success, the rollover alias may have been moved to the new index, so we attempt to remove it from there.
            // Note that the rollover request is considered "successful" even if it didn't occur due to a condition not being met
            // (no exception will be thrown). In which case the attempt to remove the alias here will fail with an
            // IndexNotFoundException. We handle this case with a secondary listener.
            removeRolloverAlias(newIndexNameResponse, rolloverAlias, aliasRequestBuilder, aliasListener);
        }, e -> {
            // If rollover fails, we must still clean up the temporary alias from the original index.
            // The index name is either the original one provided or the original with a suffix appended.
            var indexName = MlIndexAndAlias.has6DigitSuffix(index) ? index : index + MlIndexAndAlias.FIRST_INDEX_SIX_DIGIT_SUFFIX;
            // Execute the cleanup, no need to propagate the original failure.
            removeRolloverAlias(indexName, rolloverAlias, aliasRequestBuilder, aliasListener);
        });

        // 2 rollover the index alias to the new index name
        ActionListener<IndicesAliasesResponse> getIndicesAliasesListener = ActionListener.wrap(getIndicesAliasesResponse -> {
            MlIndexAndAlias.rollover(
                originSettingClient,
                new RolloverRequestBuilder(originSettingClient).setRolloverTarget(rolloverAlias)
                    .setNewIndexName(newIndexName)
                    .setConditions(rolloverConditions)
                    .request(),
                rolloverListener
            );
        }, rolloverListener::onFailure);

        // 1. Create necessary aliases
        MlIndexAndAlias.createAliasForRollover(originSettingClient, index, rolloverAlias, getIndicesAliasesListener);
    }

    // public for testing
    public void setRolloverConditions(RolloverConditions rolloverConditions) {
        this.rolloverConditions = Objects.requireNonNull(rolloverConditions);
    }

    // public for testing
    public void triggerRollResultsIndicesIfNecessaryTask(ActionListener<AcknowledgedResponse> finalListener) {

        ClusterState clusterState = clusterService.state();
        // list all indices starting .ml-anomalies-
        // this includes the shared index and all custom results indices
        String[] indices = expressionResolver.concreteIndexNames(
            clusterState,
            IndicesOptions.lenientExpandOpenHidden(),
            AnomalyDetectorsIndex.jobResultsIndexPattern()
        );

        logger.info("[ML] maintenance task: triggerRollResultsIndicesIfNecessaryTask");

        if (indices.length == 0) {
            // Early bath
            finalListener.onResponse(AcknowledgedResponse.TRUE);
        }

        for (String index : indices) {
            // Check if this index has already been rolled over
            String latestIndex = MlIndexAndAlias.latestIndexMatchingBaseName(index, expressionResolver, clusterState);

            if (index.equals(latestIndex) == false) {
                continue;
            }

            ActionListener<Boolean> rollAndUpdateAliasesResponseListener = finalListener.delegateFailureAndWrap(
                (l, rolledAndUpdatedAliasesResponse) -> {
                    if (rolledAndUpdatedAliasesResponse) {
                        logger.info(
                            "Successfully completed [ML] maintenance task: triggerRollResultsIndicesIfNecessaryTask for index [{}]",
                            index
                        );
                    } else {
                        logger.warn(
                            "Unsuccessful run of [ML] maintenance task: triggerRollResultsIndicesIfNecessaryTask for index [{}]",
                            index
                        );
                    }
                    l.onResponse(AcknowledgedResponse.TRUE); // TODO return false if operation failed for any index?
                }
            );

            rollAndUpdateAliases(clusterState, index, rollAndUpdateAliasesResponseListener);
        }
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
