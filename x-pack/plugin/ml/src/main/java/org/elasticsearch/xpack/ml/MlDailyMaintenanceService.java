/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
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

    private static final Logger logger = LogManager.getLogger(MlDailyMaintenanceService.class);

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

    private final boolean isAnomalyDetectionEnabled;
    private final boolean isDataFrameAnalyticsEnabled;
    private final boolean isNlpEnabled;

    private volatile Scheduler.Cancellable cancellable;
    private volatile float deleteExpiredDataRequestsPerSecond;

    MlDailyMaintenanceService(
        Settings settings,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService,
        MlAssignmentNotifier mlAssignmentNotifier,
        Supplier<TimeValue> scheduleProvider,
        boolean isAnomalyDetectionEnabled,
        boolean isDataFrameAnalyticsEnabled,
        boolean isNlpEnabled
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.mlAssignmentNotifier = Objects.requireNonNull(mlAssignmentNotifier);
        this.schedulerProvider = Objects.requireNonNull(scheduleProvider);
        this.deleteExpiredDataRequestsPerSecond = MachineLearning.NIGHTLY_MAINTENANCE_REQUESTS_PER_SECOND.get(settings);
        this.isAnomalyDetectionEnabled = isAnomalyDetectionEnabled;
        this.isDataFrameAnalyticsEnabled = isDataFrameAnalyticsEnabled;
        this.isNlpEnabled = isNlpEnabled;
    }

    public MlDailyMaintenanceService(
        Settings settings,
        ClusterName clusterName,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService,
        MlAssignmentNotifier mlAssignmentNotifier,
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
            isAnomalyDetectionEnabled,
            isDataFrameAnalyticsEnabled,
            isNlpEnabled
        );
    }

    void setDeleteExpiredDataRequestsPerSecond(float value) {
        this.deleteExpiredDataRequestsPerSecond = value;
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
        // Step 4: Log any error that could have happened
        ActionListener<AcknowledgedResponse> finalListener = ActionListener.wrap(
            unused -> {},
            e -> logger.warn("An error occurred during [ML] maintenance tasks execution", e)
        );

        // Step 3: Delete expired data
        ActionListener<AcknowledgedResponse> deleteJobsListener = ActionListener.wrap(
            unused -> triggerDeleteExpiredDataTask(finalListener),
            e -> {
                logger.warn("[ML] maintenance task: triggerResetJobsInStateResetWithoutResetTask failed", e);
                // Note: Steps 1-3 are independent, so continue upon errors.
                triggerDeleteExpiredDataTask(finalListener);
            }
        );

        // Step 2: Reset jobs that are in resetting state without task
        ActionListener<AcknowledgedResponse> resetJobsListener = ActionListener.wrap(
            unused -> triggerResetJobsInStateResetWithoutResetTask(deleteJobsListener),
            e -> {
                logger.warn("[ML] maintenance task: triggerDeleteJobsInStateDeletingWithoutDeletionTask failed", e);
                // Note: Steps 1-3 are independent, so continue upon errors.
                triggerResetJobsInStateResetWithoutResetTask(deleteJobsListener);
            }
        );

        // Step 1: Delete jobs that are in deleting state without task
        triggerDeleteJobsInStateDeletingWithoutDeletionTask(resetJobsListener);
    }

    private void triggerDataFrameAnalyticsMaintenance() {
        // Currently a NOOP
    }

    private void triggerNlpMaintenance() {
        // Currently a NOOP
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
