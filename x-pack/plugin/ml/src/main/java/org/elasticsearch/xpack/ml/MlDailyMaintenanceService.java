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
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
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
            cancellable = threadPool.schedule(this::triggerTasks, schedulerProvider.get(), ThreadPool.Names.GENERIC);
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
        // Step 3: Log any error that could have happened
        ActionListener<AcknowledgedResponse> finalListener = ActionListener.wrap(
            unused -> {},
            e -> logger.error("An error occurred during [ML] maintenance tasks execution", e)
        );

        // Step 2: Delete expired data
        ActionListener<AcknowledgedResponse> deleteJobsListener = ActionListener.wrap(
            unused -> triggerDeleteExpiredDataTask(finalListener),
            e -> {
                logger.info("[ML] maintenance task: triggerDeleteJobsInStateDeletingWithoutDeletionTask failed", e);
                // Note: Steps 1 and 2 are independent of each other and step 2 is executed even if step 1 failed.
                triggerDeleteExpiredDataTask(finalListener);
            }
        );

        // Step 1: Delete jobs that are in deleting state
        triggerDeleteJobsInStateDeletingWithoutDeletionTask(deleteJobsListener);
    }

    private void triggerDataFrameAnalyticsMaintenance() {
        // Currently a NOOP
    }

    private void triggerNlpMaintenance() {
        // Currently a NOOP
    }

    private void triggerDeleteExpiredDataTask(ActionListener<AcknowledgedResponse> finalListener) {
        ActionListener<DeleteExpiredDataAction.Response> deleteExpiredDataActionListener = ActionListener.wrap(
            deleteExpiredDataResponse -> {
                if (deleteExpiredDataResponse.isDeleted()) {
                    logger.info("Successfully completed [ML] maintenance task: triggerDeleteExpiredDataTask");
                } else {
                    logger.info("Halting [ML] maintenance tasks before completion as elapsed time is too great");
                }
                finalListener.onResponse(AcknowledgedResponse.TRUE);
            },
            finalListener::onFailure
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
        SetOnce<Set<String>> jobsInStateDeletingHolder = new SetOnce<>();

        ActionListener<List<Tuple<DeleteJobAction.Request, AcknowledgedResponse>>> deleteJobsActionListener = ActionListener.wrap(
            deleteJobsResponses -> {
                List<String> jobIds = deleteJobsResponses.stream()
                    .filter(t -> t.v2().isAcknowledged() == false)
                    .map(Tuple::v1)
                    .map(DeleteJobAction.Request::getJobId)
                    .collect(toList());
                if (jobIds.isEmpty()) {
                    logger.info("Successfully completed [ML] maintenance task: triggerDeleteJobsInStateDeletingWithoutDeletionTask");
                } else {
                    logger.info("The following ML jobs could not be deleted: [" + String.join(",", jobIds) + "]");
                }
                finalListener.onResponse(AcknowledgedResponse.TRUE);
            },
            finalListener::onFailure
        );

        ActionListener<ListTasksResponse> listTasksActionListener = ActionListener.wrap(listTasksResponse -> {
            Set<String> jobsInStateDeleting = jobsInStateDeletingHolder.get();
            Set<String> jobsWithDeletionTask = listTasksResponse.getTasks()
                .stream()
                .filter(t -> t.description() != null)
                .filter(t -> t.description().startsWith(DeleteJobAction.DELETION_TASK_DESCRIPTION_PREFIX))
                .map(t -> t.description().substring(DeleteJobAction.DELETION_TASK_DESCRIPTION_PREFIX.length()))
                .collect(toSet());
            Set<String> jobsInStateDeletingWithoutDeletionTask = Sets.difference(jobsInStateDeleting, jobsWithDeletionTask);
            if (jobsInStateDeletingWithoutDeletionTask.isEmpty()) {
                finalListener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            TypedChainTaskExecutor<Tuple<DeleteJobAction.Request, AcknowledgedResponse>> chainTaskExecutor = new TypedChainTaskExecutor<>(
                threadPool.executor(ThreadPool.Names.SAME),
                unused -> true,
                unused -> true
            );
            for (String jobId : jobsInStateDeletingWithoutDeletionTask) {
                DeleteJobAction.Request request = new DeleteJobAction.Request(jobId);
                chainTaskExecutor.add(
                    listener -> executeAsyncWithOrigin(
                        client,
                        ML_ORIGIN,
                        DeleteJobAction.INSTANCE,
                        request,
                        ActionListener.wrap(response -> listener.onResponse(Tuple.tuple(request, response)), listener::onFailure)
                    )
                );
            }
            chainTaskExecutor.execute(deleteJobsActionListener);
        }, finalListener::onFailure);

        ActionListener<GetJobsAction.Response> getJobsActionListener = ActionListener.wrap(getJobsResponse -> {
            Set<String> jobsInStateDeleting = getJobsResponse.getResponse()
                .results()
                .stream()
                .filter(Job::isDeleting)
                .map(Job::getId)
                .collect(toSet());
            if (jobsInStateDeleting.isEmpty()) {
                finalListener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            jobsInStateDeletingHolder.set(jobsInStateDeleting);
            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                ListTasksAction.INSTANCE,
                new ListTasksRequest().setActions(DeleteJobAction.NAME),
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
        PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasks != null) {
            mlAssignmentNotifier.auditUnassignedMlTasks(state.nodes(), tasks);
        }
    }
}
