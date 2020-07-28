/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
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

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * A service that runs once a day and triggers maintenance tasks.
 */
public class MlDailyMaintenanceService implements Releasable {

    private static final Logger LOGGER = LogManager.getLogger(MlDailyMaintenanceService.class);

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

    private volatile Scheduler.Cancellable cancellable;
    private volatile float deleteExpiredDataRequestsPerSecond;

    MlDailyMaintenanceService(Settings settings, ThreadPool threadPool, Client client, ClusterService clusterService,
                              MlAssignmentNotifier mlAssignmentNotifier, Supplier<TimeValue> scheduleProvider) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.client = Objects.requireNonNull(client);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.mlAssignmentNotifier = Objects.requireNonNull(mlAssignmentNotifier);
        this.schedulerProvider = Objects.requireNonNull(scheduleProvider);
        this.deleteExpiredDataRequestsPerSecond = MachineLearning.NIGHTLY_MAINTENANCE_REQUESTS_PER_SECOND.get(settings);
    }

    public MlDailyMaintenanceService(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                     Client client, ClusterService clusterService, MlAssignmentNotifier mlAssignmentNotifier) {
        this(settings, threadPool, client, clusterService, mlAssignmentNotifier, () -> delayToNextTime(clusterName));
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
        ZonedDateTime next = now.plusDays(1)
            .toLocalDate()
            .atStartOfDay(now.getZone())
            .plusMinutes(30)
            .plusMinutes(minutesOffset);
        return TimeValue.timeValueMillis(next.toInstant().toEpochMilli() - now.toInstant().toEpochMilli());
    }

    public synchronized void start() {
        LOGGER.debug("Starting ML daily maintenance service");
        scheduleNext();
    }

    public synchronized void stop() {
        LOGGER.debug("Stopping ML daily maintenance service");
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
                LOGGER.debug("failed to schedule next maintenance task; shutting down", e);
            } else {
                throw e;
            }
        }
    }

    private void triggerTasks() {
        try {
            if (MlMetadata.getMlMetadata(clusterService.state()).isUpgradeMode()) {
                LOGGER.warn("skipping scheduled [ML] maintenance tasks because upgrade mode is enabled");
                return;
            }
            LOGGER.info("triggering scheduled [ML] maintenance tasks");

            // Step 3: Log whether the tasks have finished
            ActionListener<AcknowledgedResponse> finalListener = ActionListener.wrap(
                unused -> LOGGER.info("Completed [ML] maintenance tasks"),
                e -> LOGGER.error("An error occurred during maintenance tasks execution", e)
            );

            // Step 2: Delete jobs that are in deleting state
            ActionListener<AcknowledgedResponse> deleteExpiredDataActionListener = ActionListener.wrap(
                unused -> triggerDeleteJobsInStateDeletingWithoutDeletionTask(finalListener),
                finalListener::onFailure
            );

            // Step 1: Delete expired data
            triggerDeleteExpiredDataTask(deleteExpiredDataActionListener);

            auditUnassignedMlTasks();
        } finally {
            scheduleNext();
        }
    }

    private void triggerDeleteExpiredDataTask(ActionListener<AcknowledgedResponse> finalListener) {
        ActionListener<DeleteExpiredDataAction.Response> deleteExpiredDataActionListener = ActionListener.wrap(
            deleteExpiredDataResponse -> {
                if (deleteExpiredDataResponse.isDeleted()) {
                    LOGGER.info("Successfully completed [ML] maintenance task: triggerDeleteExpiredDataTask");
                } else {
                    LOGGER.info("Halting [ML] maintenance tasks before completion as elapsed time is too great");
                }
                finalListener.onResponse(new AcknowledgedResponse(true));
            },
            finalListener::onFailure
        );

        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            DeleteExpiredDataAction.INSTANCE,
            new DeleteExpiredDataAction.Request(deleteExpiredDataRequestsPerSecond, TimeValue.timeValueHours(8)),
            deleteExpiredDataActionListener);
    }

    // Visible for testing
    public void triggerDeleteJobsInStateDeletingWithoutDeletionTask(ActionListener<AcknowledgedResponse> finalListener) {
        SetOnce<Set<String>> jobsInStateDeletingHolder = new SetOnce<>();

        ActionListener<List<AcknowledgedResponse>> deleteJobsActionListener = ActionListener.wrap(
            deleteJobsResponses -> {
                if (deleteJobsResponses.stream().allMatch(AcknowledgedResponse::isAcknowledged)) {
                    LOGGER.info("Successfully completed [ML] maintenance task: triggerDeleteJobsInStateDeletingWithoutDeletionTask");
                } else {
                    LOGGER.info("At least one of the ML jobs could not be deleted.");
                }
                finalListener.onResponse(new AcknowledgedResponse(true));
            },
            finalListener::onFailure
        );

        ActionListener<ListTasksResponse> listTasksActionListener = ActionListener.wrap(
            listTasksResponse -> {
                Set<String> jobsInStateDeleting = jobsInStateDeletingHolder.get();
                Set<String> jobsWithDeletionTask =
                    listTasksResponse.getTasks().stream()
                        .filter(t -> t.getDescription() != null)
                        .filter(t -> t.getDescription().startsWith(DeleteJobAction.DELETION_TASK_DESCRIPTION_PREFIX))
                        .map(t -> t.getDescription().substring(DeleteJobAction.DELETION_TASK_DESCRIPTION_PREFIX.length()))
                        .collect(toSet());
                Set<String> jobsInStateDeletingWithoutDeletionTask = Sets.difference(jobsInStateDeleting, jobsWithDeletionTask);

                TypedChainTaskExecutor<AcknowledgedResponse> chainTaskExecutor =
                    new TypedChainTaskExecutor<>(threadPool.generic(), unused -> true, unused -> true);
                for (String jobId : jobsInStateDeletingWithoutDeletionTask) {
                    chainTaskExecutor.add(
                        listener ->
                            executeAsyncWithOrigin(
                                client, ML_ORIGIN, DeleteJobAction.INSTANCE, new DeleteJobAction.Request(jobId), listener)
                    );
                }
                chainTaskExecutor.execute(deleteJobsActionListener);
            },
            finalListener::onFailure
        );

        ActionListener<GetJobsAction.Response> getJobsActionListener = ActionListener.wrap(
            getJobsResponse -> {
                Set<String> jobsInStateDeleting =
                    getJobsResponse.getResponse().results().stream()
                        .filter(Job::isDeleting)
                        .map(Job::getId)
                        .collect(toSet());
                if (jobsInStateDeleting.isEmpty()) {
                    finalListener.onResponse(new AcknowledgedResponse(true));
                    return;
                }
                jobsInStateDeletingHolder.set(jobsInStateDeleting);
                executeAsyncWithOrigin(
                    client,
                    ML_ORIGIN,
                    ListTasksAction.INSTANCE,
                    new ListTasksRequest().setActions(DeleteJobAction.NAME),
                    listTasksActionListener);
            },
            finalListener::onFailure
        );

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
