/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.scheduler.CronSchedule;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.ilm.OperationModeUpdateTask;

import java.io.Closeable;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * {@code SnapshotLifecycleService} manages snapshot policy scheduling and triggering of the
 * {@link SnapshotLifecycleTask}. It reacts to new policies in the cluster state by scheduling a
 * task according to the policy's schedule.
 */
public class SnapshotLifecycleService implements Closeable, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SnapshotLifecycleService.class);
    private static final String JOB_PATTERN_SUFFIX = "-\\d+$";

    private final SchedulerEngine scheduler;
    private final ClusterService clusterService;
    private final SnapshotLifecycleTask snapshotTask;
    private final Map<String, SchedulerEngine.Job> scheduledTasks = ConcurrentCollections.newConcurrentMap();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile boolean isMaster = false;

    public SnapshotLifecycleService(Settings settings,
                                    Supplier<SnapshotLifecycleTask> taskSupplier,
                                    ClusterService clusterService,
                                    Clock clock) {
        this.scheduler = new SchedulerEngine(settings, clock);
        this.clusterService = clusterService;
        this.snapshotTask = taskSupplier.get();
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public void init() {
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        // Instead of using a LocalNodeMasterListener to track master changes, this service will
        // track them here to avoid conditions where master listener events run after other
        // listeners that depend on what happened in the master listener
        final boolean prevIsMaster = this.isMaster;
        if (prevIsMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster();
            if (this.isMaster) {
                scheduler.register(snapshotTask);
            } else {
                scheduler.unregister(snapshotTask);
                cancelSnapshotJobs();
            }
        }

        if (this.isMaster) {
            final ClusterState state = event.state();

            if (slmStoppedOrStopping(state)) {
                if (scheduler.scheduledJobIds().size() > 0) {
                    cancelSnapshotJobs();
                }
                if (slmStopping(state)) {
                    submitOperationModeUpdate(OperationMode.STOPPED);
                }
                return;
            }

            scheduleSnapshotJobs(state);
            cleanupDeletedPolicies(state);
        }
    }

    // Only used for testing
    SchedulerEngine getScheduler() {
        return this.scheduler;
    }

    /**
     * Returns true if SLM is in the stopping or stopped state
     */
    static boolean slmStoppedOrStopping(ClusterState state) {
        return Optional.ofNullable((SnapshotLifecycleMetadata) state.metadata().custom(SnapshotLifecycleMetadata.TYPE))
            .map(SnapshotLifecycleMetadata::getOperationMode)
            .map(mode -> OperationMode.STOPPING == mode || OperationMode.STOPPED == mode)
            .orElse(false);
    }

    /**
     * Returns true if SLM is in the stopping state
     */
    static boolean slmStopping(ClusterState state) {
        return Optional.ofNullable((SnapshotLifecycleMetadata) state.metadata().custom(SnapshotLifecycleMetadata.TYPE))
            .map(SnapshotLifecycleMetadata::getOperationMode)
            .map(mode -> OperationMode.STOPPING == mode)
            .orElse(false);
    }

    public void submitOperationModeUpdate(OperationMode mode) {
        clusterService.submitStateUpdateTask("slm_operation_mode_update", OperationModeUpdateTask.slmMode(mode));
    }

    /**
     * Schedule all non-scheduled snapshot jobs contained in the cluster state
     */
    public void scheduleSnapshotJobs(final ClusterState state) {
        SnapshotLifecycleMetadata snapMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta != null) {
            snapMeta.getSnapshotConfigurations().values().forEach(this::maybeScheduleSnapshot);
        }
    }

    public void cleanupDeletedPolicies(final ClusterState state) {
        SnapshotLifecycleMetadata snapMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        if (snapMeta != null) {
            // Retrieve all of the expected policy job ids from the policies in the metadata
            final Set<String> policyJobIds = snapMeta.getSnapshotConfigurations().values().stream()
                .map(SnapshotLifecycleService::getJobId)
                .collect(Collectors.toSet());

            // Cancel all jobs that are *NOT* in the scheduled tasks map
            scheduledTasks.keySet().stream()
                .filter(jobId -> policyJobIds.contains(jobId) == false)
                .forEach(this::cancelScheduledSnapshot);
        }
    }

    /**
     * Schedule the {@link SnapshotLifecyclePolicy} job if it does not already exist. First checks
     * to see if any previous versions of the policy were scheduled, and if so, cancels those. If
     * the same version of a policy has already been scheduled it does not overwrite the job.
     */
    public void maybeScheduleSnapshot(final SnapshotLifecyclePolicyMetadata snapshotLifecyclePolicy) {
        if (this.running.get() == false) {
            return;
        }

        final String jobId = getJobId(snapshotLifecyclePolicy);
        final Pattern existingJobPattern = Pattern.compile(snapshotLifecyclePolicy.getPolicy().getId() + JOB_PATTERN_SUFFIX);

        // Find and cancel any existing jobs for this policy
        final boolean existingJobsFoundAndCancelled = scheduledTasks.keySet().stream()
            // Find all jobs matching the `jobid-\d+` pattern
            .filter(jId -> existingJobPattern.matcher(jId).matches())
            // Filter out a job that has not been changed (matches the id exactly meaning the version is the same)
            .filter(jId -> jId.equals(jobId) == false)
            .map(existingJobId -> {
                // Cancel existing job so the new one can be scheduled
                logger.debug("removing existing snapshot lifecycle job [{}] as it has been updated", existingJobId);
                scheduledTasks.remove(existingJobId);
                boolean existed = scheduler.remove(existingJobId);
                assert existed : "expected job for " + existingJobId + " to exist in scheduler";
                return existed;
            })
            .reduce(false, (a, b) -> a || b);

        // Now atomically schedule the new job and add it to the scheduled tasks map. If the jobId
        // is identical to an existing job (meaning the version has not changed) then this does
        // not reschedule it.
        scheduledTasks.computeIfAbsent(jobId, id -> {
            final SchedulerEngine.Job job = new SchedulerEngine.Job(jobId,
                new CronSchedule(snapshotLifecyclePolicy.getPolicy().getSchedule()));
            if (existingJobsFoundAndCancelled) {
                logger.info("rescheduling updated snapshot lifecycle job [{}]", jobId);
            } else {
                logger.info("scheduling snapshot lifecycle job [{}]", jobId);
            }
            scheduler.add(job);
            return job;
        });
    }

    /**
     * Generate the job id for a given policy metadata. The job id is {@code <policyid>-<version>}
     */
    public static String getJobId(SnapshotLifecyclePolicyMetadata policyMeta) {
        return policyMeta.getPolicy().getId() + "-" + policyMeta.getVersion();
    }

    /**
     * Cancel all scheduled snapshot jobs
     */
    public void cancelSnapshotJobs() {
        logger.trace("cancelling all snapshot lifecycle jobs");
        scheduler.scheduledJobIds().forEach(scheduler::remove);
        scheduledTasks.clear();
    }

    /**
     * Cancel the given policy job id (from {@link #getJobId(SnapshotLifecyclePolicyMetadata)}
     */
    public void cancelScheduledSnapshot(final String lifecycleJobId) {
        logger.debug("cancelling snapshot lifecycle job [{}] as it no longer exists", lifecycleJobId);
        scheduledTasks.remove(lifecycleJobId);
        scheduler.remove(lifecycleJobId);
    }

    /**
     * Validates that the {@code repository} exists as a registered snapshot repository
     * @throws IllegalArgumentException if the repository does not exist
     */
    public static void validateRepositoryExists(final String repository, final ClusterState state) {
        if (state.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY).repository(repository) == null) {
            throw new IllegalArgumentException("no such repository [" + repository + "]");
        }
    }

    /**
     * Validates that the interval between snapshots is not smaller than the minimum interval
     * (see {@link LifecycleSettings#SLM_MINIMUM_INTERVAL_SETTING})
     * @throws IllegalArgumentException if the interval is less than the minimum
     */
    public static void validateMinimumInterval(final SnapshotLifecyclePolicy lifecycle, final ClusterState state) {
        TimeValue minimum = LifecycleSettings.SLM_MINIMUM_INTERVAL_SETTING.get(state.metadata().settings());
        TimeValue next = lifecycle.calculateNextInterval();
        if (next.duration() > 0 && minimum.duration() > 0 && next.millis() < minimum.millis()) {
            throw new IllegalArgumentException("invalid schedule [" + lifecycle.getSchedule() + "]: " +
                "schedule would be too frequent, executing more than every [" + minimum.getStringRep() + "]");
        }
    }

    @Override
    public void close() {
        if (this.running.compareAndSet(true, false)) {
            this.scheduler.stop();
        }
    }
}
