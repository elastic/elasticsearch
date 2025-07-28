/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

/**
 * This class keeps track of the memory requirement of ML jobs.
 * It only functions on the master node - for this reason it should only be used by master node actions.
 * The memory requirement for ML jobs can be updated in 4 ways:
 * 1. For all open ML data frame analytics jobs, anomaly detector jobs and model snapshot upgrades (via {@link #asyncRefresh})
 * 2. For all open/started ML jobs and model snapshot upgrades, plus one named ML anomaly detector job that may not be open
 *    (via {@link #refreshAnomalyDetectorJobMemoryAndAllOthers})
 * 3. For all open/started ML jobs and model snapshot upgrades, plus one named ML data frame analytics job that is not started
 *    (via {@link #addDataFrameAnalyticsJobMemoryAndRefreshAllOthers})
 * 4. For one named ML anomaly detector job or model snapshot upgrade (via {@link #refreshAnomalyDetectorJobMemory})
 * In cases 2, 3 and 4 a listener informs the caller when the requested updates are complete.
 */
public class MlMemoryTracker implements LocalNodeMasterListener {

    private static final Duration RECENT_UPDATE_THRESHOLD = Duration.ofMinutes(1);
    private static final Duration DEFAULT_AUTOSCALING_CHECK_INTERVAL = Duration.ofMinutes(5);

    private static final Logger logger = LogManager.getLogger(MlMemoryTracker.class);
    private final Map<String, Long> memoryRequirementByAnomalyDetectorJob = new ConcurrentHashMap<>();
    private final Map<String, Long> memoryRequirementByDataFrameAnalyticsJob = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> memoryRequirementByTaskName;
    private final List<ActionListener<Void>> fullRefreshCompletionListeners = new ArrayList<>();

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final JobManager jobManager;
    private final JobResultsProvider jobResultsProvider;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final Phaser stopPhaser;
    private final AtomicInteger phase = new AtomicInteger(0);
    private volatile boolean isMaster;
    private volatile boolean stopped;
    private volatile Instant lastUpdateTime;
    private volatile Duration reassignmentRecheckInterval;
    private volatile Duration autoscalingCheckInterval = DEFAULT_AUTOSCALING_CHECK_INTERVAL;

    public MlMemoryTracker(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        JobManager jobManager,
        JobResultsProvider jobResultsProvider,
        DataFrameAnalyticsConfigProvider configProvider
    ) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.jobManager = jobManager;
        this.jobResultsProvider = jobResultsProvider;
        this.configProvider = configProvider;
        this.stopPhaser = new Phaser(1);

        Map<String, Map<String, Long>> memoryRequirementByTaskName = new TreeMap<>();
        memoryRequirementByTaskName.put(MlTasks.JOB_TASK_NAME, memoryRequirementByAnomalyDetectorJob);
        memoryRequirementByTaskName.put(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, memoryRequirementByDataFrameAnalyticsJob);
        // We don't add snapshot upgrade tasks here - instead, we assume they
        // have the same memory requirement as the job they correspond to.
        this.memoryRequirementByTaskName = Collections.unmodifiableMap(memoryRequirementByTaskName);

        setReassignmentRecheckInterval(PersistentTasksClusterService.CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING.get(settings));
        clusterService.addLocalNodeMasterListener(this);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                PersistentTasksClusterService.CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING,
                this::setReassignmentRecheckInterval
            );
    }

    private void setReassignmentRecheckInterval(TimeValue recheckInterval) {
        reassignmentRecheckInterval = Duration.ofNanos(recheckInterval.getNanos());
    }

    public void setAutoscalingCheckInterval(Duration autoscalingCheckInterval) {
        this.autoscalingCheckInterval = Objects.requireNonNull(autoscalingCheckInterval);
    }

    @Override
    public void onMaster() {
        isMaster = true;
        try {
            asyncRefresh();
        } catch (Exception ex) {
            logger.warn("unexpected failure while attempting asynchronous refresh on new master assignment", ex);
        }
        logger.trace("ML memory tracker on master");
    }

    @Override
    public void offMaster() {
        isMaster = false;
        logger.trace("ML memory tracker off master");
        clear();
    }

    public void awaitAndClear(ActionListener<Void> listener) {
        // We never terminate the phaser
        logger.trace("awaiting and clearing memory tracker");
        assert stopPhaser.isTerminated() == false;
        // If there are no registered parties or no unarrived parties then there is a flaw
        // in the register/arrive/unregister logic in another method that uses the phaser
        assert stopPhaser.getRegisteredParties() > 0;
        assert stopPhaser.getUnarrivedParties() > 0;
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            try {
                // We await all current refreshes to complete, this increments the "current phase" and prevents
                // further interaction while we clear contents
                int newPhase = stopPhaser.arriveAndAwaitAdvance();
                assert newPhase > 0;
                clear();
                phase.incrementAndGet();
                logger.trace("completed awaiting and clearing memory tracker");
                listener.onResponse(null);
            } catch (Exception e) {
                logger.warn("failed to wait for all refresh requests to complete", e);
                listener.onFailure(e);
            }
        });

    }

    private void clear() {
        logger.trace("clearing ML Memory tracker contents");
        for (Map<String, Long> memoryRequirementByJob : memoryRequirementByTaskName.values()) {
            memoryRequirementByJob.clear();
        }
        lastUpdateTime = null;
    }

    /**
     * Wait for all outstanding searches to complete.
     * After returning, no new searches can be started.
     */
    public void stop() {
        stopped = true;
        logger.trace("ML memory tracker stop called");
        // We never terminate the phaser
        assert stopPhaser.isTerminated() == false;
        // If there are no registered parties or no unarrived parties then there is a flaw
        // in the register/arrive/unregister logic in another method that uses the phaser
        assert stopPhaser.getRegisteredParties() > 0;
        assert stopPhaser.getUnarrivedParties() > 0;
        stopPhaser.arriveAndAwaitAdvance();
        assert stopPhaser.getPhase() > 0;
        logger.debug("ML memory tracker stopped");
    }

    /**
     * Is the information in this object sufficiently up to date
     * for valid task assignment decisions to be made using it?
     */
    public boolean isRecentlyRefreshed() {
        Instant localLastUpdateTime = lastUpdateTime;
        return isMaster && localLastUpdateTime != null && localLastUpdateTime.plus(getStalenessDuration()).isAfter(Instant.now());
    }

    /**
     * @return The definition of "staleness" used by {@link #isRecentlyRefreshed()}. This method is intended only as
     *         a debugging aid, as calling it separately to {@link #isRecentlyRefreshed()} could return a different
     *         number if settings were modified in between the two calls.
     */
    public Duration getStalenessDuration() {
        return max(reassignmentRecheckInterval, autoscalingCheckInterval).plus(RECENT_UPDATE_THRESHOLD);
    }

    static Duration max(Duration first, Duration second) {
        return first.compareTo(second) > 0 ? first : second;
    }

    /**
     * Has the information in this object ever been refreshed?
     * This method is intended for use when it's not essential to
     * have the most up-to-date information, but having <em>some</em>
     * information is necessary, for example in telemetry.
     */
    public boolean isEverRefreshed() {
        return isMaster && lastUpdateTime != null;
    }

    /**
     * Get the memory requirement for an anomaly detector job.
     * This method only works on the master node.
     * @param jobId The job ID.
     * @return The memory requirement of the job specified by {@code jobId},
     *         or <code>null</code> if it cannot be calculated.
     */
    public Long getAnomalyDetectorJobMemoryRequirement(String jobId) {
        return getJobMemoryRequirement(MlTasks.JOB_TASK_NAME, jobId);
    }

    /**
     * Get the memory requirement for a data frame analytics job.
     * This method only works on the master node.
     * @param id The job ID.
     * @return The memory requirement of the job specified by {@code id},
     *         or <code>null</code> if it cannot be found.
     */
    public Long getDataFrameAnalyticsJobMemoryRequirement(String id) {
        return getJobMemoryRequirement(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, id);
    }

    /**
     * Get the memory requirement for a trained model task.
     * This method only works on the master node.
     * @param modelId The model ID.
     * @return The memory requirement of the trained model task specified by {@code modelId},
     *         or <code>null</code> if it cannot be found.
     */
    public Long getTrainedModelAssignmentMemoryRequirement(String modelId) {
        if (isMaster == false) {
            logger.warn("Request to get trained model assignment memory not on master node; modelId was [{}]", modelId);
            return null;
        }

        return Optional.ofNullable(TrainedModelAssignmentMetadata.fromState(clusterService.state()).allAssignments().get(modelId))
            .map(TrainedModelAssignment::getTaskParams)
            .map(StartTrainedModelDeploymentAction.TaskParams::estimateMemoryUsageBytes)
            .orElse(null);
    }

    /**
     * Get the memory requirement for the type of job corresponding to a specified persistent task name.
     * This method only works on the master node.
     * @param taskName The persistent task name.
     * @param id The job ID.
     * @return The memory requirement of the job specified by {@code id},
     *         or <code>null</code> if it cannot be found.
     */
    public Long getJobMemoryRequirement(String taskName, String id) {

        if (isMaster == false) {
            logger.warn("Request to get job memory not on master node; taskName [{}], id [{}]", taskName, id);
            return null;
        }

        // Assume snapshot upgrade tasks have the same memory requirement as the job they correspond to.
        if (MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME.equals(taskName)) {
            taskName = MlTasks.JOB_TASK_NAME;
        }

        Map<String, Long> memoryRequirementByJob = memoryRequirementByTaskName.get(taskName);
        if (memoryRequirementByJob == null) {
            assert false : "Unknown taskName type [" + taskName + "]";
            return null;
        }
        return memoryRequirementByJob.get(id);
    }

    /**
     * Remove any memory requirement that is stored for the specified anomaly detector job.
     * It doesn't matter if this method is called for a job that doesn't have a
     * stored memory requirement.
     */
    public void removeAnomalyDetectorJob(String jobId) {
        memoryRequirementByAnomalyDetectorJob.remove(jobId);
    }

    /**
     * Remove any memory requirement that is stored for the specified data frame analytics
     * job.  It doesn't matter if this method is called for a job that doesn't have a
     * stored memory requirement.
     */
    public void removeDataFrameAnalyticsJob(String id) {
        memoryRequirementByDataFrameAnalyticsJob.remove(id);
    }

    /**
     * Uses a separate thread to refresh the memory requirement for every ML anomaly detector job that has
     * a corresponding persistent task.  This method only works on the master node.
     * @return <code>true</code> if the async refresh is scheduled, and <code>false</code>
     *         if this is not possible for some reason.
     */
    public boolean asyncRefresh() {

        if (isMaster) {
            try {
                ActionListener<Void> listener = ActionListener.wrap(
                    aVoid -> logger.trace("Job memory requirement refresh request completed successfully"),
                    e -> logIfNecessary(() -> logger.warn("Failed to refresh job memory requirements", e))
                );
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
                    .execute(
                        () -> refresh(
                            clusterService.state().getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE),
                            listener
                        )
                    );
                return true;
            } catch (EsRejectedExecutionException e) {
                logger.warn("Couldn't schedule ML memory update - node might be shutting down", e);
            }
        }

        return false;
    }

    /**
     * This refreshes the memory requirement for every ML job that has a corresponding
     * persistent task and, in addition, one job that doesn't have a persistent task.
     * This method only works on the master node.
     * @param jobId The job ID of the job whose memory requirement is to be refreshed
     *              despite not having a corresponding persistent task.
     * @param listener Receives the memory requirement of the job specified by {@code jobId},
     *                 or <code>null</code> if it cannot be calculated.
     */
    public void refreshAnomalyDetectorJobMemoryAndAllOthers(String jobId, ActionListener<Long> listener) {

        if (isMaster == false) {
            String msg = "Request to refresh anomaly detector memory requirements on non-master node";
            logger.warn(msg);
            listener.onFailure(new NotMasterException(msg));
            return;
        }

        // Skip the provided job ID in the main refresh, as we unconditionally do it at the end.
        // Otherwise it might get refreshed twice, because it could have both a job task and a snapshot upgrade task.
        refresh(
            clusterService.state().getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE),
            Collections.singleton(jobId),
            listener.delegateFailureAndWrap((l, aVoid) -> refreshAnomalyDetectorJobMemory(jobId, l))
        );
    }

    /**
     * This refreshes the memory requirement for every ML job that has a corresponding
     * persistent task and, in addition, adds the memory requirement of one data frame analytics
     * job that doesn't have a persistent task.  This method only works on the master node.
     * @param id The job ID of the job whose memory requirement is to be added.
     * @param mem The memory requirement (in bytes) of the job specified by {@code id}.
     * @param listener Called when the refresh is complete or fails.
     */
    public void addDataFrameAnalyticsJobMemoryAndRefreshAllOthers(String id, long mem, ActionListener<Void> listener) {

        if (isMaster == false) {
            String msg = "Request to put data frame analytics memory requirement on non-master node";
            logger.warn(msg);
            listener.onFailure(new NotMasterException(msg));
            return;
        }

        memoryRequirementByDataFrameAnalyticsJob.put(id, mem + DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes());

        PersistentTasksCustomMetadata persistentTasks = clusterService.state()
            .getMetadata()
            .getProject()
            .custom(PersistentTasksCustomMetadata.TYPE);
        refresh(persistentTasks, listener);
    }

    /**
     * This refreshes the memory requirement for every ML job that has a corresponding persistent task.
     * It does NOT remove entries for jobs that no longer have a persistent task, because that would lead
     * to a race where a job was opened part way through the refresh.  (Instead, entries are removed when
     * jobs are deleted.)
     */
    public void refresh(PersistentTasksCustomMetadata persistentTasks, ActionListener<Void> onCompletion) {
        refresh(persistentTasks, Collections.emptySet(), onCompletion);
    }

    void refresh(PersistentTasksCustomMetadata persistentTasks, Set<String> jobIdsToSkip, ActionListener<Void> onCompletion) {

        synchronized (fullRefreshCompletionListeners) {
            fullRefreshCompletionListeners.add(onCompletion);
            if (fullRefreshCompletionListeners.size() > 1) {
                // A refresh is already in progress, so don't do another
                return;
            }
        }

        ActionListener<Void> refreshComplete = ActionListener.wrap(aVoid -> {
            synchronized (fullRefreshCompletionListeners) {
                assert fullRefreshCompletionListeners.isEmpty() == false;
                if (isMaster) {
                    lastUpdateTime = Instant.now();
                    for (ActionListener<Void> listener : fullRefreshCompletionListeners) {
                        listener.onResponse(null);
                    }
                    logger.debug("ML memory tracker last update time now [{}] and listeners called", lastUpdateTime);
                } else {
                    Exception e = new NotMasterException("Node ceased to be master during ML memory tracker refresh");
                    for (ActionListener<Void> listener : fullRefreshCompletionListeners) {
                        listener.onFailure(e);
                    }
                    logger.debug(e.getMessage());
                }
                fullRefreshCompletionListeners.clear();
            }
        }, e -> {
            synchronized (fullRefreshCompletionListeners) {
                assert fullRefreshCompletionListeners.isEmpty() == false;
                for (ActionListener<Void> listener : fullRefreshCompletionListeners) {
                    listener.onFailure(e);
                }
                logIfNecessary(() -> logger.warn("ML memory tracker last update failed and listeners called", e));
                // It's critical that we empty out the current listener list on
                // error otherwise subsequent retries to refresh will be ignored
                fullRefreshCompletionListeners.clear();
            }
        });

        // persistentTasks will be null if there's never been a persistent task created in this cluster
        if (persistentTasks == null) {
            refreshComplete.onResponse(null);
        } else {
            List<PersistentTasksCustomMetadata.PersistentTask<?>> mlDataFrameAnalyticsJobTasks = persistentTasks.tasks()
                .stream()
                .filter(task -> MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME.equals(task.getTaskName()))
                .toList();
            ActionListener<Void> refreshDataFrameAnalyticsJobs = ActionListener.wrap(
                aVoid -> refreshAllDataFrameAnalyticsJobTasks(mlDataFrameAnalyticsJobTasks, refreshComplete),
                refreshComplete::onFailure
            );

            Set<String> mlAnomalyDetectorJobTasks = Stream.concat(
                persistentTasks.tasks()
                    .stream()
                    .filter(task -> MlTasks.JOB_TASK_NAME.equals(task.getTaskName()))
                    .map(task -> ((OpenJobAction.JobParams) task.getParams()).getJobId()),
                persistentTasks.tasks()
                    .stream()
                    .filter(task -> MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME.equals(task.getTaskName()))
                    .map(task -> ((SnapshotUpgradeTaskParams) task.getParams()).getJobId())
            ).filter(jobId -> jobIdsToSkip.contains(jobId) == false).collect(Collectors.toSet());
            iterateAnomalyDetectorJobs(mlAnomalyDetectorJobTasks.iterator(), refreshDataFrameAnalyticsJobs);
        }
    }

    private void iterateAnomalyDetectorJobs(Iterator<String> iterator, ActionListener<Void> refreshComplete) {
        if (iterator.hasNext()) {
            refreshAnomalyDetectorJobMemory(
                iterator.next(),
                ActionListener.wrap(
                    // Do the next iteration in a different thread, otherwise stack overflow
                    // can occur if the searches happen to be on the local node, as the huge
                    // chain of listeners are all called in the same thread if only one node
                    // is involved
                    mem -> threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
                        .execute(() -> iterateAnomalyDetectorJobs(iterator, refreshComplete)),
                    refreshComplete::onFailure
                )
            );
        } else {
            refreshComplete.onResponse(null);
        }
    }

    private void refreshAllDataFrameAnalyticsJobTasks(
        List<PersistentTasksCustomMetadata.PersistentTask<?>> mlDataFrameAnalyticsJobTasks,
        ActionListener<Void> listener
    ) {
        if (mlDataFrameAnalyticsJobTasks.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        Set<String> jobsWithTasks = mlDataFrameAnalyticsJobTasks.stream()
            .map(task -> ((StartDataFrameAnalyticsAction.TaskParams) task.getParams()).getId())
            .collect(Collectors.toSet());

        configProvider.getConfigsForJobsWithTasksLeniently(jobsWithTasks, listener.delegateFailureAndWrap((delegate, analyticsConfigs) -> {
            for (DataFrameAnalyticsConfig analyticsConfig : analyticsConfigs) {
                memoryRequirementByDataFrameAnalyticsJob.put(
                    analyticsConfig.getId(),
                    analyticsConfig.getModelMemoryLimit().getBytes() + DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes()
                );
            }
            delegate.onResponse(null);
        }));
    }

    /**
     * Refresh the memory requirement for a single anomaly detector job.
     * This method only works on the master node.
     * @param jobId    The ID of the job to refresh the memory requirement for.
     * @param listener Receives the job's memory requirement, or <code>null</code>
     *                 if it cannot be calculated.
     */
    public void refreshAnomalyDetectorJobMemory(String jobId, ActionListener<Long> listener) {
        if (isMaster == false) {
            String msg = "Request to refresh anomaly detector memory requirement on non-master node";
            logger.warn(msg);
            listener.onFailure(new NotMasterException(msg));
            return;
        }

        // The phaser prevents searches being started after the memory tracker's stop() method has returned
        // Note: `phase` is incremented if cache is reset via the feature reset API
        if (stopPhaser.register() != phase.get()) {
            // Phases above not equal to `phase` mean we've been stopped, so don't do any operations that involve external interaction
            stopPhaser.arriveAndDeregister();
            logger.info(() -> "[" + jobId + "] not refreshing anomaly detector memory as node is shutting down");
            listener.onFailure(new EsRejectedExecutionException("Couldn't run ML memory update - node is shutting down"));
            return;
        }
        ActionListener<Long> phaserListener = ActionListener.wrap(r -> {
            stopPhaser.arriveAndDeregister();
            listener.onResponse(r);
        }, e -> {
            stopPhaser.arriveAndDeregister();
            listener.onFailure(e);
        });

        try {
            jobResultsProvider.getEstablishedMemoryUsage(jobId, null, null, establishedModelMemoryBytes -> {
                if (establishedModelMemoryBytes <= 0L) {
                    setAnomalyDetectorJobMemoryToLimit(jobId, phaserListener);
                } else {
                    Long memoryRequirementBytes = establishedModelMemoryBytes + Job.PROCESS_MEMORY_OVERHEAD.getBytes();
                    memoryRequirementByAnomalyDetectorJob.put(jobId, memoryRequirementBytes);
                    phaserListener.onResponse(memoryRequirementBytes);
                }
            }, e -> {
                logIfNecessary(
                    () -> logger.error(
                        () -> format("[%s] failed to calculate anomaly detector job established model memory requirement", jobId),
                        e
                    )
                );
                setAnomalyDetectorJobMemoryToLimit(jobId, phaserListener);
            });
        } catch (Exception e) {
            logIfNecessary(
                () -> logger.error(
                    () -> format("[%s] failed to calculate anomaly detector job established model memory requirement", jobId),
                    e
                )
            );
            setAnomalyDetectorJobMemoryToLimit(jobId, phaserListener);
        }
    }

    private void setAnomalyDetectorJobMemoryToLimit(String jobId, ActionListener<Long> listener) {
        jobManager.getJob(jobId, ActionListener.wrap(job -> {
            Long memoryLimitMb = (job.getAnalysisLimits() != null) ? job.getAnalysisLimits().getModelMemoryLimit() : null;
            // Although recent versions of the code enforce a non-null model_memory_limit
            // when parsing, the job could have been streamed from an older version node in
            // a mixed version cluster
            if (memoryLimitMb == null) {
                memoryLimitMb = AnalysisLimits.PRE_6_1_DEFAULT_MODEL_MEMORY_LIMIT_MB;
            }
            Long memoryRequirementBytes = ByteSizeValue.ofMb(memoryLimitMb).getBytes() + Job.PROCESS_MEMORY_OVERHEAD.getBytes();
            memoryRequirementByAnomalyDetectorJob.put(jobId, memoryRequirementBytes);
            listener.onResponse(memoryRequirementBytes);
        }, e -> {
            if (e instanceof ResourceNotFoundException) {
                // TODO: does this also happen if the .ml-config index exists but is unavailable?
                // However, note that we wait for the .ml-config index to be available earlier on in the
                // job assignment process, so that scenario should be very rare, i.e. somebody has closed
                // the .ml-config index (which would be unexpected and unsupported for an internal index)
                // during the memory refresh.
                logger.trace("[{}] anomaly detector job deleted during ML memory update", jobId);
            } else {
                logIfNecessary(() -> logger.error(() -> "[" + jobId + "] failed to get anomaly detector job during ML memory update", e));

            }
            memoryRequirementByAnomalyDetectorJob.remove(jobId);
            listener.onResponse(null);
        }));
    }

    /**
     * To reduce spamming the log in an unstable environment, this method will only call the runnable if:
     *  - The current node is the master node (and thus valid for tracking memory)
     *  - The current node is NOT stopped (and thus not shutting down)
     * @param log Runnable that writes the log message
     */
    private void logIfNecessary(Runnable log) {
        if (isMaster && (stopped == false)) {
            log.run();
        }
    }
}
