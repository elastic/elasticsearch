/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class keeps track of the memory requirement of ML jobs.
 * It only functions on the master node - for this reason it should only be used by master node actions.
 * The memory requirement for ML jobs can be updated in 3 ways:
 * 1. For all open ML jobs (via {@link #asyncRefresh})
 * 2. For all open ML jobs, plus one named ML job that is not open (via {@link #refreshJobMemoryAndAllOthers})
 * 3. For one named ML job (via {@link #refreshJobMemory})
 * In cases 2 and 3 a listener informs the caller when the requested updates are complete.
 */
public class MlMemoryTracker implements LocalNodeMasterListener {

    private static final Duration RECENT_UPDATE_THRESHOLD = Duration.ofMinutes(1);

    private final Logger logger = LogManager.getLogger(MlMemoryTracker.class);
    private final ConcurrentHashMap<String, Long> memoryRequirementByJob = new ConcurrentHashMap<>();
    private final List<ActionListener<Void>> fullRefreshCompletionListeners = new ArrayList<>();

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final JobManager jobManager;
    private final JobResultsProvider jobResultsProvider;
    private volatile boolean isMaster;
    private volatile Instant lastUpdateTime;
    private volatile Duration reassignmentRecheckInterval;

    public MlMemoryTracker(Settings settings, ClusterService clusterService, ThreadPool threadPool, JobManager jobManager,
                           JobResultsProvider jobResultsProvider) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.jobManager = jobManager;
        this.jobResultsProvider = jobResultsProvider;
        setReassignmentRecheckInterval(PersistentTasksClusterService.CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING.get(settings));
        clusterService.addLocalNodeMasterListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            PersistentTasksClusterService.CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING, this::setReassignmentRecheckInterval);
    }

    private void setReassignmentRecheckInterval(TimeValue recheckInterval) {
        reassignmentRecheckInterval = Duration.ofNanos(recheckInterval.getNanos());
    }

    @Override
    public void onMaster() {
        isMaster = true;
        logger.trace("ML memory tracker on master");
    }

    @Override
    public void offMaster() {
        isMaster = false;
        logger.trace("ML memory tracker off master");
        memoryRequirementByJob.clear();
        lastUpdateTime = null;
    }

    @Override
    public String executorName() {
        return MachineLearning.UTILITY_THREAD_POOL_NAME;
    }

    /**
     * Is the information in this object sufficiently up to date
     * for valid task assignment decisions to be made using it?
     */
    public boolean isRecentlyRefreshed() {
        Instant localLastUpdateTime = lastUpdateTime;
        return localLastUpdateTime != null &&
            localLastUpdateTime.plus(RECENT_UPDATE_THRESHOLD).plus(reassignmentRecheckInterval).isAfter(Instant.now());
    }

    /**
     * Get the memory requirement for a job.
     * This method only works on the master node.
     * @param jobId The job ID.
     * @return The memory requirement of the job specified by {@code jobId},
     *         or <code>null</code> if it cannot be calculated.
     */
    public Long getJobMemoryRequirement(String jobId) {

        if (isMaster == false) {
            return null;
        }

        Long memoryRequirement = memoryRequirementByJob.get(jobId);
        if (memoryRequirement != null) {
            return memoryRequirement;
        }

        return null;
    }

    /**
     * Remove any memory requirement that is stored for the specified job.
     * It doesn't matter if this method is called for a job that doesn't have
     * a stored memory requirement.
     */
    public void removeJob(String jobId) {
        memoryRequirementByJob.remove(jobId);
    }

    /**
     * Uses a separate thread to refresh the memory requirement for every ML job that has
     * a corresponding persistent task.  This method only works on the master node.
     * @return <code>true</code> if the async refresh is scheduled, and <code>false</code>
     *         if this is not possible for some reason.
     */
    public boolean asyncRefresh() {

        if (isMaster) {
            try {
                ActionListener<Void> listener = ActionListener.wrap(
                    aVoid -> logger.trace("Job memory requirement refresh request completed successfully"),
                    e -> logger.error("Failed to refresh job memory requirements", e)
                );
                threadPool.executor(executorName()).execute(
                    () -> refresh(clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE), listener));
                return true;
            } catch (EsRejectedExecutionException e) {
                logger.debug("Couldn't schedule ML memory update - node might be shutting down", e);
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
    public void refreshJobMemoryAndAllOthers(String jobId, ActionListener<Long> listener) {

        if (isMaster == false) {
            listener.onResponse(null);
            return;
        }

        PersistentTasksCustomMetaData persistentTasks = clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        refresh(persistentTasks, ActionListener.wrap(aVoid -> refreshJobMemory(jobId, listener), listener::onFailure));
    }

    /**
     * This refreshes the memory requirement for every ML job that has a corresponding persistent task.
     * It does NOT remove entries for jobs that no longer have a persistent task, because that would
     * lead to a race where a job was opened part way through the refresh.  (Instead, entries are removed
     * when jobs are deleted.)
     */
    void refresh(PersistentTasksCustomMetaData persistentTasks, ActionListener<Void> onCompletion) {

        synchronized (fullRefreshCompletionListeners) {
            fullRefreshCompletionListeners.add(onCompletion);
            if (fullRefreshCompletionListeners.size() > 1) {
                // A refresh is already in progress, so don't do another
                return;
            }
        }

        ActionListener<Void> refreshComplete = ActionListener.wrap(aVoid -> {
            lastUpdateTime = Instant.now();
            synchronized (fullRefreshCompletionListeners) {
                assert fullRefreshCompletionListeners.isEmpty() == false;
                for (ActionListener<Void> listener : fullRefreshCompletionListeners) {
                    listener.onResponse(null);
                }
                fullRefreshCompletionListeners.clear();
            }
        }, onCompletion::onFailure);

        // persistentTasks will be null if there's never been a persistent task created in this cluster
        if (persistentTasks == null) {
            refreshComplete.onResponse(null);
        } else {
            List<PersistentTasksCustomMetaData.PersistentTask<?>> mlJobTasks = persistentTasks.tasks().stream()
                .filter(task -> MlTasks.JOB_TASK_NAME.equals(task.getTaskName())).collect(Collectors.toList());
            iterateMlJobTasks(mlJobTasks.iterator(), refreshComplete);
        }
    }

    private void iterateMlJobTasks(Iterator<PersistentTasksCustomMetaData.PersistentTask<?>> iterator,
                                   ActionListener<Void> refreshComplete) {
        if (iterator.hasNext()) {
            OpenJobAction.JobParams jobParams = (OpenJobAction.JobParams) iterator.next().getParams();
            refreshJobMemory(jobParams.getJobId(),
                ActionListener.wrap(
                    // Do the next iteration in a different thread, otherwise stack overflow
                    // can occur if the searches happen to be on the local node, as the huge
                    // chain of listeners are all called in the same thread if only one node
                    // is involved
                    mem -> threadPool.executor(executorName()).execute(() -> iterateMlJobTasks(iterator, refreshComplete)),
                    refreshComplete::onFailure));
        } else {
            refreshComplete.onResponse(null);
        }
    }

    /**
     * Refresh the memory requirement for a single job.
     * This method only works on the master node.
     * @param jobId    The ID of the job to refresh the memory requirement for.
     * @param listener Receives the job's memory requirement, or <code>null</code>
     *                 if it cannot be calculated.
     */
    public void refreshJobMemory(String jobId, ActionListener<Long> listener) {
        if (isMaster == false) {
            listener.onResponse(null);
            return;
        }

        try {
            jobResultsProvider.getEstablishedMemoryUsage(jobId, null, null,
                establishedModelMemoryBytes -> {
                    if (establishedModelMemoryBytes <= 0L) {
                        setJobMemoryToLimit(jobId, listener);
                    } else {
                        Long memoryRequirementBytes = establishedModelMemoryBytes + Job.PROCESS_MEMORY_OVERHEAD.getBytes();
                        memoryRequirementByJob.put(jobId, memoryRequirementBytes);
                        listener.onResponse(memoryRequirementBytes);
                    }
                },
                e -> {
                    logger.error("[" + jobId + "] failed to calculate job established model memory requirement", e);
                    setJobMemoryToLimit(jobId, listener);
                }
            );
        } catch (Exception e) {
            logger.error("[" + jobId + "] failed to calculate job established model memory requirement", e);
            setJobMemoryToLimit(jobId, listener);
        }
    }

    private void setJobMemoryToLimit(String jobId, ActionListener<Long> listener) {
        jobManager.getJob(jobId, ActionListener.wrap(job -> {
            Long memoryLimitMb = job.getAnalysisLimits().getModelMemoryLimit();
            if (memoryLimitMb != null) {
                Long memoryRequirementBytes = ByteSizeUnit.MB.toBytes(memoryLimitMb) + Job.PROCESS_MEMORY_OVERHEAD.getBytes();
                memoryRequirementByJob.put(jobId, memoryRequirementBytes);
                listener.onResponse(memoryRequirementBytes);
            } else {
                memoryRequirementByJob.remove(jobId);
                listener.onResponse(null);
            }
        }, e -> {
            if (e instanceof ResourceNotFoundException) {
                // TODO: does this also happen if the .ml-config index exists but is unavailable?
                logger.trace("[{}] job deleted during ML memory update", jobId);
            } else {
                logger.error("[" + jobId + "] failed to get job during ML memory update", e);
            }
            memoryRequirementByJob.remove(jobId);
            listener.onResponse(null);
        }));
    }
}
