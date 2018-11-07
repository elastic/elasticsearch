/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class keeps track of the memory requirement of ML jobs.
 * It only functions on the master node - for this reason it should only be used by master node actions.
 * The memory requirement for open ML jobs is updated at the following times:
 * 1. When a master node is elected the memory requirement for all non-closed ML jobs is updated
 * 2. The memory requirement for all non-closed ML jobs is updated periodically thereafter - every 30 seconds by default
 * 3. When a job is opened the memory requirement for that single job is updated
 * As a result of this every open job should have a value for its memory requirement that is no more than 30 seconds out-of-date.
 */
public class MlMemoryTracker extends AbstractComponent implements LocalNodeMasterListener {

    private static final Long RECENT_UPDATE_THRESHOLD_NS = 30_000_000_000L; // 30 seconds

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final JobManager jobManager;
    private final JobResultsProvider jobResultsProvider;
    private final ConcurrentHashMap<String, Long> memoryRequirementByJob;
    private final List<ActionListener<Void>> fullRefreshCompletionListeners;
    private volatile boolean isMaster;
    private volatile Long lastUpdateNanotime;

    public MlMemoryTracker(Settings settings, ClusterService clusterService, ThreadPool threadPool, JobManager jobManager,
                           JobResultsProvider jobResultsProvider) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.jobManager = jobManager;
        this.jobResultsProvider = jobResultsProvider;
        memoryRequirementByJob = new ConcurrentHashMap<>();
        fullRefreshCompletionListeners = new ArrayList<>();
        clusterService.addLocalNodeMasterListener(this);
    }

    @Override
    public void onMaster() {
        isMaster = true;
        logger.trace("ML memory tracker on master");
    }

    @Override
    public void offMaster() {
        isMaster = false;
        memoryRequirementByJob.clear();
        lastUpdateNanotime = null;
    }

    @Override
    public String executorName() {
        return MachineLearning.UTILITY_THREAD_POOL_NAME;
    }

    /**
     * Is the information in this object sufficiently up to date
     * for valid allocation decisions to be made using it?
     */
    public boolean isRecentlyRefreshed() {
        Long localLastUpdateNanotime = lastUpdateNanotime;
        return localLastUpdateNanotime != null && System.nanoTime() - localLastUpdateNanotime < RECENT_UPDATE_THRESHOLD_NS;
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

        // Fallback for mixed version 6.6+/pre-6.6 cluster - TODO: remove in 7.0
        Job job = MlMetadata.getMlMetadata(clusterService.state()).getJobs().get(jobId);
        if (job != null) {
            return job.estimateMemoryFootprint();
        }

        return null;
    }

    public void removeJob(String jobId) {
        memoryRequirementByJob.remove(jobId);
    }

    /**
     * Uses a separate thread to refresh the memory requirement for every ML job that has
     * a corresponding persistent task.  This method only works on the master node.
     * @param listener Will be called when the async refresh completes or fails.
     * @return <code>true</code> if the async refresh is scheduled, and <code>false</code>
     *         if this is not possible for some reason.
     */
    public boolean asyncRefresh(ActionListener<Void> listener) {

        if (isMaster) {
            try {
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
            lastUpdateNanotime = System.nanoTime();
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
                ActionListener.wrap(mem -> iterateMlJobTasks(iterator, refreshComplete), refreshComplete::onFailure));
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
