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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
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

    public static final Setting<TimeValue> ML_MEMORY_UPDATE_FREQUENCY =
        Setting.timeSetting("xpack.ml.memory_update_frequency", TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(10),
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final JobManager jobManager;
    private final JobResultsProvider jobResultsProvider;
    private final ConcurrentHashMap<String, Long> memoryRequirementByJob;
    private volatile boolean isMaster;
    private volatile TimeValue updateFrequency;

    public MlMemoryTracker(Settings settings, ClusterService clusterService, ThreadPool threadPool, JobManager jobManager,
                           JobResultsProvider jobResultsProvider) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.jobManager = jobManager;
        this.jobResultsProvider = jobResultsProvider;
        this.updateFrequency = ML_MEMORY_UPDATE_FREQUENCY.get(settings);
        memoryRequirementByJob = new ConcurrentHashMap<>();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ML_MEMORY_UPDATE_FREQUENCY,
            this::setUpdateFrequency);
        clusterService.addLocalNodeMasterListener(this);
    }

    @Override
    public void onMaster() {
        isMaster = true;
        logger.trace("Elected master - scheduling ML memory update");
        try {
            // Submit a job that will start after updateFrequency, and reschedule itself after running
            threadPool.schedule(updateFrequency, executorName(), new SubmitReschedulingMlMemoryUpdate());
            threadPool.executor(executorName()).execute(
                () -> refresh(clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE)));
        } catch (EsRejectedExecutionException e) {
            logger.debug("Couldn't schedule ML memory update - node might be shutting down", e);
        }
    }

    @Override
    public void offMaster() {
        isMaster = false;
        memoryRequirementByJob.clear();
    }

    @Override
    public String executorName() {
        return MachineLearning.UTILITY_THREAD_POOL_NAME;
    }

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

    void setUpdateFrequency(TimeValue updateFrequency) {
        this.updateFrequency = updateFrequency;
    }

    /**
     * This refreshes the memory requirement for every ML job that has a corresponding persistent task.
     */
    void refresh(PersistentTasksCustomMetaData persistentTasks) {

        // persistentTasks will be null if there's never been a persistent task created in this cluster
        if (isMaster == false || persistentTasks == null) {
            return;
        }

        List<PersistentTasksCustomMetaData.PersistentTask<?>> mlJobTasks = persistentTasks.tasks().stream()
            .filter(task -> MlTasks.JOB_TASK_NAME.equals(task.getTaskName())).collect(Collectors.toList());
        for (PersistentTasksCustomMetaData.PersistentTask<?> mlJobTask : mlJobTasks) {
            OpenJobAction.JobParams jobParams = (OpenJobAction.JobParams) mlJobTask.getParams();
            refreshJobMemory(jobParams.getJobId(), mem -> {});
        }
    }

    /**
     * Refresh the memory requirement for a single job.
     * @param jobId The ID of the job to refresh the memory requirement for
     * @param listener A callback that will receive the memory requirement,
     *                 or <code>null</code> if it cannot be calculated
     */
    public void refreshJobMemory(String jobId, Consumer<Long> listener) {
        if (isMaster == false) {
            listener.accept(null);
            return;
        }

        jobResultsProvider.getEstablishedMemoryUsage(jobId, null, null, establishedModelMemoryBytes -> {
            if (establishedModelMemoryBytes <= 0L) {
                setJobMemoryToLimit(jobId, listener);
            } else {
                Long memoryRequirementBytes = establishedModelMemoryBytes + Job.PROCESS_MEMORY_OVERHEAD.getBytes();
                memoryRequirementByJob.put(jobId, memoryRequirementBytes);
                listener.accept(memoryRequirementBytes);
            }
        }, e -> {
            logger.error("[" + jobId + "] failed to calculate job established model memory requirement", e);
            setJobMemoryToLimit(jobId, listener);
        });
    }

    private void setJobMemoryToLimit(String jobId, Consumer<Long> listener) {
        jobManager.getJob(jobId, ActionListener.wrap(job -> {
            Long memoryLimitMb = job.getAnalysisLimits().getModelMemoryLimit();
            if (memoryLimitMb != null) {
                Long memoryRequirementBytes = ByteSizeUnit.MB.toBytes(memoryLimitMb) + Job.PROCESS_MEMORY_OVERHEAD.getBytes();
                memoryRequirementByJob.put(jobId, memoryRequirementBytes);
                listener.accept(memoryRequirementBytes);
            } else {
                memoryRequirementByJob.remove(jobId);
                listener.accept(null);
            }
        }, e -> {
            if (e instanceof ResourceNotFoundException) {
                // TODO: does this also happen if the .ml-config index exists but is unavailable?
                logger.trace("[{}] job deleted during ML memory update", jobId);
            } else {
                logger.error("[" + jobId + "] failed to get job during ML memory update", e);
            }
            memoryRequirementByJob.remove(jobId);
            listener.accept(null);
        }));
    }

    /**
     * Class used to submit {@link #refresh} on the {@link MlMemoryTracker} threadpool, these jobs will
     * reschedule themselves by placing a new instance of this class onto the scheduled threadpool.
     */
    private class SubmitReschedulingMlMemoryUpdate implements Runnable {
        @Override
        public void run() {
            try {
                threadPool.executor(executorName()).execute(() -> {
                    try {
                        refresh(clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
                    } finally {
                        // schedule again if still on master
                        if (isMaster) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Scheduling next run for updating ML memory in: {}", updateFrequency);
                            }
                            try {
                                threadPool.schedule(updateFrequency, executorName(), this);
                            } catch (EsRejectedExecutionException e) {
                                logger.debug("Couldn't schedule ML memory update - node might be shutting down", e);
                            }
                        }
                    }
                });
            } catch (EsRejectedExecutionException e) {
                logger.debug("Couldn't execute ML memory update - node might be shutting down", e);
            }
        }
    }
}
