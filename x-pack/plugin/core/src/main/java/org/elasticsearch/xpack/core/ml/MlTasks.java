/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class MlTasks {

    public static final String JOB_TASK_NAME = "xpack/ml/job";
    public static final String DATAFEED_TASK_NAME = "xpack/ml/datafeed";

    private static final String JOB_TASK_ID_PREFIX = "job-";
    private static final String DATAFEED_TASK_ID_PREFIX = "datafeed-";

    private MlTasks() {
    }

    /**
     * Namespaces the task ids for jobs.
     * A datafeed id can be used as a job id, because they are stored separately in cluster state.
     */
    public static String jobTaskId(String jobId) {
        return JOB_TASK_ID_PREFIX + jobId;
    }

    /**
     * Namespaces the task ids for datafeeds.
     * A job id can be used as a datafeed id, because they are stored separately in cluster state.
     */
    public static String datafeedTaskId(String datafeedId) {
        return DATAFEED_TASK_ID_PREFIX + datafeedId;
    }

    @Nullable
    public static PersistentTasksCustomMetaData.PersistentTask<?> getJobTask(String jobId, @Nullable PersistentTasksCustomMetaData tasks) {
        return tasks == null ? null : tasks.getTask(jobTaskId(jobId));
    }

    @Nullable
    public static PersistentTasksCustomMetaData.PersistentTask<?> getDatafeedTask(String datafeedId,
                                                                                  @Nullable PersistentTasksCustomMetaData tasks) {
        return tasks == null ? null : tasks.getTask(datafeedTaskId(datafeedId));
    }

    /**
     * Note that the return value of this method does NOT take node relocations into account.
     * Use {@link #getJobStateModifiedForReassignments} to return a value adjusted to the most
     * appropriate value following relocations.
     */
    public static JobState getJobState(String jobId, @Nullable PersistentTasksCustomMetaData tasks) {
        PersistentTasksCustomMetaData.PersistentTask<?> task = getJobTask(jobId, tasks);
        if (task != null) {
            JobTaskState jobTaskState = (JobTaskState) task.getState();
            if (jobTaskState == null) {
                return JobState.OPENING;
            }
            return jobTaskState.getState();
        }
        // If we haven't opened a job than there will be no persistent task, which is the same as if the job was closed
        return JobState.CLOSED;
    }

    public static JobState getJobStateModifiedForReassignments(String jobId, @Nullable PersistentTasksCustomMetaData tasks) {
        return getJobStateModifiedForReassignments(getJobTask(jobId, tasks));
    }

    public static JobState getJobStateModifiedForReassignments(@Nullable PersistentTasksCustomMetaData.PersistentTask<?> task) {
        if (task == null) {
            // A closed job has no persistent task
            return JobState.CLOSED;
        }
        JobTaskState jobTaskState = (JobTaskState) task.getState();
        if (jobTaskState == null) {
            return JobState.OPENING;
        }
        JobState jobState = jobTaskState.getState();
        if (jobTaskState.isStatusStale(task)) {
            // the job is re-locating
            if (jobState == JobState.CLOSING) {
                // previous executor node failed while the job was closing - it won't
                // be reopened on another node, so consider it CLOSED for most purposes
                return JobState.CLOSED;
            }
            if (jobState != JobState.FAILED) {
                // previous executor node failed and current executor node didn't
                // have the chance to set job status to OPENING
                return JobState.OPENING;
            }
        }
        return jobState;
    }

    public static DatafeedState getDatafeedState(String datafeedId, @Nullable PersistentTasksCustomMetaData tasks) {
        PersistentTasksCustomMetaData.PersistentTask<?> task = getDatafeedTask(datafeedId, tasks);
        if (task != null && task.getState() != null) {
            return (DatafeedState) task.getState();
        } else {
            // If we haven't started a datafeed then there will be no persistent task,
            // which is the same as if the datafeed was't started
            return DatafeedState.STOPPED;
        }
    }

    /**
     * The job Ids of anomaly detector job tasks.
     * All anomaly detector jobs are returned regardless of the status of the
     * task (OPEN, CLOSED, FAILED etc).
     *
     * @param tasks Persistent tasks. If null an empty set is returned.
     * @return The job Ids of anomaly detector job tasks
     */
    public static Set<String> openJobIds(@Nullable PersistentTasksCustomMetaData tasks) {
        if (tasks == null) {
            return Collections.emptySet();
        }

        return tasks.findTasks(JOB_TASK_NAME, task -> true)
                .stream()
                .map(t -> t.getId().substring(JOB_TASK_ID_PREFIX.length()))
                .collect(Collectors.toSet());
    }

    /**
     * The datafeed Ids of started datafeed tasks
     *
     * @param tasks Persistent tasks. If null an empty set is returned.
     * @return The Ids of running datafeed tasks
     */
    public static Set<String> startedDatafeedIds(@Nullable PersistentTasksCustomMetaData tasks) {
        if (tasks == null) {
            return Collections.emptySet();
        }

        return tasks.findTasks(DATAFEED_TASK_NAME, task -> true)
                .stream()
                .map(t -> t.getId().substring(DATAFEED_TASK_ID_PREFIX.length()))
                .collect(Collectors.toSet());
    }

    /**
     * Is there an ml anomaly detector job task for the job {@code jobId}?
     * @param jobId The job id
     * @param tasks Persistent tasks
     * @return True if the job has a task
     */
    public static boolean taskExistsForJob(String jobId, PersistentTasksCustomMetaData tasks) {
        return openJobIds(tasks).contains(jobId);
    }

    /**
     * Read the active anomaly detector job tasks.
     * Active tasks are not {@code JobState.CLOSED} or {@code JobState.FAILED}.
     *
     * @param tasks Persistent tasks
     * @return The job tasks excluding closed and failed jobs
     */
    public static List<PersistentTasksCustomMetaData.PersistentTask<?>> activeJobTasks(PersistentTasksCustomMetaData tasks) {
        return tasks.findTasks(JOB_TASK_NAME, task -> true)
                .stream()
                .filter(task -> ((JobTaskState) task.getState()).getState().isAnyOf(JobState.CLOSED, JobState.FAILED) == false)
                .collect(Collectors.toList());
    }
}
