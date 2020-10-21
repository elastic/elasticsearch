/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public final class MlTasks {

    public static final String JOB_TASK_NAME = "xpack/ml/job";
    public static final String DATAFEED_TASK_NAME = "xpack/ml/datafeed";
    public static final String DATA_FRAME_ANALYTICS_TASK_NAME = "xpack/ml/data_frame/analytics";

    public static final String JOB_TASK_ID_PREFIX = "job-";
    public static final String DATAFEED_TASK_ID_PREFIX = "datafeed-";
    public static final String DATA_FRAME_ANALYTICS_TASK_ID_PREFIX = "data_frame_analytics-";

    public static final PersistentTasksCustomMetadata.Assignment AWAITING_UPGRADE =
        new PersistentTasksCustomMetadata.Assignment(null,
            "persistent task cannot be assigned while upgrade mode is enabled.");

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

    /**
     * Namespaces the task ids for data frame analytics.
     */
    public static String dataFrameAnalyticsTaskId(String id) {
        return DATA_FRAME_ANALYTICS_TASK_ID_PREFIX + id;
    }

    @Nullable
    public static PersistentTasksCustomMetadata.PersistentTask<?> getJobTask(String jobId, @Nullable PersistentTasksCustomMetadata tasks) {
        return tasks == null ? null : tasks.getTask(jobTaskId(jobId));
    }

    @Nullable
    public static PersistentTasksCustomMetadata.PersistentTask<?> getDatafeedTask(String datafeedId,
                                                                                  @Nullable PersistentTasksCustomMetadata tasks) {
        return tasks == null ? null : tasks.getTask(datafeedTaskId(datafeedId));
    }

    @Nullable
    public static PersistentTasksCustomMetadata.PersistentTask<?> getDataFrameAnalyticsTask(String analyticsId,
                                                                                            @Nullable PersistentTasksCustomMetadata tasks) {
        return tasks == null ? null : tasks.getTask(dataFrameAnalyticsTaskId(analyticsId));
    }

    /**
     * Note that the return value of this method does NOT take node relocations into account.
     * Use {@link #getJobStateModifiedForReassignments} to return a value adjusted to the most
     * appropriate value following relocations.
     */
    public static JobState getJobState(String jobId, @Nullable PersistentTasksCustomMetadata tasks) {
        PersistentTasksCustomMetadata.PersistentTask<?> task = getJobTask(jobId, tasks);
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

    public static JobState getJobStateModifiedForReassignments(String jobId, @Nullable PersistentTasksCustomMetadata tasks) {
        return getJobStateModifiedForReassignments(getJobTask(jobId, tasks));
    }

    public static JobState getJobStateModifiedForReassignments(@Nullable PersistentTasksCustomMetadata.PersistentTask<?> task) {
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

    public static DatafeedState getDatafeedState(String datafeedId, @Nullable PersistentTasksCustomMetadata tasks) {
        PersistentTasksCustomMetadata.PersistentTask<?> task = getDatafeedTask(datafeedId, tasks);
        if (task == null) {
            // If we haven't started a datafeed then there will be no persistent task,
            // which is the same as if the datafeed was't started
            return DatafeedState.STOPPED;
        }
        DatafeedState taskState = (DatafeedState) task.getState();
        if (taskState == null) {
            // If we haven't set a state yet then the task has never been assigned, so
            // report that it's starting
            return DatafeedState.STARTING;
        }
        return taskState;
    }

    public static DataFrameAnalyticsState getDataFrameAnalyticsState(String analyticsId, @Nullable PersistentTasksCustomMetadata tasks) {
        PersistentTasksCustomMetadata.PersistentTask<?> task = getDataFrameAnalyticsTask(analyticsId, tasks);
        return getDataFrameAnalyticsState(task);
    }

    public static DataFrameAnalyticsState getDataFrameAnalyticsState(@Nullable PersistentTasksCustomMetadata.PersistentTask<?> task) {
        if (task == null) {
            return DataFrameAnalyticsState.STOPPED;
        }
        DataFrameAnalyticsTaskState taskState = (DataFrameAnalyticsTaskState) task.getState();
        if (taskState == null) {
            return DataFrameAnalyticsState.STARTING;
        }

        DataFrameAnalyticsState state = taskState.getState();
        if (taskState.isStatusStale(task)) {
            if (state == DataFrameAnalyticsState.STOPPING) {
                // previous executor node failed while the job was stopping - it won't
                // be restarted on another node, so consider it STOPPED for reassignment purposes
                return DataFrameAnalyticsState.STOPPED;
            }
            if (state != DataFrameAnalyticsState.FAILED) {
                // we are relocating at the moment
                return DataFrameAnalyticsState.STARTING;
            }
        }
        return state;
    }

    /**
     * The job Ids of anomaly detector job tasks.
     * All anomaly detector jobs are returned regardless of the status of the
     * task (OPEN, CLOSED, FAILED etc).
     *
     * @param tasks Persistent tasks. If null an empty set is returned.
     * @return The job Ids of anomaly detector job tasks
     */
    public static Set<String> openJobIds(@Nullable PersistentTasksCustomMetadata tasks) {
        if (tasks == null) {
            return Collections.emptySet();
        }

        return tasks.findTasks(JOB_TASK_NAME, task -> true)
                .stream()
                .map(t -> t.getId().substring(JOB_TASK_ID_PREFIX.length()))
                .collect(Collectors.toSet());
    }

    /**
     * Get the job Ids of anomaly detector job tasks that do
     * not have an assignment.
     *
     * @param tasks Persistent tasks. If null an empty set is returned.
     * @param nodes The cluster nodes
     * @return The job Ids of tasks to do not have an assignment.
     */
    public static Set<String> unassignedJobIds(@Nullable PersistentTasksCustomMetadata tasks,
                                               DiscoveryNodes nodes) {
        return unassignedJobTasks(tasks, nodes).stream()
                .map(task -> task.getId().substring(JOB_TASK_ID_PREFIX.length()))
                .collect(Collectors.toSet());
    }

    /**
     * The job tasks that do not have an assignment as determined by
     * {@link PersistentTasksClusterService#needsReassignment(PersistentTasksCustomMetadata.Assignment, DiscoveryNodes)}
     *
     * @param tasks Persistent tasks. If null an empty set is returned.
     * @param nodes The cluster nodes
     * @return Unassigned job tasks
     */
    public static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> unassignedJobTasks(
            @Nullable PersistentTasksCustomMetadata tasks,
            DiscoveryNodes nodes) {
        if (tasks == null) {
            return Collections.emptyList();
        }

        return tasks.findTasks(JOB_TASK_NAME, task -> PersistentTasksClusterService.needsReassignment(task.getAssignment(), nodes));
    }

    /**
     * The datafeed Ids of started datafeed tasks
     *
     * @param tasks Persistent tasks. If null an empty set is returned.
     * @return The Ids of running datafeed tasks
     */
    public static Set<String> startedDatafeedIds(@Nullable PersistentTasksCustomMetadata tasks) {
        if (tasks == null) {
            return Collections.emptySet();
        }

        return tasks.findTasks(DATAFEED_TASK_NAME, task -> true)
                .stream()
                .map(t -> t.getId().substring(DATAFEED_TASK_ID_PREFIX.length()))
                .collect(Collectors.toSet());
    }

    /**
     * Get the datafeed Ids of started datafeed tasks
     * that do not have an assignment.
     *
     * @param tasks Persistent tasks. If null an empty set is returned.
     * @param nodes The cluster nodes
     * @return The job Ids of tasks to do not have an assignment.
     */
    public static Set<String> unassignedDatafeedIds(@Nullable PersistentTasksCustomMetadata tasks,
                                                    DiscoveryNodes nodes) {

        return unassignedDatafeedTasks(tasks, nodes).stream()
                .map(task -> task.getId().substring(DATAFEED_TASK_ID_PREFIX.length()))
                .collect(Collectors.toSet());
    }

    /**
     * The datafeed tasks that do not have an assignment as determined by
     * {@link PersistentTasksClusterService#needsReassignment(PersistentTasksCustomMetadata.Assignment, DiscoveryNodes)}
     *
     * @param tasks Persistent tasks. If null an empty set is returned.
     * @param nodes The cluster nodes
     * @return Unassigned datafeed tasks
     */
    public static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> unassignedDatafeedTasks(
            @Nullable PersistentTasksCustomMetadata tasks,
            DiscoveryNodes nodes) {
        if (tasks == null) {
            return Collections.emptyList();
        }

        return tasks.findTasks(DATAFEED_TASK_NAME, task -> PersistentTasksClusterService.needsReassignment(task.getAssignment(), nodes));
    }
}
