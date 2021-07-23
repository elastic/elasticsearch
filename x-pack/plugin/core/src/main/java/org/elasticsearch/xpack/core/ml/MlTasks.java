/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;
import org.elasticsearch.xpack.core.ml.utils.MemoryTrackedTaskState;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public final class MlTasks {

    public static final String JOB_TASK_NAME = "xpack/ml/job";
    public static final String DATAFEED_TASK_NAME = "xpack/ml/datafeed";
    public static final String DATA_FRAME_ANALYTICS_TASK_NAME = "xpack/ml/data_frame/analytics";
    public static final String JOB_SNAPSHOT_UPGRADE_TASK_NAME = "xpack/ml/job/snapshot/upgrade";

    public static final String JOB_TASK_ID_PREFIX = "job-";
    public static final String DATAFEED_TASK_ID_PREFIX = "datafeed-";
    public static final String DATA_FRAME_ANALYTICS_TASK_ID_PREFIX = "data_frame_analytics-";
    public static final String JOB_SNAPSHOT_UPGRADE_TASK_ID_PREFIX = "job-snapshot-upgrade-";

    public static final PersistentTasksCustomMetadata.Assignment AWAITING_UPGRADE =
        new PersistentTasksCustomMetadata.Assignment(null,
            "persistent task cannot be assigned while upgrade mode is enabled.");
    public static final PersistentTasksCustomMetadata.Assignment RESET_IN_PROGRESS =
        new PersistentTasksCustomMetadata.Assignment(null,
            "persistent task will not be assigned as a feature reset is in progress.");

    // When a master node action is executed and there is no master node the transport will wait
    // for a new master node to be elected and retry against that, but will only wait as long as
    // the configured master node timeout. In ESS rolling upgrades can be very slow, and a cluster
    // might not have a master node for an extended period - over 1 minute was observed in the test
    // that led to this constant being added. The default master node timeout is 30 seconds, which
    // makes sense for user-invoked actions. But for actions invoked by persistent tasks that will
    // cause the persistent task to fail if they time out waiting for a master node we should wait
    // pretty much indefinitely, because failing the task just because a rolling upgrade was slow
    // defeats the point of the task being "persistent".
    public static final TimeValue PERSISTENT_TASK_MASTER_NODE_TIMEOUT = TimeValue.timeValueDays(365);

    private MlTasks() {
    }

    /**
     * Namespaces the task ids for jobs.
     * A datafeed id can be used as a job id, because they are stored separately in cluster state.
     */
    public static String jobTaskId(String jobId) {
        return JOB_TASK_ID_PREFIX + jobId;
    }

    public static String jobId(String jobTaskId) {
        return jobTaskId.substring(JOB_TASK_ID_PREFIX.length());
    }

    /**
     * Namespaces the task ids for datafeeds.
     * A job id can be used as a datafeed id, because they are stored separately in cluster state.
     */
    public static String datafeedTaskId(String datafeedId) {
        return DATAFEED_TASK_ID_PREFIX + datafeedId;
    }

    public static String snapshotUpgradeTaskId(String jobId, String snapshotId) {
        return JOB_SNAPSHOT_UPGRADE_TASK_ID_PREFIX + jobId + "-" + snapshotId;
    }

    /**
     * Namespaces the task ids for data frame analytics.
     */
    public static String dataFrameAnalyticsTaskId(String id) {
        return DATA_FRAME_ANALYTICS_TASK_ID_PREFIX + id;
    }

    public static String dataFrameAnalyticsId(String taskId) {
        return taskId.substring(DATA_FRAME_ANALYTICS_TASK_ID_PREFIX.length());
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

    @Nullable
    public static PersistentTasksCustomMetadata.PersistentTask<?> getSnapshotUpgraderTask(String jobId,
                                                                                          String snapshotId,
                                                                                          @Nullable PersistentTasksCustomMetadata tasks) {
        return tasks == null ? null : tasks.getTask(snapshotUpgradeTaskId(jobId, snapshotId));
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

        return openJobTasks(tasks)
                .stream()
                .map(t -> t.getId().substring(JOB_TASK_ID_PREFIX.length()))
                .collect(Collectors.toSet());
    }

    public static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> openJobTasks(@Nullable PersistentTasksCustomMetadata tasks) {
        if (tasks == null) {
            return Collections.emptyList();
        }

        return tasks.findTasks(JOB_TASK_NAME, task -> true);
    }

    public static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> datafeedTasksOnNode(
        @Nullable PersistentTasksCustomMetadata tasks, String nodeId) {
        if (tasks == null) {
            return Collections.emptyList();
        }

        return tasks.findTasks(DATAFEED_TASK_NAME, task -> nodeId.equals(task.getExecutorNode()));
    }

    public static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> jobTasksOnNode(
        @Nullable PersistentTasksCustomMetadata tasks, String nodeId) {
        if (tasks == null) {
            return Collections.emptyList();
        }

        return tasks.findTasks(JOB_TASK_NAME, task -> nodeId.equals(task.getExecutorNode()));
    }

    public static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> nonFailedJobTasksOnNode(
        @Nullable PersistentTasksCustomMetadata tasks, String nodeId) {
        if (tasks == null) {
            return Collections.emptyList();
        }

        return tasks.findTasks(JOB_TASK_NAME, task -> {
            if (nodeId.equals(task.getExecutorNode()) == false) {
                return false;
            }
            JobTaskState state = (JobTaskState) task.getState();
            if (state == null) {
                return true;
            }
            return state.getState() != JobState.FAILED;
        });
    }

    public static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> snapshotUpgradeTasksOnNode(
        @Nullable PersistentTasksCustomMetadata tasks, String nodeId) {
        if (tasks == null) {
            return Collections.emptyList();
        }

        return tasks.findTasks(JOB_SNAPSHOT_UPGRADE_TASK_NAME, task -> nodeId.equals(task.getExecutorNode()));
    }

    public static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> nonFailedSnapshotUpgradeTasksOnNode(
        @Nullable PersistentTasksCustomMetadata tasks, String nodeId) {
        if (tasks == null) {
            return Collections.emptyList();
        }

        return tasks.findTasks(JOB_SNAPSHOT_UPGRADE_TASK_NAME, task -> {
            if (nodeId.equals(task.getExecutorNode()) == false) {
                return false;
            }
            SnapshotUpgradeTaskState taskState = (SnapshotUpgradeTaskState) task.getState();
            if (taskState == null) {
                return true;
            }
            SnapshotUpgradeState state = taskState.getState();
            return state != SnapshotUpgradeState.FAILED;
        });
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

    public static MemoryTrackedTaskState getMemoryTrackedTaskState(PersistentTasksCustomMetadata.PersistentTask<?> task) {
        String taskName = task.getTaskName();
        switch (taskName) {
            case JOB_TASK_NAME:
                return getJobStateModifiedForReassignments(task);
            case JOB_SNAPSHOT_UPGRADE_TASK_NAME:
                SnapshotUpgradeTaskState taskState = (SnapshotUpgradeTaskState) task.getState();
                return taskState == null ? SnapshotUpgradeState.LOADING_OLD_STATE : taskState.getState();
            case DATA_FRAME_ANALYTICS_TASK_NAME:
                return getDataFrameAnalyticsState(task);
            default:
                throw new IllegalStateException("unexpected task type [" + task.getTaskName() + "]");
        }
    }
}
