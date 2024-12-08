/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.MlTasks.getDataFrameAnalyticsState;
import static org.elasticsearch.xpack.core.ml.MlTasks.getJobStateModifiedForReassignments;
import static org.elasticsearch.xpack.core.ml.MlTasks.getSnapshotUpgradeState;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

class MlAutoscalingContext {

    final Collection<PersistentTasksCustomMetadata.PersistentTask<?>> anomalyDetectionTasks;
    final Collection<PersistentTasksCustomMetadata.PersistentTask<?>> snapshotUpgradeTasks;
    final Collection<PersistentTasksCustomMetadata.PersistentTask<?>> dataframeAnalyticsTasks;
    final Map<String, TrainedModelAssignment> modelAssignments;

    final List<String> waitingAnomalyJobs;
    final List<String> waitingSnapshotUpgrades;
    final List<String> waitingAnalyticsJobs;
    final List<String> waitingAllocatedModels;

    final List<DiscoveryNode> mlNodes;
    final PersistentTasksCustomMetadata persistentTasks;

    MlAutoscalingContext() {
        this(List.of(), List.of(), List.of(), Map.of(), List.of(), null);
    }

    MlAutoscalingContext(
        final Collection<PersistentTasksCustomMetadata.PersistentTask<?>> anomalyDetectionTasks,
        final Collection<PersistentTasksCustomMetadata.PersistentTask<?>> snapshotUpgradeTasks,
        final Collection<PersistentTasksCustomMetadata.PersistentTask<?>> dataframeAnalyticsTasks,
        final Map<String, TrainedModelAssignment> modelAssignments,
        final List<DiscoveryNode> mlNodes,
        final PersistentTasksCustomMetadata persistentTasks
    ) {
        this.anomalyDetectionTasks = anomalyDetectionTasks;
        this.snapshotUpgradeTasks = snapshotUpgradeTasks;
        this.dataframeAnalyticsTasks = dataframeAnalyticsTasks;
        this.modelAssignments = modelAssignments;
        this.mlNodes = mlNodes;
        this.persistentTasks = persistentTasks;

        waitingAnomalyJobs = waitingAnomalyJobs(anomalyDetectionTasks);
        waitingSnapshotUpgrades = getWaitingSnapshotUpgrades(snapshotUpgradeTasks);
        waitingAnalyticsJobs = getWaitingAnalyticsJobs(dataframeAnalyticsTasks);
        waitingAllocatedModels = getWaitingAllocatedModels(modelAssignments);
    }

    MlAutoscalingContext(ClusterState clusterState) {
        persistentTasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);

        anomalyDetectionTasks = anomalyDetectionTasks(persistentTasks);
        snapshotUpgradeTasks = snapshotUpgradeTasks(persistentTasks);
        dataframeAnalyticsTasks = dataframeAnalyticsTasks(persistentTasks);
        modelAssignments = TrainedModelAssignmentMetadata.fromState(clusterState).allAssignments();

        waitingAnomalyJobs = waitingAnomalyJobs(anomalyDetectionTasks);
        waitingSnapshotUpgrades = getWaitingSnapshotUpgrades(snapshotUpgradeTasks);
        waitingAnalyticsJobs = getWaitingAnalyticsJobs(dataframeAnalyticsTasks);
        waitingAllocatedModels = getWaitingAllocatedModels(modelAssignments);

        mlNodes = getMlNodes(clusterState);
    }

    private static List<String> getWaitingAllocatedModels(Map<String, TrainedModelAssignment> modelAssignments) {
        return modelAssignments.entrySet()
            .stream()
            // TODO: Eventually care about those that are STARTED but not FULLY_ALLOCATED
            .filter(e -> e.getValue().getAssignmentState().equals(AssignmentState.STARTING) && e.getValue().getNodeRoutingTable().isEmpty())
            .map(Map.Entry::getKey)
            .toList();
    }

    private static List<String> getWaitingAnalyticsJobs(
        Collection<PersistentTasksCustomMetadata.PersistentTask<?>> dataframeAnalyticsTasks
    ) {
        return dataframeAnalyticsTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> ((StartDataFrameAnalyticsAction.TaskParams) t.getParams()).getId())
            .toList();
    }

    private static List<String> getWaitingSnapshotUpgrades(
        Collection<PersistentTasksCustomMetadata.PersistentTask<?>> snapshotUpgradeTasks
    ) {
        return snapshotUpgradeTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> ((SnapshotUpgradeTaskParams) t.getParams()).getJobId())
            .toList();
    }

    private static List<String> waitingAnomalyJobs(Collection<PersistentTasksCustomMetadata.PersistentTask<?>> anomalyDetectionTasks) {
        return anomalyDetectionTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> ((OpenJobAction.JobParams) t.getParams()).getJobId())
            .toList();
    }

    private static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> anomalyDetectionTasks(
        PersistentTasksCustomMetadata tasksCustomMetadata
    ) {
        if (tasksCustomMetadata == null) {
            return List.of();
        }

        return tasksCustomMetadata.findTasks(MlTasks.JOB_TASK_NAME, t -> taskStateFilter(getJobStateModifiedForReassignments(t)));
    }

    private static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> snapshotUpgradeTasks(
        PersistentTasksCustomMetadata tasksCustomMetadata
    ) {
        if (tasksCustomMetadata == null) {
            return List.of();
        }

        return tasksCustomMetadata.findTasks(MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME, t -> taskStateFilter(getSnapshotUpgradeState(t)));
    }

    static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> dataframeAnalyticsTasks(
        PersistentTasksCustomMetadata tasksCustomMetadata
    ) {
        if (tasksCustomMetadata == null) {
            return List.of();
        }

        return tasksCustomMetadata.findTasks(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, t -> taskStateFilter(getDataFrameAnalyticsState(t)));
    }

    private static boolean taskStateFilter(JobState jobState) {
        return jobState == null || jobState.isNoneOf(JobState.CLOSED, JobState.FAILED);
    }

    private static boolean taskStateFilter(SnapshotUpgradeState snapshotUpgradeState) {
        return snapshotUpgradeState == null || snapshotUpgradeState.isNoneOf(SnapshotUpgradeState.STOPPED, SnapshotUpgradeState.FAILED);
    }

    private static boolean taskStateFilter(DataFrameAnalyticsState dataFrameAnalyticsState) {
        // Don't count stopped and failed df-analytics tasks as they don't consume native memory
        return dataFrameAnalyticsState == null
            || dataFrameAnalyticsState.isNoneOf(DataFrameAnalyticsState.STOPPED, DataFrameAnalyticsState.FAILED);
    }

    public boolean hasWaitingTasks() {
        return waitingAnomalyJobs.isEmpty() == false
            || waitingSnapshotUpgrades.isEmpty() == false
            || waitingAnalyticsJobs.isEmpty() == false
            || waitingAllocatedModels.isEmpty() == false;
    }

    public boolean isEmpty() {
        return anomalyDetectionTasks.isEmpty()
            && snapshotUpgradeTasks.isEmpty()
            && dataframeAnalyticsTasks.isEmpty()
            && modelAssignments.values().stream().allMatch(assignment -> assignment.getTaskParams().getNumberOfAllocations() == 0);
    }

    public List<String> findPartiallyAllocatedModels() {
        return modelAssignments.entrySet()
            .stream()
            .filter(
                e -> e.getValue()
                    .calculateAllocationStatus()
                    .map(AllocationStatus::calculateState)
                    .orElse(AllocationStatus.State.FULLY_ALLOCATED)
                    .equals(AllocationStatus.State.FULLY_ALLOCATED) == false
            )
            .map(Map.Entry::getKey)
            .toList();
    }

    static List<DiscoveryNode> getMlNodes(final ClusterState clusterState) {
        return clusterState.nodes().mastersFirstStream().filter(MachineLearning::isMlNode).toList();
    }
}
