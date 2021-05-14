/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.snapshot.upgrader.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;


public class NodeLoadDetector {

    private final MlMemoryTracker mlMemoryTracker;

    public NodeLoadDetector(MlMemoryTracker memoryTracker) {
        this.mlMemoryTracker = memoryTracker;
    }

    public MlMemoryTracker getMlMemoryTracker() {
        return mlMemoryTracker;
    }

    public NodeLoad detectNodeLoad(ClusterState clusterState,
                                   boolean allNodesHaveDynamicMaxWorkers,
                                   DiscoveryNode node,
                                   int dynamicMaxOpenJobs,
                                   int maxMachineMemoryPercent,
                                   boolean useAutoMachineMemoryCalculation) {
        PersistentTasksCustomMetadata persistentTasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        Map<String, String> nodeAttributes = node.getAttributes();
        List<String> errors = new ArrayList<>();
        int maxNumberOfOpenJobs = dynamicMaxOpenJobs;
        // TODO: remove this in 8.0.0
        if (allNodesHaveDynamicMaxWorkers == false) {
            String maxNumberOfOpenJobsStr = nodeAttributes.get(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR);
            try {
                maxNumberOfOpenJobs = Integer.parseInt(maxNumberOfOpenJobsStr);
            } catch (NumberFormatException e) {
                errors.add(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR + " attribute [" + maxNumberOfOpenJobsStr + "] is not an integer");
                maxNumberOfOpenJobs = -1;
            }
        }
        OptionalLong maxMlMemory = NativeMemoryCalculator.allowedBytesForMl(node,
            maxMachineMemoryPercent,
            useAutoMachineMemoryCalculation);
        if (maxMlMemory.isEmpty()) {
            errors.add(MachineLearning.MACHINE_MEMORY_NODE_ATTR
                + " attribute ["
                + nodeAttributes.get(MachineLearning.MACHINE_MEMORY_NODE_ATTR)
                + "] is not a long");
        }

        NodeLoad.Builder nodeLoad = NodeLoad.builder(node.getId())
            .setMaxMemory(maxMlMemory.orElse(-1L))
            .setMaxJobs(maxNumberOfOpenJobs)
            .setUseMemory(true);
        if (errors.isEmpty() == false) {
            return nodeLoad.setError(Strings.collectionToCommaDelimitedString(errors)).build();
        }
        updateLoadGivenTasks(nodeLoad, persistentTasks);
        return nodeLoad.build();
    }

    private void updateLoadGivenTasks(NodeLoad.Builder nodeLoad, PersistentTasksCustomMetadata persistentTasks) {
        if (persistentTasks != null) {
            // find all the anomaly detector job tasks assigned to this node
            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> assignedAnomalyDetectorTasks = persistentTasks.findTasks(
                MlTasks.JOB_TASK_NAME, task -> nodeLoad.getNodeId().equals(task.getExecutorNode()));
            for (PersistentTasksCustomMetadata.PersistentTask<?> assignedTask : assignedAnomalyDetectorTasks) {
                JobState jobState = MlTasks.getJobStateModifiedForReassignments(assignedTask);
                OpenJobAction.JobParams params = (OpenJobAction.JobParams) assignedTask.getParams();
                nodeLoad.adjustForAnomalyJob(jobState, params == null ? null : params.getJobId(), mlMemoryTracker);
            }
            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> assignedShapshotUpgraderTasks = persistentTasks.findTasks(
                MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME, task -> nodeLoad.getNodeId().equals(task.getExecutorNode()));
            for (PersistentTasksCustomMetadata.PersistentTask<?> assignedTask : assignedShapshotUpgraderTasks) {
                SnapshotUpgradeTaskParams params = (SnapshotUpgradeTaskParams) assignedTask.getParams();
                nodeLoad.adjustForAnomalyJob(JobState.OPENED, params == null ? null : params.getJobId(), mlMemoryTracker);
            }

            // find all the data frame analytics job tasks assigned to this node
            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> assignedAnalyticsTasks = persistentTasks.findTasks(
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, task -> nodeLoad.getNodeId().equals(task.getExecutorNode()));
            for (PersistentTasksCustomMetadata.PersistentTask<?> assignedTask : assignedAnalyticsTasks) {
                nodeLoad.adjustForAnalyticsJob(assignedTask, mlMemoryTracker);
            }
            // if any jobs are running then the native code will be loaded, but shared between all jobs,
            // so increase the total memory usage of the assigned jobs to account for this
            if (nodeLoad.getNumAssignedJobs() > 0) {
                nodeLoad.incAssignedJobMemory(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes());
            }
        }
    }

}
