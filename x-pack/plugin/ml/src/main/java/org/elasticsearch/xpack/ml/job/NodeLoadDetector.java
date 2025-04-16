/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.utils.MemoryTrackedTaskState;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.MachineLearning.MACHINE_MEMORY_NODE_ATTR;

public class NodeLoadDetector {

    private static final Logger logger = LogManager.getLogger(NodeLoadDetector.class);

    private final MlMemoryTracker mlMemoryTracker;

    /**
     * Returns the node's total memory size.
     * @param node The node whose size to grab
     * @return maybe the answer, will be empty if size cannot be determined
     */
    public static OptionalLong getNodeSize(DiscoveryNode node) {
        String memoryString = node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR);
        try {
            return OptionalLong.of(Long.parseLong(memoryString));
        } catch (NumberFormatException e) {
            assert e == null : "ml.machine_memory should parse because we set it internally: invalid value was " + memoryString;
            return OptionalLong.empty();
        }
    }

    public NodeLoadDetector(MlMemoryTracker memoryTracker) {
        this.mlMemoryTracker = memoryTracker;
    }

    public MlMemoryTracker getMlMemoryTracker() {
        return mlMemoryTracker;
    }

    public NodeLoad detectNodeLoad(
        ClusterState clusterState,
        DiscoveryNode node,
        int dynamicMaxOpenJobs,
        int maxMachineMemoryPercent,
        boolean useAutoMachineMemoryCalculation
    ) {
        return detectNodeLoad(
            clusterState,
            TrainedModelAssignmentMetadata.fromState(clusterState),
            node,
            dynamicMaxOpenJobs,
            maxMachineMemoryPercent,
            useAutoMachineMemoryCalculation
        );
    }

    public NodeLoad detectNodeLoad(
        ClusterState clusterState,
        TrainedModelAssignmentMetadata assignmentMetadata,
        DiscoveryNode node,
        int maxNumberOfOpenJobs,
        int maxMachineMemoryPercent,
        boolean useAutoMachineMemoryCalculation
    ) {
        PersistentTasksCustomMetadata persistentTasks = clusterState.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        Map<String, String> nodeAttributes = node.getAttributes();
        List<String> errors = new ArrayList<>();
        OptionalLong maxMlMemory = NativeMemoryCalculator.allowedBytesForMl(node, maxMachineMemoryPercent, useAutoMachineMemoryCalculation);
        if (maxMlMemory.isEmpty()) {
            errors.add(
                MachineLearning.MACHINE_MEMORY_NODE_ATTR
                    + " attribute ["
                    + nodeAttributes.get(MachineLearning.MACHINE_MEMORY_NODE_ATTR)
                    + "] is not a long"
            );
        }

        NodeLoad.Builder nodeLoad = NodeLoad.builder(node.getId())
            .setMaxMemory(maxMlMemory.orElse(-1L))
            .setMaxJobs(maxNumberOfOpenJobs)
            .setUseMemory(true);
        if (errors.isEmpty() == false) {
            String errorMsg = Strings.collectionToCommaDelimitedString(errors);
            logger.warn("error detecting load for node [{}]: {}", node.getId(), errorMsg);
            return nodeLoad.setError(errorMsg).build();
        }
        updateLoadGivenTasks(nodeLoad, persistentTasks);
        updateLoadGivenModelAssignments(nodeLoad, assignmentMetadata);
        // if any processes are running then the native code will be loaded, but shared between all processes,
        // so increase the total memory usage to account for this
        if (nodeLoad.getNumAssignedJobs() > 0) {
            nodeLoad.incAssignedNativeCodeOverheadMemory(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes());
        }
        return nodeLoad.build();
    }

    private void updateLoadGivenTasks(NodeLoad.Builder nodeLoad, PersistentTasksCustomMetadata persistentTasks) {
        if (persistentTasks != null) {
            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> memoryTrackedTasks = findAllMemoryTrackedTasks(
                persistentTasks,
                nodeLoad.getNodeId()
            );
            for (PersistentTasksCustomMetadata.PersistentTask<?> task : memoryTrackedTasks) {
                MemoryTrackedTaskState state = MlTasks.getMemoryTrackedTaskState(task);
                assert state != null : "null MemoryTrackedTaskState for memory tracked task with params " + task.getParams();
                if (state != null && state.consumesMemory()) {
                    MlTaskParams taskParams = (MlTaskParams) task.getParams();
                    nodeLoad.addTask(task.getTaskName(), taskParams.getMlId(), state.isAllocating(), mlMemoryTracker);
                }
            }
        }
    }

    private static void updateLoadGivenModelAssignments(
        NodeLoad.Builder nodeLoad,
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata
    ) {
        if (trainedModelAssignmentMetadata != null && trainedModelAssignmentMetadata.allAssignments().isEmpty() == false) {
            for (TrainedModelAssignment assignment : trainedModelAssignmentMetadata.allAssignments().values()) {
                if (Optional.ofNullable(assignment.getNodeRoutingTable().get(nodeLoad.getNodeId()))
                    .map(RoutingInfo::getState)
                    .orElse(RoutingState.STOPPED)
                    .consumesMemory()) {
                    nodeLoad.incNumAssignedNativeInferenceModels();
                    nodeLoad.incAssignedNativeInferenceMemory(assignment.getTaskParams().estimateMemoryUsageBytes());
                }
            }
        }
    }

    private static Collection<PersistentTasksCustomMetadata.PersistentTask<?>> findAllMemoryTrackedTasks(
        PersistentTasksCustomMetadata persistentTasks,
        String nodeId
    ) {
        return persistentTasks.tasks()
            .stream()
            .filter(NodeLoadDetector::isMemoryTrackedTask)
            .filter(task -> nodeId.equals(task.getExecutorNode()))
            .collect(Collectors.toList());
    }

    private static boolean isMemoryTrackedTask(PersistentTasksCustomMetadata.PersistentTask<?> task) {
        return MlTasks.JOB_TASK_NAME.equals(task.getTaskName())
            || MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME.equals(task.getTaskName())
            || MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME.equals(task.getTaskName());
    }

}
