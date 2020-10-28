/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;

/**
 * Class that contains the logic to decide which node to assign each job to.
 *
 * The assignment rules are as follows:
 *
 * 1. Reject nodes that are not ML nodes
 * 2. Reject nodes for which the node filter returns a rejection reason
 * 3. Reject nodes where the new job would result in more than the permitted number of concurrent "opening" jobs
 * 4. Reject nodes where the new job would result in more than the permitted number of assigned jobs
 * 5. If assigning by memory, reject nodes where the new job would result in the permitted amount of memory being exceeded
 * 6. If assigning by memory, pick the node that remains after rejections that has the most remaining memory
 * 7. If assigning by count, pick the node that remains after rejections that has the fewest jobs assigned to it
 *
 * The decision on whether to assign by memory or by count is:
 * - If values are available for every node's memory size and every job's memory requirement then assign by memory
 * - Otherwise assign by count
 */
public class JobNodeSelector {

    public static final PersistentTasksCustomMetadata.Assignment AWAITING_LAZY_ASSIGNMENT =
        new PersistentTasksCustomMetadata.Assignment(null, "persistent task is awaiting node assignment.");

    private static final Logger logger = LogManager.getLogger(JobNodeSelector.class);

    private final String jobId;
    private final String taskName;
    private final ClusterState clusterState;
    private final MlMemoryTracker memoryTracker;
    private final Function<DiscoveryNode, String> nodeFilter;
    private final NodeLoadDetector nodeLoadDetector;
    private final int maxLazyNodes;

    /**
     * @param nodeFilter Optionally a function that returns a reason beyond the general
     *                   reasons why a job cannot be assigned to a particular node.  May
     *                   be <code>null</code> if no such function is needed.
     */
    public JobNodeSelector(ClusterState clusterState, String jobId, String taskName, MlMemoryTracker memoryTracker, int maxLazyNodes,
                           Function<DiscoveryNode, String> nodeFilter) {
        this.jobId = Objects.requireNonNull(jobId);
        this.taskName = Objects.requireNonNull(taskName);
        this.clusterState = Objects.requireNonNull(clusterState);
        this.memoryTracker = Objects.requireNonNull(memoryTracker);
        this.nodeLoadDetector = new NodeLoadDetector(Objects.requireNonNull(memoryTracker));
        this.maxLazyNodes = maxLazyNodes;
        this.nodeFilter = node -> {
            if (MachineLearning.isMlNode(node)) {
                return (nodeFilter != null) ? nodeFilter.apply(node) : null;
            }
            return "Not opening job [" + jobId + "] on node [" + nodeNameOrId(node) + "], because this node isn't a ml node.";
        };
    }

    public PersistentTasksCustomMetadata.Assignment selectNode(int dynamicMaxOpenJobs, int maxConcurrentJobAllocations,
                                                               int maxMachineMemoryPercent, boolean isMemoryTrackerRecentlyRefreshed,
                                                               boolean useAutoMemoryPercentage) {
        // Try to allocate jobs according to memory usage, but if that's not possible (maybe due to a mixed version cluster or maybe
        // because of some weird OS problem) then fall back to the old mechanism of only considering numbers of assigned jobs
        boolean allocateByMemory = isMemoryTrackerRecentlyRefreshed;
        if (isMemoryTrackerRecentlyRefreshed == false) {
            logger.warn("Falling back to allocating job [{}] by job counts because a memory requirement refresh could not be scheduled",
                jobId);
        }

        List<String> reasons = new LinkedList<>();
        long maxAvailableCount = Long.MIN_VALUE;
        long maxAvailableMemory = Long.MIN_VALUE;
        DiscoveryNode minLoadedNodeByCount = null;
        DiscoveryNode minLoadedNodeByMemory = null;
        for (DiscoveryNode node : clusterState.getNodes()) {

            // First check conditions that would rule out the node regardless of what other tasks are assigned to it
            String reason = nodeFilter.apply(node);
            if (reason != null) {
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            NodeLoadDetector.NodeLoad currentLoad = nodeLoadDetector.detectNodeLoad(
                clusterState,
                true, // Remove in 8.0.0
                node,
                dynamicMaxOpenJobs,
                maxMachineMemoryPercent,
                allocateByMemory,
                useAutoMemoryPercentage
            );
            if (currentLoad.getError() != null) {
                reason = "Not opening job [" + jobId + "] on node [" + nodeNameAndMlAttributes(node)
                    + "], because [" + currentLoad.getError() + "]";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            // Assuming the node is eligible at all, check loading
            allocateByMemory = currentLoad.isUseMemory();
            int maxNumberOfOpenJobs = currentLoad.getMaxJobs();

            if (currentLoad.getNumAllocatingJobs() >= maxConcurrentJobAllocations) {
                reason = "Not opening job ["
                    + jobId
                    + "] on node [" + nodeNameAndMlAttributes(node) + "], because node exceeds ["
                    + currentLoad.getNumAllocatingJobs()
                    + "] the maximum number of jobs [" + maxConcurrentJobAllocations
                    + "] in opening state";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            long availableCount = maxNumberOfOpenJobs - currentLoad.getNumAssignedJobs();
            if (availableCount == 0) {
                reason = "Not opening job [" + jobId + "] on node [" + nodeNameAndMlAttributes(node)
                    + "], because this node is full. Number of opened jobs [" + currentLoad.getNumAssignedJobs()
                    + "], " + MAX_OPEN_JOBS_PER_NODE.getKey() + " [" + maxNumberOfOpenJobs + "]";
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (maxAvailableCount < availableCount) {
                maxAvailableCount = availableCount;
                minLoadedNodeByCount = node;
            }

            if (allocateByMemory) {
                if (currentLoad.getMaxMlMemory() > 0) {
                    Long estimatedMemoryFootprint = memoryTracker.getJobMemoryRequirement(taskName, jobId);
                    if (estimatedMemoryFootprint != null) {
                        // If this will be the first job assigned to the node then it will need to
                        // load the native code shared libraries, so add the overhead for this
                        if (currentLoad.getNumAssignedJobs() == 0) {
                            estimatedMemoryFootprint += MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
                        }
                        long availableMemory = currentLoad.getMaxMlMemory() - currentLoad.getAssignedJobMemory();
                        if (estimatedMemoryFootprint > availableMemory) {
                            reason = "Not opening job [" + jobId + "] on node [" + nodeNameAndMlAttributes(node)
                                + "], because this node has insufficient available memory. Available memory for ML ["
                                + currentLoad.getMaxMlMemory()
                                + "], memory required by existing jobs [" + currentLoad.getAssignedJobMemory()
                                + "], estimated memory required for this job [" + estimatedMemoryFootprint + "]";
                            logger.trace(reason);
                            reasons.add(reason);
                            continue;
                        }

                        if (maxAvailableMemory < availableMemory) {
                            maxAvailableMemory = availableMemory;
                            minLoadedNodeByMemory = node;
                        }
                    } else {
                        // If we cannot get the job memory requirement,
                        // fall back to simply allocating by job count
                        allocateByMemory = false;
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "Falling back to allocating job [{}] by job counts because its memory requirement was not available",
                                jobId));
                    }
                } else {
                    // If we cannot get the available memory on any machine in
                    // the cluster, fall back to simply allocating by job count
                    allocateByMemory = false;
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "Falling back to allocating job [{}] by job counts because machine memory was not available for node [{}]",
                            jobId,
                            nodeNameAndMlAttributes(node)));
                }
            }
        }
        return createAssignment(allocateByMemory ? minLoadedNodeByMemory : minLoadedNodeByCount, reasons);
    }

    private PersistentTasksCustomMetadata.Assignment createAssignment(DiscoveryNode minLoadedNode, List<String> reasons) {
        if (minLoadedNode == null) {
            String explanation = String.join("|", reasons);
            logger.debug("no node selected for job [{}], reasons [{}]", jobId, explanation);
            return considerLazyAssignment(new PersistentTasksCustomMetadata.Assignment(null, explanation));
        }
        logger.debug("selected node [{}] for job [{}]", minLoadedNode, jobId);
        return new PersistentTasksCustomMetadata.Assignment(minLoadedNode.getId(), "");
    }

    PersistentTasksCustomMetadata.Assignment considerLazyAssignment(PersistentTasksCustomMetadata.Assignment currentAssignment) {

        assert currentAssignment.getExecutorNode() == null;

        int numMlNodes = 0;
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (MachineLearning.isMlNode(node)) {
                numMlNodes++;
            }
        }

        if (numMlNodes < maxLazyNodes) { // Means we have lazy nodes left to allocate
            return AWAITING_LAZY_ASSIGNMENT;
        }

        return currentAssignment;
    }

    static String nodeNameOrId(DiscoveryNode node) {
        String nodeNameOrID = node.getName();
        if (Strings.isNullOrEmpty(nodeNameOrID)) {
            nodeNameOrID = node.getId();
        }
        return nodeNameOrID;
    }

    public static String nodeNameAndVersion(DiscoveryNode node) {
        String nodeNameOrID = nodeNameOrId(node);
        StringBuilder builder = new StringBuilder("{").append(nodeNameOrID).append('}');
        builder.append('{').append("version=").append(node.getVersion()).append('}');
        return builder.toString();
    }

    static String nodeNameAndMlAttributes(DiscoveryNode node) {
        String nodeNameOrID = nodeNameOrId(node);

        StringBuilder builder = new StringBuilder("{").append(nodeNameOrID).append('}');
        for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
            if (entry.getKey().startsWith("ml.") || entry.getKey().equals("node.ml")) {
                builder.append('{').append(entry).append('}');
            }
        }
        return builder.toString();
    }

}
