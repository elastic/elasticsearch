/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingDeciderService;
import org.elasticsearch.xpack.ml.autoscaling.NativeMemoryCapacity;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private static String createReason(String job, String node, String msg, Object... params) {
        String preamble =  String.format(
            Locale.ROOT,
            "Not opening job [%s] on node [%s]. Reason: ",
            job,
            node);
        return preamble + ParameterizedMessage.format(msg, params);
    }
    private final String jobId;
    private final String taskName;
    private final ClusterState clusterState;
    private final Collection<DiscoveryNode> candidateNodes;
    private final MlMemoryTracker memoryTracker;
    private final Function<DiscoveryNode, String> nodeFilter;
    private final NodeLoadDetector nodeLoadDetector;
    private final int maxLazyNodes;

    /**
     * @param nodeFilter Optionally a function that returns a reason beyond the general
     *                   reasons why a job cannot be assigned to a particular node.  May
     *                   be <code>null</code> if no such function is needed.
     */
    public JobNodeSelector(ClusterState clusterState,
                           Collection<DiscoveryNode> candidateNodes,
                           String jobId,
                           String taskName,
                           MlMemoryTracker memoryTracker,
                           int maxLazyNodes,
                           Function<DiscoveryNode, String> nodeFilter) {
        this.jobId = Objects.requireNonNull(jobId);
        this.taskName = Objects.requireNonNull(taskName);
        this.clusterState = Objects.requireNonNull(clusterState);
        this.candidateNodes = Objects.requireNonNull(candidateNodes);
        this.memoryTracker = Objects.requireNonNull(memoryTracker);
        this.nodeLoadDetector = new NodeLoadDetector(Objects.requireNonNull(memoryTracker));
        this.maxLazyNodes = maxLazyNodes;
        this.nodeFilter = node -> {
            if (MachineLearning.isMlNode(node)) {
                return (nodeFilter != null) ? nodeFilter.apply(node) : null;
            }
            return createReason(jobId, nodeNameOrId(node), "This node isn't a machine learning node.");
        };
    }

    public Tuple<NativeMemoryCapacity, Long> perceivedCapacityAndMaxFreeMemory(int maxMachineMemoryPercent,
                                                                               boolean useAutoMemoryPercentage,
                                                                               int maxOpenJobs) {
        List<DiscoveryNode> capableNodes = candidateNodes.stream()
            .filter(n -> this.nodeFilter.apply(n) == null)
            .collect(Collectors.toList());
        NativeMemoryCapacity currentCapacityForMl = MlAutoscalingDeciderService.currentScale(
            capableNodes,
            maxMachineMemoryPercent,
            useAutoMemoryPercentage
        );
        long mostAvailableMemory = capableNodes.stream()
            .map(n -> nodeLoadDetector.detectNodeLoad(
                clusterState,
                true,
                n,
                maxOpenJobs,
                maxMachineMemoryPercent,
                useAutoMemoryPercentage)
            )
            .filter(nl -> nl.remainingJobs() > 0)
            .mapToLong(NodeLoad::getFreeMemory)
            .max()
            .orElse(0L);
        return Tuple.tuple(currentCapacityForMl, mostAvailableMemory);
    }

    public PersistentTasksCustomMetadata.Assignment selectNode(int dynamicMaxOpenJobs,
                                                               int maxConcurrentJobAllocations,
                                                               int maxMachineMemoryPercent,
                                                               long maxNodeSize,
                                                               boolean useAutoMemoryPercentage) {
        final Long estimatedMemoryFootprint = memoryTracker.getJobMemoryRequirement(taskName, jobId);
        return selectNode(
            estimatedMemoryFootprint,
            dynamicMaxOpenJobs,
            maxConcurrentJobAllocations,
            maxMachineMemoryPercent,
            maxNodeSize,
            useAutoMemoryPercentage
        );
    }

    public PersistentTasksCustomMetadata.Assignment selectNode(Long estimatedMemoryFootprint,
                                                               int dynamicMaxOpenJobs,
                                                               int maxConcurrentJobAllocations,
                                                               int maxMachineMemoryPercent,
                                                               long maxNodeSize,
                                                               boolean useAutoMemoryPercentage) {
        if (estimatedMemoryFootprint == null) {
            memoryTracker.asyncRefresh();
            String reason = "Not opening job [" + jobId + "] because job memory requirements are stale - refresh requested";
            logger.debug(reason);
            return new PersistentTasksCustomMetadata.Assignment(null, reason);
        }
        List<String> reasons = new LinkedList<>();
        long maxAvailableMemory = Long.MIN_VALUE;
        DiscoveryNode minLoadedNodeByMemory = null;
        long requiredMemoryForJob = estimatedMemoryFootprint;
        for (DiscoveryNode node : candidateNodes) {

            // First check conditions that would rule out the node regardless of what other tasks are assigned to it
            String reason = nodeFilter.apply(node);
            if (reason != null) {
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }
            NodeLoad currentLoad = nodeLoadDetector.detectNodeLoad(
                clusterState,
                true, // Remove in 8.0.0
                node,
                dynamicMaxOpenJobs,
                maxMachineMemoryPercent,
                useAutoMemoryPercentage
            );
            if (currentLoad.getError() != null) {
                reason = createReason(jobId, nodeNameAndMlAttributes(node), currentLoad.getError());
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }
            // Assuming the node is eligible at all, check loading
            boolean canAllocateByMemory = currentLoad.isUseMemory();
            int maxNumberOfOpenJobs = currentLoad.getMaxJobs();

            if (currentLoad.getNumAllocatingJobs() >= maxConcurrentJobAllocations) {
                reason = createReason(jobId,
                    nodeNameAndMlAttributes(node),
                    "Node exceeds [{}] the maximum number of jobs [{}] in opening state.",
                    currentLoad.getNumAllocatingJobs(),
                    maxConcurrentJobAllocations);
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (currentLoad.remainingJobs() == 0) {
                reason = createReason(jobId,
                    nodeNameAndMlAttributes(node),
                    "This node is full. Number of opened jobs [{}], {} [{}].",
                    currentLoad.getNumAssignedJobs(),
                    MAX_OPEN_JOBS_PER_NODE.getKey(),
                    maxNumberOfOpenJobs);
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (canAllocateByMemory == false) {
                reason = createReason(jobId,
                    nodeNameAndMlAttributes(node),
                    "This node is not providing accurate information to determine is load by memory.");
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (currentLoad.getMaxMlMemory() <= 0) {
                reason = createReason(jobId,
                    nodeNameAndMlAttributes(node),
                    "This node is indicating that it has no native memory for machine learning.");
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            // If this will be the first job assigned to the node then it will need to
            // load the native code shared libraries, so add the overhead for this
            if (currentLoad.getNumAssignedJobs() == 0) {
                requiredMemoryForJob += MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
            }
            long availableMemory = currentLoad.getMaxMlMemory() - currentLoad.getAssignedJobMemory();
            if (requiredMemoryForJob > availableMemory) {
                reason = createReason(jobId,
                    nodeNameAndMlAttributes(node),
                    "This node has insufficient available memory. Available memory for ML [{} ({})], "
                        + "memory required by existing jobs [{} ({})], "
                        + "estimated memory required for this job [{} ({})].",
                    currentLoad.getMaxMlMemory(),
                    ByteSizeValue.ofBytes(currentLoad.getMaxMlMemory()).toString(),
                    currentLoad.getAssignedJobMemory(),
                    ByteSizeValue.ofBytes(currentLoad.getAssignedJobMemory()).toString(),
                    requiredMemoryForJob,
                    ByteSizeValue.ofBytes(requiredMemoryForJob).toString());
                logger.trace(reason);
                reasons.add(reason);
                continue;
            }

            if (maxAvailableMemory < availableMemory) {
                maxAvailableMemory = availableMemory;
                minLoadedNodeByMemory = node;
            }
        }

        return createAssignment(
            estimatedMemoryFootprint,
            minLoadedNodeByMemory,
            reasons,
            maxNodeSize > 0L ?
                NativeMemoryCalculator.allowedBytesForMl(maxNodeSize, maxMachineMemoryPercent, useAutoMemoryPercentage) :
                Long.MAX_VALUE);
    }

    PersistentTasksCustomMetadata.Assignment createAssignment(long estimatedMemoryUsage,
                                                              DiscoveryNode minLoadedNode,
                                                              List<String> reasons,
                                                              long biggestPossibleJob) {
        if (minLoadedNode == null) {
            String explanation = String.join("|", reasons);
            PersistentTasksCustomMetadata.Assignment currentAssignment =
                new PersistentTasksCustomMetadata.Assignment(null, explanation);
            logger.debug("no node selected for job [{}], reasons [{}]", jobId, explanation);
            if ((MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes() + estimatedMemoryUsage) > biggestPossibleJob) {
                ParameterizedMessage message = new ParameterizedMessage(
                    "[{}] not waiting for node assignment as estimated job size [{}] is greater than largest possible job size [{}]",
                    jobId,
                    MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes() + estimatedMemoryUsage,
                    biggestPossibleJob);
                logger.info(message);
                List<String> newReasons = new ArrayList<>(reasons);
                newReasons.add(message.getFormattedMessage());
                explanation = String.join("|", newReasons);
                return new PersistentTasksCustomMetadata.Assignment(null, explanation);
            }
            return considerLazyAssignment(currentAssignment);
        }
        logger.debug("selected node [{}] for job [{}]", minLoadedNode, jobId);
        return new PersistentTasksCustomMetadata.Assignment(minLoadedNode.getId(), "");
    }

    PersistentTasksCustomMetadata.Assignment considerLazyAssignment(PersistentTasksCustomMetadata.Assignment currentAssignment) {

        assert currentAssignment.getExecutorNode() == null;

        int numMlNodes = 0;
        for (DiscoveryNode node : candidateNodes) {
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

    public static String nodeNameAndMlAttributes(DiscoveryNode node) {
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
