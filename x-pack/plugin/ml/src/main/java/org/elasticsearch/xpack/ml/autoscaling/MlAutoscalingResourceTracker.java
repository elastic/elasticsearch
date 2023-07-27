/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

/**
 * backend for new kubernetes based autoscaler.
 */
public final class MlAutoscalingResourceTracker {
    private static final Logger logger = LogManager.getLogger(MlAutoscalingResourceTracker.class);

    private MlAutoscalingResourceTracker() {}

    public static void getMlAutoscalingStats(
        ClusterState clusterState,
        Client client,
        TimeValue timeout,
        MlMemoryTracker mlMemoryTracker,
        Settings settings,
        ActionListener<MlAutoscalingStats> listener
    ) {
        String[] mlNodes = clusterState.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.ML_ROLE))
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        long modelMemoryAvailableFirstNode = mlNodes.length > 0
            ? NativeMemoryCalculator.allowedBytesForMl(clusterState.nodes().get(mlNodes[0]), settings).orElse(0L)
            : 0L;

        getMlNodeStats(
            mlNodes,
            client,
            timeout,
            ActionListener.wrap(
                osStatsPerNode -> getMemoryAndCpu(
                    new MlAutoscalingContext(clusterState),
                    mlMemoryTracker,
                    osStatsPerNode,
                    modelMemoryAvailableFirstNode,
                    listener
                ),
                listener::onFailure
            )
        );
    }

    static void getMlNodeStats(String[] mlNodes, Client client, TimeValue timeout, ActionListener<Map<String, OsStats>> listener) {

        // if the client is configured with no nodes, it automatically calls all
        if (mlNodes.length == 0) {
            listener.onResponse(Collections.emptyMap());
            return;
        }

        client.admin()
            .cluster()
            .prepareNodesStats(mlNodes)
            .clear()
            .setOs(true)
            .setTimeout(timeout)
            .execute(
                ActionListener.wrap(
                    nodesStatsResponse -> listener.onResponse(
                        nodesStatsResponse.getNodes()
                            .stream()
                            .collect(Collectors.toMap(nodeStats -> nodeStats.getNode().getId(), NodeStats::getOs))
                    ),
                    listener::onFailure
                )
            );
    }

    static void getMemoryAndCpu(
        MlAutoscalingContext autoscalingContext,
        MlMemoryTracker mlMemoryTracker,
        Map<String, OsStats> osStatsPerNode,
        long perNodeAvailableModelMemoryInBytes,
        ActionListener<MlAutoscalingStats> listener
    ) {
        Map<String, List<Long>> perNodeModelMemoryInBytes = new HashMap<>();

        // If the ML nodes in the cluster have different sizes, return 0.
        // Otherwise, return the size, in bytes, of the container size of the ML nodes for a single container.
        long perNodeMemoryInBytes = osStatsPerNode.values()
            .stream()
            .map(s -> s.getMem().getAdjustedTotal().getBytes())
            .distinct()
            .count() != 1 ? 0 : osStatsPerNode.values().iterator().next().getMem().getAdjustedTotal().getBytes();

        long modelMemoryBytesSum = 0;
        long extraSingleNodeModelMemoryInBytes = 0;
        long extraModelMemoryInBytes = 0;
        int extraSingleNodeProcessors = 0;
        int extraProcessors = 0;

        logger.debug(
            "getting ml resources, found [{}] ad jobs, [{}] dfa jobs and [{}] inference deployments",
            autoscalingContext.anomalyDetectionTasks.size(),
            autoscalingContext.dataframeAnalyticsTasks.size(),
            autoscalingContext.modelAssignments.size()
        );

        // start with `minNodes = 1` if any ML job is started, further adjustments are made for trained models below
        int minNodes = autoscalingContext.anomalyDetectionTasks.isEmpty()
            && autoscalingContext.dataframeAnalyticsTasks.isEmpty()
            && autoscalingContext.modelAssignments.isEmpty() ? 0 : 1;

        // anomaly detection
        for (var task : autoscalingContext.anomalyDetectionTasks) {
            String jobId = ((OpenJobAction.JobParams) task.getParams()).getJobId();
            Long jobMemory = mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId);

            if (jobMemory == null) {
                // TODO: this indicates a bug, should we indicate that the result is incomplete?
                logger.debug("could not find memory requirement for job [{}], skipping", jobId);
                continue;
            }

            if (AWAITING_LAZY_ASSIGNMENT.equals(task.getAssignment())) {
                logger.debug("job [{}] lacks assignment , memory required [{}]", jobId, jobMemory);

                // implementation decision: don't count processors for AD, if this gets a revisit, ensure to change it for the
                // old autoscaling, too
                extraSingleNodeModelMemoryInBytes = Math.max(extraSingleNodeModelMemoryInBytes, jobMemory);
                extraModelMemoryInBytes += jobMemory;
            } else {
                logger.debug("job [{}] assigned to [{}], memory required [{}]", jobId, task.getAssignment(), jobMemory);

                modelMemoryBytesSum += jobMemory;

                perNodeModelMemoryInBytes.computeIfAbsent(task.getExecutorNode(), k -> new ArrayList<>()).add(jobMemory);
            }
        }

        // data frame analytics
        for (var task : autoscalingContext.dataframeAnalyticsTasks) {
            String jobId = MlTasks.dataFrameAnalyticsId(task.getId());
            Long jobMemory = mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(jobId);

            if (jobMemory == null) {
                // TODO: this indicates a bug, should we indicate that the result is incomplete?
                logger.debug("could not find memory requirement for job [{}], skipping", jobId);
                continue;
            }

            if (AWAITING_LAZY_ASSIGNMENT.equals(task.getAssignment())) {
                logger.debug("dfa job [{}] lacks assignment , memory required [{}]", jobId, jobMemory);

                // implementation decision: don't count processors for DFA, if this gets a revisit, ensure to change it for the
                // old autoscaling, too
                extraSingleNodeModelMemoryInBytes = Math.max(extraSingleNodeModelMemoryInBytes, jobMemory);
                extraModelMemoryInBytes += jobMemory;
            } else {
                logger.debug("dfa job [{}] assigned to [{}], memory required [{}]", jobId, task.getAssignment(), jobMemory);

                modelMemoryBytesSum += jobMemory;
                perNodeModelMemoryInBytes.computeIfAbsent(task.getExecutorNode(), k -> new ArrayList<>()).add(jobMemory);
            }
        }

        // trained models
        for (var modelAssignment : autoscalingContext.modelAssignments.entrySet()) {
            final int numberOfAllocations = modelAssignment.getValue().getTaskParams().getNumberOfAllocations();
            final int numberOfThreadsPerAllocation = modelAssignment.getValue().getTaskParams().getThreadsPerAllocation();
            final long estimatedMemoryUsage = modelAssignment.getValue().getTaskParams().estimateMemoryUsageBytes();

            if (AssignmentState.STARTING.equals(modelAssignment.getValue().getAssignmentState())
                && modelAssignment.getValue().getNodeRoutingTable().isEmpty()) {

                logger.debug(
                    () -> format(
                        "trained model [%s] lacks assignment , memory required [%d]",
                        modelAssignment.getKey(),
                        estimatedMemoryUsage
                    )
                );

                extraSingleNodeModelMemoryInBytes = Math.max(extraSingleNodeModelMemoryInBytes, estimatedMemoryUsage);
                extraModelMemoryInBytes += estimatedMemoryUsage;

                // as assignments can be placed on different nodes, we only need numberOfThreadsPerAllocation here
                extraSingleNodeProcessors = Math.max(extraSingleNodeProcessors, numberOfThreadsPerAllocation);
                extraProcessors += numberOfAllocations * numberOfThreadsPerAllocation;
            } else {
                logger.debug(
                    () -> format(
                        "trained model [%s] assigned to [%s], memory required [%d]",
                        modelAssignment.getKey(),
                        Strings.arrayToCommaDelimitedString(modelAssignment.getValue().getStartedNodes()),
                        estimatedMemoryUsage
                    )
                );

                modelMemoryBytesSum += estimatedMemoryUsage;

                // min(3, max(number of allocations over all deployed models)
                minNodes = Math.min(3, Math.max(minNodes, numberOfAllocations));

                for (String node : modelAssignment.getValue().getNodeRoutingTable().keySet()) {
                    perNodeModelMemoryInBytes.computeIfAbsent(node, k -> new ArrayList<>()).add(estimatedMemoryUsage);
                }
            }
        }

        // check for downscaling
        long removeNodeMemoryInBytes = 0;

        // only consider downscale if
        // - no scaling event is in progress
        // - modelMemory on nodes is available
        // - no jobs wait for assignment
        // - the total memory usage is less than memory usage after taking away 1 node
        if (perNodeMemoryInBytes > 0
            && perNodeAvailableModelMemoryInBytes > 0
            && extraModelMemoryInBytes == 0
            && extraProcessors == 0
            && modelMemoryBytesSum < perNodeMemoryInBytes * (osStatsPerNode.size() - 1)
            && (perNodeModelMemoryInBytes.size() < osStatsPerNode.size() // a node has no assigned jobs
                || tryRemoveOneNode(perNodeModelMemoryInBytes, perNodeAvailableModelMemoryInBytes))) {
            removeNodeMemoryInBytes = perNodeMemoryInBytes;
        }

        listener.onResponse(
            new MlAutoscalingStats(
                osStatsPerNode.size(),
                perNodeMemoryInBytes,
                modelMemoryBytesSum,
                minNodes,
                extraSingleNodeModelMemoryInBytes,
                extraSingleNodeProcessors,
                extraModelMemoryInBytes,
                extraProcessors,
                removeNodeMemoryInBytes,
                MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            )
        );
    }

    /**
     * Try to remove node memory
     *
     * @param perNodeUsedModelMemoryInBytes per Node lists of used model memory
     * @param perNodeMemoryInBytes total model memory available on every node
     * @return true if a node can be removed, false if not
     */
    static boolean tryRemoveOneNode(Map<String, List<Long>> perNodeUsedModelMemoryInBytes, long perNodeMemoryInBytes) {
        if (perNodeUsedModelMemoryInBytes.size() <= 1) {
            return false;
        }

        Map<String, Long> perNodeModelMemoryInBytesSum = perNodeUsedModelMemoryInBytes.entrySet()
            .stream()
            .map(entry -> tuple(entry.getKey(), entry.getValue().stream().mapToLong(Long::longValue).sum()))
            .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        Optional<Map.Entry<String, Long>> leastLoadedNodeAndMemoryUsage = perNodeModelMemoryInBytesSum.entrySet()
            .stream()
            .min(Comparator.comparingLong(Map.Entry::getValue));

        if (leastLoadedNodeAndMemoryUsage.isPresent() == false) {
            return false;
        }

        // this currently only works based on memory, not CPU
        String candidateNode = leastLoadedNodeAndMemoryUsage.get().getKey();
        List<Long> candidateNodeMemoryList = perNodeUsedModelMemoryInBytes.get(candidateNode);
        perNodeModelMemoryInBytesSum.remove(candidateNode);

        // if all jobs fit on other nodes, we can scale down one node
        return tryMoveJobsByMemoryInLeastEfficientWay(candidateNodeMemoryList, perNodeModelMemoryInBytesSum, perNodeMemoryInBytes) == 0L;
    }

    /**
     * Try to place jobs by size to other nodes in the leas efficient way
     *
     * @param candidateNodeMemoryList list of jobmemory values given by the candidate job
     * @param perNodeModelMemoryInBytesSum other nodes memory usage
     * @param perNodeMemoryInBytes available memory per node
     * @return remaining memory, that could not be placed on other nodes, 0L if all jobs got placed
     */
    static long tryMoveJobsByMemoryInLeastEfficientWay(
        List<Long> candidateNodeMemoryList,
        Map<String, Long> perNodeModelMemoryInBytesSum,
        long perNodeMemoryInBytes
    ) {
        if (candidateNodeMemoryList.size() == 0) {
            return 0L;
        }

        // sort ascending so we put the smaller jobs first, by intention this might make it impossible to place the bigger jobs at the end
        List<Long> candidateNodeMemoryListSorted = candidateNodeMemoryList.stream().sorted().toList();
        long candidateNodeMemorySum = candidateNodeMemoryList.stream().mapToLong(Long::longValue).sum();

        if (perNodeModelMemoryInBytesSum.size() == 0) {
            return candidateNodeMemorySum;
        }

        // use a priority queue that keeps the nodes with the lowest memory usage on top(all have the same size)
        // that way we assign the jobs to the node with most available memory first
        PriorityQueue<Long> modelMemoryInBytesSums = new PriorityQueue<>(perNodeModelMemoryInBytesSum.values());

        for (Long jobMemory : candidateNodeMemoryListSorted) {
            Long memoryAfterAddingJobMemory = modelMemoryInBytesSums.poll() + jobMemory;
            if (memoryAfterAddingJobMemory <= perNodeMemoryInBytes) {
                modelMemoryInBytesSums.add(memoryAfterAddingJobMemory);
                candidateNodeMemorySum -= jobMemory;
            } else {
                // not possible to place the job, downscaling is not possible
                break;
            }
        }

        return candidateNodeMemorySum;
    }
}
