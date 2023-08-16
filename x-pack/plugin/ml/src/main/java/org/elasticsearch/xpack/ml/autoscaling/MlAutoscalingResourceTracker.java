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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.MlProcessors;
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
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

/**
 * backend for new kubernetes based autoscaler.
 */
public final class MlAutoscalingResourceTracker {
    private static final Logger logger = LogManager.getLogger(MlAutoscalingResourceTracker.class);

    record MlJobRequirements(long memory, int processors, int jobs) {
        static MlJobRequirements of(long memory, int processors, int jobs) {
            return new MlJobRequirements(memory, processors, jobs);
        }

        static MlJobRequirements of(long memory, int processors) {
            return new MlJobRequirements(memory, processors, 1);
        }
    };

    private MlAutoscalingResourceTracker() {}

    public static void getMlAutoscalingStats(
        ClusterState clusterState,
        ClusterSettings clusterSettings,
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

        // the next 2 values are only used iff > 0 and iff all nodes have the same container size
        long modelMemoryAvailableFirstNode = mlNodes.length > 0
            ? NativeMemoryCalculator.allowedBytesForMl(clusterState.nodes().get(mlNodes[0]), settings).orElse(0L)
            : 0L;
        int processorsAvailableFirstNode = mlNodes.length > 0
            ? MlProcessors.get(clusterState.nodes().get(mlNodes[0]), clusterSettings.get(MachineLearning.ALLOCATED_PROCESSORS_SCALE))
                .roundDown()
            : 0;

        // Todo: MAX_LOW_PRIORITY_MODELS_PER_NODE not checked yet
        int maxOpenJobsPerNode = MAX_OPEN_JOBS_PER_NODE.get(settings);

        getMlNodeStats(
            mlNodes,
            client,
            timeout,
            ActionListener.wrap(
                osStatsPerNode -> getMemoryAndProcessors(
                    new MlAutoscalingContext(clusterState),
                    mlMemoryTracker,
                    osStatsPerNode,
                    modelMemoryAvailableFirstNode,
                    processorsAvailableFirstNode,
                    maxOpenJobsPerNode,
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

    static void getMemoryAndProcessors(
        MlAutoscalingContext autoscalingContext,
        MlMemoryTracker mlMemoryTracker,
        Map<String, OsStats> osStatsPerNode,
        long perNodeAvailableModelMemoryInBytes,
        int perNodeAvailableProcessors,
        int maxOpenJobsPerNode,
        ActionListener<MlAutoscalingStats> listener
    ) {
        Map<String, List<MlJobRequirements>> perNodeModelMemoryInBytes = new HashMap<>();

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
        int processorsSum = 0;

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

                perNodeModelMemoryInBytes.computeIfAbsent(task.getExecutorNode(), k -> new ArrayList<>())
                    .add(MlJobRequirements.of(jobMemory, 0));
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
                perNodeModelMemoryInBytes.computeIfAbsent(task.getExecutorNode(), k -> new ArrayList<>())
                    .add(MlJobRequirements.of(jobMemory, 0));
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

                // if not low priority, check processor requirements
                if (Priority.LOW.equals(modelAssignment.getValue().getTaskParams().getPriority()) == false) {
                    // as assignments can be placed on different nodes, we only need numberOfThreadsPerAllocation here
                    extraSingleNodeProcessors = Math.max(extraSingleNodeProcessors, numberOfThreadsPerAllocation);
                    extraProcessors += numberOfAllocations * numberOfThreadsPerAllocation;
                }
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
                processorsSum += numberOfAllocations * numberOfThreadsPerAllocation;

                // min(3, max(number of allocations over all deployed models)
                minNodes = Math.min(3, Math.max(minNodes, numberOfAllocations));

                for (String node : modelAssignment.getValue().getNodeRoutingTable().keySet()) {
                    perNodeModelMemoryInBytes.computeIfAbsent(node, k -> new ArrayList<>())
                        .add(
                            MlJobRequirements.of(
                                estimatedMemoryUsage,
                                Priority.LOW.equals(modelAssignment.getValue().getTaskParams().getPriority())
                                    ? 0
                                    : numberOfThreadsPerAllocation
                            )
                        );
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
                || checkIfOneNodeCouldBeRemoved(
                    perNodeModelMemoryInBytes,
                    perNodeAvailableModelMemoryInBytes,
                    perNodeAvailableProcessors,
                    maxOpenJobsPerNode
                ))) {
            removeNodeMemoryInBytes = perNodeMemoryInBytes;
        }

        listener.onResponse(
            new MlAutoscalingStats(
                osStatsPerNode.size(),
                perNodeMemoryInBytes,
                modelMemoryBytesSum,
                processorsSum,
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
     * Check if one node can be removed by placing the jobs of the least loaded node to others.
     *
     * @param perNodeJobRequirements per Node lists of requirements
     * @param perNodeMemoryInBytes total model memory available on every node
     * @param maxOpenJobsPerNode the maximum number of jobs per node
     * @return true if a node can be removed, false if not
     */
    static boolean checkIfOneNodeCouldBeRemoved(
        Map<String, List<MlJobRequirements>> perNodeJobRequirements,
        long perNodeMemoryInBytes,
        int perNodeProcessors,
        int maxOpenJobsPerNode
    ) {
        if (perNodeJobRequirements.size() <= 1) {
            return false;
        }

        Map<String, MlJobRequirements> perNodeMlJobRequirementSum = perNodeJobRequirements.entrySet()
            .stream()
            .map(
                entry -> tuple(
                    entry.getKey(),
                    entry.getValue()
                        .stream()
                        .reduce(
                            MlJobRequirements.of(0L, 0, 0),
                            (subtotal, element) -> MlJobRequirements.of(
                                subtotal.memory + element.memory,
                                subtotal.processors + element.processors,
                                subtotal.jobs + element.jobs
                            )
                        )
                )
            )
            .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        // we define least loaded exclusively on memory, not CPU load
        Optional<Map.Entry<String, MlJobRequirements>> leastLoadedNodeAndMemoryUsage = perNodeMlJobRequirementSum.entrySet()
            .stream()
            .min(Comparator.comparingLong(entry -> entry.getValue().memory));

        if (leastLoadedNodeAndMemoryUsage.isPresent() == false) {
            return false;
        }

        assert leastLoadedNodeAndMemoryUsage.get().getValue().memory >= 0L;

        String candidateNode = leastLoadedNodeAndMemoryUsage.get().getKey();
        List<MlJobRequirements> candidateJobRequirements = perNodeJobRequirements.get(candidateNode);
        perNodeMlJobRequirementSum.remove(candidateNode);

        // if all jobs fit on other nodes, we can scale down one node
        return checkIfJobsCanBeMovedInLeastEfficientWay(
            candidateJobRequirements,
            perNodeMlJobRequirementSum,
            perNodeMemoryInBytes,
            perNodeProcessors,
            maxOpenJobsPerNode
        ) == 0L;
    }

    /**
     * Try to place jobs by size to other nodes in the least efficient way
     *
     * <p>Because the metric has no influence on how the jobs are placed in the end, it calculates the possibility of moving in the least
     * efficient way. That way we ensure that autoscaling is not looping between scaling down, up, down, up ... </p>
     *
     * @param candidateJobRequirements list of job requirements given running on the candidate node
     * @param perNodeMlJobRequirementsSum other nodes requirements
     * @param perNodeMemoryInBytes available memory per node
     * @return remaining memory, that could not be placed on other nodes, 0L if all jobs got placed
     */
    static long checkIfJobsCanBeMovedInLeastEfficientWay(
        List<MlJobRequirements> candidateJobRequirements,
        Map<String, MlJobRequirements> perNodeMlJobRequirementsSum,
        long perNodeMemoryInBytes,
        int perNodeProcessors,
        int maxOpenJobsPerNode
    ) {
        if (candidateJobRequirements.size() == 0) {
            return 0L;
        }

        // sort ascending so we put the smaller jobs first, by intention this might make it impossible to place the bigger jobs at the end
        List<MlJobRequirements> candidateNodeMemoryListSorted = candidateJobRequirements.stream()
            .sorted(Comparator.comparingLong(MlJobRequirements::memory))
            .toList();
        long candidateNodeMemorySum = candidateJobRequirements.stream().mapToLong(MlJobRequirements::memory).sum();

        if (perNodeMlJobRequirementsSum.size() == 0) {
            return candidateNodeMemorySum;
        }

        // use a priority queue that keeps the nodes with the lowest memory usage on top(all have the same size)
        // that way we assign the jobs to the node with most available memory first
        PriorityQueue<MlJobRequirements> nodesWithSpareCapacitySortedByMemory = perNodeMlJobRequirementsSum.values()
            .stream()
            .filter(e -> e.jobs < maxOpenJobsPerNode) // add to the queue only if enough space is available
            .collect(Collectors.toCollection(() -> new PriorityQueue<>(perNodeMlJobRequirementsSum.size(), (c1, c2) -> {
                if (c1.memory == c2.memory) {
                    return Integer.compare(c1.processors, c2.processors);
                }
                return Long.compare(c1.memory, c2.memory);
            })));

        for (MlJobRequirements jobRequirement : candidateNodeMemoryListSorted) {
            //
            assert jobRequirement.jobs == 1;

            if (jobRequirement.processors == 0) {
                MlJobRequirements nodeWithSpareCapacity = nodesWithSpareCapacitySortedByMemory.poll();
                long memoryAfterAddingJobMemory = nodeWithSpareCapacity.memory + jobRequirement.memory;
                if (memoryAfterAddingJobMemory <= perNodeMemoryInBytes) {
                    if (nodeWithSpareCapacity.jobs + jobRequirement.jobs < maxOpenJobsPerNode) {
                        nodesWithSpareCapacitySortedByMemory.add(
                            MlJobRequirements.of(
                                memoryAfterAddingJobMemory,
                                nodeWithSpareCapacity.processors,
                                nodeWithSpareCapacity.jobs + jobRequirement.jobs
                            )
                        );
                    }
                    candidateNodeMemorySum -= jobRequirement.memory;
                } else {
                    // not possible to place the job, downscaling is not possible
                    break;
                }
            } else {
                List<MlJobRequirements> stash = new ArrayList<>();
                boolean foundNodeThatCanTakeTheJob = false;
                while (nodesWithSpareCapacitySortedByMemory.isEmpty() == false) {
                    MlJobRequirements nodeWithSpareCapacity = nodesWithSpareCapacitySortedByMemory.poll();
                    long memoryAfterAddingJobMemory = nodeWithSpareCapacity.memory + jobRequirement.memory;
                    if (memoryAfterAddingJobMemory > perNodeMemoryInBytes) {
                        // not possible to place the job, downscaling is not possible
                        break;
                    }
                    if (nodeWithSpareCapacity.processors + jobRequirement.processors <= perNodeProcessors) {
                        if (nodeWithSpareCapacity.jobs + jobRequirement.jobs < maxOpenJobsPerNode) {
                            nodesWithSpareCapacitySortedByMemory.add(
                                MlJobRequirements.of(
                                    memoryAfterAddingJobMemory,
                                    nodeWithSpareCapacity.processors + jobRequirement.processors,
                                    nodeWithSpareCapacity.jobs + jobRequirement.jobs
                                )
                            );
                        }
                        candidateNodeMemorySum -= jobRequirement.memory;
                        foundNodeThatCanTakeTheJob = true;
                        break;
                    } else {
                        // the node fits regarding memory but not regarding spare processor capacity, stash it and check the next node
                        stash.add(nodeWithSpareCapacity);
                    }
                }
                if (foundNodeThatCanTakeTheJob == false) {
                    break;
                }

                // add the list of stashed items back to the queue
                nodesWithSpareCapacitySortedByMemory.addAll(stash);
            }

            // if nodes got removed due to maxOpenJobsPerNode the queue might be empty
            if (nodesWithSpareCapacitySortedByMemory.isEmpty()) {
                break;
            }
        }

        return candidateNodeMemorySum;
    }
}
