/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a Generative AI
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.utils.MemoryTrackedTaskState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.MlProcessors;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.xpack.ml.MachineLearning.DUMMY_ENTITY_MEMORY;
import static org.elasticsearch.xpack.ml.MachineLearning.DUMMY_ENTITY_PROCESSORS;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

/**
 * backend for new kubernetes based autoscaler.
 */
public final class MlAutoscalingResourceTracker {
    private static final Logger logger = LogManager.getLogger(MlAutoscalingResourceTracker.class);

    /**
     * @param memory     (bytes)
     * @param processors (count)
     * @param jobs       (count)
     */
    record MlJobRequirements(long memory, int processors, int jobs) {
        static MlJobRequirements of(long memory, int processors, int jobs) {
            return new MlJobRequirements(memory, processors, jobs);
        }

        static MlJobRequirements of(long memory, int processors) {
            return new MlJobRequirements(memory, processors, 1);
        }
    }

    record MlDummyAutoscalingEntity(long memory, int processors) {
        static MlDummyAutoscalingEntity of(long memory, int processors) {
            return new MlDummyAutoscalingEntity(memory, processors);
        }
    }

    private MlAutoscalingResourceTracker() {}

    public static void getMlAutoscalingStats(
        ClusterState clusterState,
        ClusterSettings clusterSettings,
        MlMemoryTracker mlMemoryTracker,
        Settings settings,
        ActionListener<MlAutoscalingStats> listener
    ) {
        Map<String, Long> nodeSizeByMlNode = clusterState.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.ML_ROLE))
            .collect(Collectors.toMap(DiscoveryNode::getId, node -> NodeLoadDetector.getNodeSize(node).orElse(0L)));

        String firstMlNode = (nodeSizeByMlNode.size() > 0) ? nodeSizeByMlNode.keySet().iterator().next() : null;

        // the next 2 values are only used iff > 0 and iff all nodes have the same container size
        long modelMemoryAvailableFirstNode = (firstMlNode != null)
            ? NativeMemoryCalculator.allowedBytesForMl(clusterState.nodes().get(firstMlNode), settings).orElse(0L)
            : 0L;
        int processorsAvailableFirstNode = (firstMlNode != null)
            ? MlProcessors.get(clusterState.nodes().get(firstMlNode), clusterSettings.get(MachineLearning.ALLOCATED_PROCESSORS_SCALE))
                .roundUp()
            : 0;

        MlDummyAutoscalingEntity mlDummyAutoscalingEntity = new MlDummyAutoscalingEntity(
            // Treat a ByteSizeValue of -1 as 0, since 0 is the default dummy entity size
            Math.max(0L, DUMMY_ENTITY_MEMORY.get(settings).getBytes()),
            DUMMY_ENTITY_PROCESSORS.get(settings)
        );

        // Todo: MAX_LOW_PRIORITY_MODELS_PER_NODE not checked yet
        int maxOpenJobsPerNode = MAX_OPEN_JOBS_PER_NODE.get(settings);

        getMemoryAndProcessors(
            new MlAutoscalingContext(clusterState),
            mlMemoryTracker,
            nodeSizeByMlNode,
            modelMemoryAvailableFirstNode,
            processorsAvailableFirstNode,
            maxOpenJobsPerNode,
            mlDummyAutoscalingEntity,
            clusterSettings.get(MachineLearning.ALLOCATED_PROCESSORS_SCALE),
            listener
        );
    }

    static void getMemoryAndProcessors(
        MlAutoscalingContext autoscalingContext,
        MlMemoryTracker mlMemoryTracker,
        Map<String, Long> nodeSizeByMlNode,
        long perNodeAvailableModelMemoryBytes,
        int perNodeAvailableProcessors,
        int maxOpenJobsPerNode,
        MlDummyAutoscalingEntity dummyAutoscalingEntity,
        int allocatedProcessorsScale,
        ActionListener<MlAutoscalingStats> listener
    ) {

        Map<String, List<MlJobRequirements>> jobRequirementsByNode = new HashMap<>();

        int numberMlNodes = nodeSizeByMlNode.size();

        logger.debug(
            "getting ml resources, found [{}] ad jobs, [{}] dfa jobs and [{}] inference deployments",
            autoscalingContext.anomalyDetectionTasks.size(),
            autoscalingContext.dataframeAnalyticsTasks.size(),
            autoscalingContext.modelAssignments.size()
        );

        MlAutoscalingStats cumulative = new MlAutoscalingStats(
            // calculate the existing hardware values
            numberMlNodes,
            nodeSizeByMlNode.values().stream().mapToLong(Long::longValue).filter((l) -> l > 0).min().orElse(0L),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
        );

        // anomaly detection
        for (var task : autoscalingContext.anomalyDetectionTasks) {
            MemoryTrackedTaskState state = MlTasks.getMemoryTrackedTaskState(task);
            if (state != null && state.consumesMemory() == false) {
                continue;
            }

            String jobId = ((OpenJobAction.JobParams) task.getParams()).getJobId();
            Long jobMemory = mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId);
            if (jobMemory == null) {
                logger.debug("could not find memory requirement for job [{}], returning no-scale", jobId);
                listener.onResponse(noScaleStats(numberMlNodes));
                return;
            }
            if (AWAITING_LAZY_ASSIGNMENT.equals(task.getAssignment())) {
                logger.debug("job [{}] lacks assignment , memory required [{}]", jobId, jobMemory);

                // implementation decision: don't count processors for AD, if this gets a revisit, ensure to change it for the
                // old autoscaling, too
                cumulative = cumulative.accumulateWantedWith(jobMemory);
            } else {
                logger.debug("job [{}] assigned to [{}], memory required [{}]", jobId, task.getAssignment(), jobMemory);
                cumulative = cumulative.accumulateExistingOnly(jobMemory, 0);
                jobRequirementsByNode.computeIfAbsent(task.getExecutorNode(), k -> new ArrayList<>())
                    .add(MlJobRequirements.of(jobMemory, 0));
            }
        }

        // data frame analytics
        for (var task : autoscalingContext.dataframeAnalyticsTasks) {
            MemoryTrackedTaskState state = MlTasks.getMemoryTrackedTaskState(task);
            if (state != null && state.consumesMemory() == false) {
                continue;
            }

            String jobId = MlTasks.dataFrameAnalyticsId(task.getId());
            Long jobMemory = mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(jobId);
            if (jobMemory == null) {
                logger.debug("could not find memory requirement for job [{}], returning no-scale", jobId);
                listener.onResponse(noScaleStats(numberMlNodes));
                return;
            }

            if (AWAITING_LAZY_ASSIGNMENT.equals(task.getAssignment())) {
                logger.debug("dfa job [{}] lacks assignment , memory required [{}]", jobId, jobMemory);
                cumulative = cumulative.accumulateWantedWith(jobMemory);
            } else {
                logger.debug("dfa job [{}] assigned to [{}], memory required [{}]", jobId, task.getAssignment(), jobMemory);
                cumulative = cumulative.accumulateExistingOnly(jobMemory, 0);
                jobRequirementsByNode.computeIfAbsent(task.getExecutorNode(), k -> new ArrayList<>())
                    .add(MlJobRequirements.of(jobMemory, 0));
            }

        }

        // trained models
        Map<String, Integer> remainingProcessorsByNode = calculateRemainingProcessorsByNode(autoscalingContext, allocatedProcessorsScale);
        Map<String, Long> remainingMemoryByNode = calculateRemainingMemoryByNode(autoscalingContext);
        for (var assignment : autoscalingContext.modelAssignments.values()) {
            if (assignment.getTaskParams().getPriority() == Priority.LOW) {
                logger.debug("skipping low priority model [{}]", assignment.getModelId());
                continue;
            }

            accumulateJobRequirementsByNode(assignment, jobRequirementsByNode);
            int remainingProcessors = remainingProcessors(assignment);
            cumulative = cumulative.accumulateWanted(
                calculateTrainedModelsStats(assignment, remainingProcessorsByNode, remainingMemoryByNode)
            );
            if (remainingProcessors == 0) {
                logger.warn("deployment [{}] fully allocated", assignment.getDeploymentId());
            } else if (remainingProcessors > 0) {
                logger.warn("some allocations remaining for deployment [{}]", assignment.getDeploymentId());
            } else {
                logger.error(
                    "unexpected state for deployment [{}] remaining processors [{}]",
                    assignment.getDeploymentId(),
                    remainingProcessors
                );
            }
        }

        // dummy autoscaling entity
        if (dummyEntityFitsOnLeastLoadedNode(
            jobRequirementsByNode,
            perNodeAvailableModelMemoryBytes,
            perNodeAvailableProcessors,
            dummyAutoscalingEntity
        ) == false) {
            logger.debug(
                "Scaling up due to dummy entity: dummyEntityMemory: [{}], dummyEntityProcessors: [{}]",
                dummyAutoscalingEntity.memory,
                dummyAutoscalingEntity.processors
            );

            var stats = new MlAutoscalingStats(
                0,
                0,
                dummyAutoscalingEntity.memory,
                dummyAutoscalingEntity.processors,
                0,
                0,
                0,
                0,
                0,
                0,
                MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            cumulative = cumulative.accumulateWanted(stats);
        }

        // check for downscaling
        long removeNodeMemoryInBytes = 0;

        // only consider downscale if
        // - no scaling event is in progress
        // - modelMemory on nodes is available
        // - no jobs wait for assignment
        // - the total memory usage is less than memory usage after taking away 1 node
        // - the current number of nodes is greater than the minimum number of nodes
        if (cumulative.isUnwanted()
            && (jobRequirementsByNode.size() < numberMlNodes // a node has no assigned jobs
                || checkIfOneNodeCouldBeRemoved(
                    jobRequirementsByNode,
                    perNodeAvailableModelMemoryBytes,
                    perNodeAvailableProcessors,
                    maxOpenJobsPerNode,
                    dummyAutoscalingEntity
                ))) {
            cumulative = cumulative.accumulateUnwanted(
                new MlAutoscalingStats(
                    0, // current hardware
                    0, // current hardware
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    cumulative.currentPerNodeMemoryBytes(),
                    MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
                )
            );
        }

        listener.onResponse(cumulative);
    }

    private static void accumulateJobRequirementsByNode(
        TrainedModelAssignment assignment,
        Map<String, List<MlJobRequirements>> jobRequirementsByNode
    ) {
        for (String node : assignment.getNodeRoutingTable().keySet()) {
            jobRequirementsByNode.computeIfAbsent(node, k -> new ArrayList<>())
                .add(
                    MlJobRequirements.of(
                        assignment.getTaskParams().estimateMemoryUsageBytes(),
                        Priority.LOW.equals(assignment.getTaskParams().getPriority())
                            ? 0
                            : assignment.getTaskParams().getThreadsPerAllocation()
                    )
                );
        }
    }

    private static Map<String, Long> calculateRemainingMemoryByNode(MlAutoscalingContext autoscalingContext) {
        Map<String, Long> remainingMemoryByNode = new HashMap<>(autoscalingContext.mlNodes.size());
        for (var node : autoscalingContext.mlNodes) {
            remainingMemoryByNode.put(node.getId(), NodeLoadDetector.getNodeSize(node).orElse(0L));
        }
        return remainingMemoryByNode;
    }

    private static Map<String, Integer> calculateRemainingProcessorsByNode(
        MlAutoscalingContext autoscalingContext,
        int allocatedProcessorsScale
    ) {
        Map<String, Integer> remainingProcessorsByNode = new HashMap<>(autoscalingContext.mlNodes.size());
        for (var node : autoscalingContext.mlNodes) {
            remainingProcessorsByNode.put(node.getId(), MlProcessors.get(node, allocatedProcessorsScale).roundUp());
        }
        for (var assignment : autoscalingContext.modelAssignments.values()) {
            if (assignment.getTaskParams().getPriority() == Priority.LOW) {
                logger.debug("skipping low priority model in remainingProcessorsCalculation [{}]", assignment.getModelId());
                continue;
            }

            for (var nodeEntry : assignment.getNodeRoutingTable().entrySet()) {
                if (nodeEntry.getValue().getState().isAnyOf(RoutingState.STARTED, RoutingState.STARTING)) {
                    try {
                        int remainingProcessors = remainingProcessorsByNode.get(nodeEntry.getKey()) - nodeEntry.getValue()
                            .getTargetAllocations() * assignment.getTaskParams().getThreadsPerAllocation();
                        remainingProcessorsByNode.put(nodeEntry.getKey(), remainingProcessors);
                    } catch (NullPointerException e) {
                        logger.error("Node [{}] not found in remainingProcessorsByNode, [{}]", nodeEntry.getKey(), e);

                    }
                }
            }
        }
        return remainingProcessorsByNode;
    }

    private static int remainingProcessors(TrainedModelAssignment assignment) {
        int allocationsCount = assignment.getTaskParams().getNumberOfAllocations();
        int threadsCount = assignment.getTaskParams().getThreadsPerAllocation();

        return allocationsCount * threadsCount - countProcessors(assignment);
    }

    private static int countProcessors(TrainedModelAssignment assignment) {
        AtomicInteger startedProcessors = new AtomicInteger();
        assignment.getNodeRoutingTable().values().forEach(node -> {
            if (node.getState().isAnyOf(RoutingState.STARTED, RoutingState.STARTING)) {
                startedProcessors.addAndGet(node.getTargetAllocations() * assignment.getTaskParams().getThreadsPerAllocation());
            }
        });
        return startedProcessors.get();
    }

    private static MlAutoscalingStats calculateTrainedModelsStats(
        TrainedModelAssignment assignment,
        Map<String, Integer> remainingProcessorsByNode,
        Map<String, Long> remainingMemoryByNode
    ) {
        long estimatedMemoryUsageBytes = assignment.getTaskParams().estimateMemoryUsageBytes();
        int allocationsCount = assignment.getTaskParams().getNumberOfAllocations();
        int threadsCount = assignment.getTaskParams().getThreadsPerAllocation();
        int minNodes = calculateMinNodes(allocationsCount);
        int currentProcessors = countProcessors(assignment);
        int processorsRemaining = threadsCount * allocationsCount - currentProcessors;

        if (assignment.getAssignmentState().isAnyOf(AssignmentState.STARTING, AssignmentState.STARTED)) {
            if (processorsRemaining <= 0) {

                return alreadyFullyAllocated(estimatedMemoryUsageBytes, currentProcessors, minNodes);
            } else {
                for (var processorsEntry : remainingProcessorsByNode.entrySet().stream().sorted(Map.Entry.comparingByValue()).toList()) {
                    // try to fit on the fullest nodes first
                    if (processorsEntry.getValue() >= processorsRemaining
                        && remainingMemoryByNode.get(processorsEntry.getKey()) >= estimatedMemoryUsageBytes) {
                        remainingProcessorsByNode.put(processorsEntry.getKey(), processorsEntry.getValue() - processorsRemaining);
                        remainingMemoryByNode.put(
                            processorsEntry.getKey(),
                            remainingMemoryByNode.get(processorsEntry.getKey()) - estimatedMemoryUsageBytes
                        );

                        return fitsOnExistingNode(currentProcessors, estimatedMemoryUsageBytes, minNodes);
                    }
                }

                return requiresNewNodes(currentProcessors, estimatedMemoryUsageBytes, minNodes, threadsCount, processorsRemaining);
            }
        } else {

            return assignmentNotRunning();
        }
    }

    private static MlAutoscalingStats alreadyFullyAllocated(long estimatedMemoryUsageBytes, int currentProcessors, int minNodes) {
        return new MlAutoscalingStats(
            0, // current hardware
            0, // current hardware
            estimatedMemoryUsageBytes,
            currentProcessors,
            minNodes,
            0,
            0,
            0,
            0,
            0,
            MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
        );
    }

    private static MlAutoscalingStats assignmentNotRunning() {
        return alreadyFullyAllocated(0, 0, 0);
    }

    private static MlAutoscalingStats requiresNewNodes(
        int currentProcessors,
        long estimatedMemoryUsageBytes,
        int minNodes,
        int threadsCount,
        int processorsRemaining
    ) {
        return new MlAutoscalingStats(
            0, // current hardware
            0, // current hardware
            currentProcessors > 0 ? estimatedMemoryUsageBytes : 0, // some allocations may have already been assigned
            currentProcessors,
            minNodes,
            estimatedMemoryUsageBytes,
            threadsCount,
            estimatedMemoryUsageBytes,
            processorsRemaining,
            0,
            MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
        );
    }

    private static MlAutoscalingStats fitsOnExistingNode(int currentProcessors, long estimatedMemoryUsageBytes, int minNodes) {
        return new MlAutoscalingStats(
            0, // current hardware
            0, // current hardware
            currentProcessors > 0 ? estimatedMemoryUsageBytes : 0, // some allocations may have already been assigned
            currentProcessors,
            minNodes,
            0,
            0,
            0,
            0,
            0,
            MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
        );
    }

    private static int calculateMinNodes(int allocationsCount) {
        return Math.max(1, Math.min(3, allocationsCount));
    }

    /**
     * Check if the dummy autoscaling entity task can be added by placing
     * the task on the least loaded node.
     * <p>
     * If there exists a node that can accommodate the dummy entity then return true (nothing to do),
     * else return false and increment the memory and processor counts accordingly.
     * <p>
     * We perform the calculation by identifying the least loaded node in terms of memory
     * and determining if the addition of the dummy entity's memory and processor requirements could
     * be accommodated on it.
     * <p>
     * If the calculation returns false then treat the case as for a single trained model job
     * that is already assigned, i.e. increment modelMemoryBytesSum and currentTotalProcessorsInUse appropriately.
     *
     * @param perNodeJobRequirements per Node lists of requirements
     * @param perNodeMemoryInBytes   total model memory available on every node
     * @param perNodeProcessors      total processors on every node
     * @param dummyAutoscalingEntity "dummy" entity requirements used to potentially trigger a scaling event
     * @return true if the dummy entity can be accommodated, false if not
     */
    static boolean dummyEntityFitsOnLeastLoadedNode(
        Map<String, List<MlJobRequirements>> perNodeJobRequirements, // total up requirements...
        long perNodeMemoryInBytes,
        int perNodeProcessors,
        MlDummyAutoscalingEntity dummyAutoscalingEntity
    ) {

        if (dummyAutoscalingEntity.processors == 0 && dummyAutoscalingEntity.memory == 0L) {
            return true;
        }

        if (perNodeJobRequirements.isEmpty()) {
            return false;
        }

        // Note: we check least loaded based _only_ on memory...
        Optional<MlJobRequirements> leastLoadedNodeRequirements = perNodeJobRequirements.values()
            .stream()
            .map(
                value -> value.stream()
                    .reduce(
                        MlJobRequirements.of(0L, 0, 0),
                        (subtotal, element) -> MlJobRequirements.of(
                            subtotal.memory + element.memory,
                            subtotal.processors + element.processors,
                            subtotal.jobs + element.jobs
                        )
                    )
            )
            .min(Comparator.comparingLong(value -> value.memory));

        assert leastLoadedNodeRequirements.get().memory >= 0L;
        assert leastLoadedNodeRequirements.get().processors >= 0;

        // Check if the dummy entity could be accommodated
        if (leastLoadedNodeRequirements.get().memory + dummyAutoscalingEntity.memory > perNodeMemoryInBytes) {
            return false;
        }

        if (leastLoadedNodeRequirements.get().processors + dummyAutoscalingEntity.processors > perNodeProcessors) {
            return false;
        }

        return true;
    }

    /**
     * Return some autoscaling stats that tell the autoscaler not to change anything, but without making it think an error has occurred.
     */
    public static MlAutoscalingStats noScaleStats(ClusterState clusterState) {
        int numberMlNodes = (int) clusterState.nodes().stream().filter(node -> node.getRoles().contains(DiscoveryNodeRole.ML_ROLE)).count();
        return noScaleStats(numberMlNodes);
    }

    private static MlAutoscalingStats noScaleStats(int numberMlNodes) {
        return new MlAutoscalingStats(
            numberMlNodes,
            0,
            0,
            0,
            Math.min(3, numberMlNodes),
            0,
            0,
            0,
            0,
            0,
            MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
        );
    }

    /**
     * Check if one node can be removed by placing the jobs of the least loaded node to others.
     *
     * @param perNodeJobRequirements per Node lists of requirements
     * @param perNodeMemoryInBytes   total model memory available on every node
     * @param maxOpenJobsPerNode     the maximum number of jobs per node
     * @return true if a node can be removed, false if not
     */
    static boolean checkIfOneNodeCouldBeRemoved(
        Map<String, List<MlJobRequirements>> perNodeJobRequirements,
        long perNodeMemoryInBytes,
        int perNodeProcessors,
        int maxOpenJobsPerNode,
        MlDummyAutoscalingEntity dummyAutoscalingEntity
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
        if (dummyAutoscalingEntity.memory > 0L || dummyAutoscalingEntity.processors > 0) {
            candidateJobRequirements = new ArrayList<>(candidateJobRequirements);
            candidateJobRequirements.add(MlJobRequirements.of(dummyAutoscalingEntity.memory, dummyAutoscalingEntity.processors));
        }
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
     * @param candidateJobRequirements    list of job requirements given running on the candidate node
     * @param perNodeMlJobRequirementsSum other nodes requirements
     * @param perNodeMemoryInBytes        available memory per node
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
