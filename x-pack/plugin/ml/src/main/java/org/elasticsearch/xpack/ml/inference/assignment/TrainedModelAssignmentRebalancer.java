/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.NodeAvailabilityZoneMapper;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan;
import org.elasticsearch.xpack.ml.inference.assignment.planning.ZoneAwareAssignmentPlanner;
import org.elasticsearch.xpack.ml.job.NodeLoad;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

class TrainedModelAssignmentRebalancer {

    private static final Logger logger = LogManager.getLogger(TrainedModelAssignmentRebalancer.class);

    private final TrainedModelAssignmentMetadata currentMetadata;
    private final Map<DiscoveryNode, NodeLoad> nodeLoads;
    private final NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper;
    private final Optional<StartTrainedModelDeploymentAction.TaskParams> modelToAdd;

    TrainedModelAssignmentRebalancer(
        TrainedModelAssignmentMetadata currentMetadata,
        Map<DiscoveryNode, NodeLoad> nodeLoads,
        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper,
        Optional<StartTrainedModelDeploymentAction.TaskParams> modelToAdd
    ) {
        this.currentMetadata = Objects.requireNonNull(currentMetadata);
        this.nodeLoads = Objects.requireNonNull(nodeLoads);
        this.nodeAvailabilityZoneMapper = Objects.requireNonNull(nodeAvailabilityZoneMapper);
        this.modelToAdd = Objects.requireNonNull(modelToAdd);
    }

    TrainedModelAssignmentMetadata.Builder rebalance() throws Exception {
        if (modelToAdd.isPresent() && currentMetadata.hasModel(modelToAdd.get().getModelId())) {
            throw new ResourceAlreadyExistsException("assignment for model with id [{}] already exists", modelToAdd.get().getModelId());
        }

        if (modelToAdd.isEmpty() && areAllModelsSatisfiedAndNoOutdatedRoutingEntries()) {
            logger.trace(() -> "No need to rebalance as all model deployments are satisfied");
            return TrainedModelAssignmentMetadata.Builder.fromMetadata(currentMetadata);
        }

        AssignmentPlan assignmentPlan = computeAssignmentPlan();
        return buildAssignmentsFromPlan(assignmentPlan);
    }

    private boolean areAllModelsSatisfiedAndNoOutdatedRoutingEntries() {
        Set<String> assignableNodeIds = nodeLoads.keySet().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        for (TrainedModelAssignment model : currentMetadata.modelAssignments().values()) {
            if (model.isSatisfied(assignableNodeIds) == false || model.hasOutdatedRoutingEntries()) {
                return false;
            }
        }
        return true;
    }

    AssignmentPlan computeAssignmentPlan() {
        final Map<List<String>, List<AssignmentPlan.Node>> nodesByZone = createNodesByZoneMap();

        final List<AssignmentPlan.Model> planModels = new ArrayList<>(
            currentMetadata.modelAssignments().size() + (modelToAdd.isPresent() ? 1 : 0)
        );
        final Set<String> assignableNodeIds = nodesByZone.values()
            .stream()
            .flatMap(List::stream)
            .map(AssignmentPlan.Node::id)
            .collect(Collectors.toSet());
        currentMetadata.modelAssignments().values().stream().map(assignment -> {
            Map<String, Integer> currentAssignments = assignment.getNodeRoutingTable()
                .entrySet()
                .stream()
                // Filter out nodes that are no longer assignable
                .filter(e -> assignableNodeIds.contains(e.getKey()))
                // Filter out allocation without current and target allocations as they are from before using the rebalancer
                .filter(e -> e.getValue().getCurrentAllocations() > 0 && e.getValue().getTargetAllocations() > 0)
                .filter(e -> e.getValue().getState().isAnyOf(RoutingState.STARTING, RoutingState.STARTED, RoutingState.FAILED))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getTargetAllocations()));
            return new AssignmentPlan.Model(
                assignment.getModelId(),
                assignment.getTaskParams().estimateMemoryUsageBytes(),
                assignment.getTaskParams().getNumberOfAllocations(),
                assignment.getTaskParams().getThreadsPerAllocation(),
                currentAssignments,
                assignment.getMaxAssignedAllocations()
            );
        }).forEach(planModels::add);
        modelToAdd.ifPresent(
            taskParams -> planModels.add(
                new AssignmentPlan.Model(
                    taskParams.getModelId(),
                    taskParams.estimateMemoryUsageBytes(),
                    taskParams.getNumberOfAllocations(),
                    taskParams.getThreadsPerAllocation(),
                    Map.of(),
                    0
                )
            )
        );
        return new ZoneAwareAssignmentPlanner(nodesByZone, planModels).computePlan();
    }

    private Map<List<String>, List<AssignmentPlan.Node>> createNodesByZoneMap() {
        Map<List<String>, Collection<DiscoveryNode>> mlNodesByZone = nodeAvailabilityZoneMapper.getMlNodesByAvailabilityZone();
        return mlNodesByZone.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
            Collection<DiscoveryNode> discoveryNodes = e.getValue();
            List<AssignmentPlan.Node> nodes = new ArrayList<>();
            for (DiscoveryNode discoveryNode : discoveryNodes) {
                if (nodeLoads.containsKey(discoveryNode)) {
                    NodeLoad load = nodeLoads.get(discoveryNode);
                    if (Strings.isNullOrEmpty(load.getError())) {
                        nodes.add(
                            new AssignmentPlan.Node(
                                discoveryNode.getId(),
                                // We subtract native inference memory as the planner expects available memory for
                                // native inference including current assignments.
                                getNodeFreeMemoryExcludingPerNodeOverheadAndNativeInference(load),
                                getNodeAllocatedProcessors(discoveryNode).roundUp()
                            )
                        );
                    } else {
                        logger.warn(
                            format("ignoring node [%s] as detecting its load failed with [%s]", discoveryNode.getId(), load.getError())
                        );
                    }
                } else {
                    logger.warn(format("ignoring node [%s] as no load could be detected", discoveryNode.getId()));
                }
            }
            return nodes;
        }));
    }

    private static Processors getNodeAllocatedProcessors(DiscoveryNode node) {
        String allocatedProcessorsString = node.getAttributes().get(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR);
        try {
            double allocatedProcessorsAsDouble = allocatedProcessorsString == null ? 0.0 : Double.parseDouble(allocatedProcessorsString);
            return allocatedProcessorsAsDouble > 0 ? Processors.of(allocatedProcessorsAsDouble) : Processors.ZERO;
        } catch (NumberFormatException e) {
            assert e == null
                : MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR
                    + " should parse because we set it internally: invalid value was "
                    + allocatedProcessorsString;
            return Processors.ZERO;
        }
    }

    private static long getNodeFreeMemoryExcludingPerNodeOverheadAndNativeInference(NodeLoad load) {
        return load.getFreeMemoryExcludingPerNodeOverhead() - load.getAssignedNativeInferenceMemory();
    }

    private TrainedModelAssignmentMetadata.Builder buildAssignmentsFromPlan(AssignmentPlan assignmentPlan) {
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.Builder.empty();
        for (AssignmentPlan.Model model : assignmentPlan.models()) {
            TrainedModelAssignment existingAssignment = currentMetadata.getModelAssignment(model.id());

            TrainedModelAssignment.Builder assignmentBuilder = TrainedModelAssignment.Builder.empty(
                existingAssignment == null && modelToAdd.isPresent()
                    ? modelToAdd.get()
                    : currentMetadata.getModelAssignment(model.id()).getTaskParams()
            );
            if (existingAssignment != null) {
                assignmentBuilder.setStartTime(existingAssignment.getStartTime());
                assignmentBuilder.setMaxAssignedAllocations(existingAssignment.getMaxAssignedAllocations());
            }

            Map<AssignmentPlan.Node, Integer> assignments = assignmentPlan.assignments(model).orElseGet(Map::of);
            for (Map.Entry<AssignmentPlan.Node, Integer> assignment : assignments.entrySet()) {
                if (existingAssignment != null && existingAssignment.isRoutedToNode(assignment.getKey().id())) {
                    RoutingInfo existingRoutingInfo = existingAssignment.getNodeRoutingTable().get(assignment.getKey().id());
                    RoutingState state = existingRoutingInfo.getState();
                    String reason = existingRoutingInfo.getReason();
                    if (state == RoutingState.FAILED) {
                        state = RoutingState.STARTING;
                        reason = "";
                    }
                    assignmentBuilder.addRoutingEntry(
                        assignment.getKey().id(),
                        new RoutingInfo(existingRoutingInfo.getCurrentAllocations(), assignment.getValue(), state, reason)
                    );
                } else {
                    assignmentBuilder.addRoutingEntry(
                        assignment.getKey().id(),
                        new RoutingInfo(assignment.getValue(), assignment.getValue(), RoutingState.STARTING, "")
                    );
                }
            }
            assignmentBuilder.calculateAndSetAssignmentState();

            explainAssignments(assignmentPlan, nodeLoads, model).ifPresent(assignmentBuilder::setReason);
            builder.addNewAssignment(model.id(), assignmentBuilder);
        }
        return builder;
    }

    private Optional<String> explainAssignments(
        AssignmentPlan assignmentPlan,
        Map<DiscoveryNode, NodeLoad> nodeLoads,
        AssignmentPlan.Model model
    ) {
        if (assignmentPlan.satisfiesAllocations(model)) {
            return Optional.empty();
        }

        if (nodeLoads.isEmpty()) {
            return Optional.of("No ML nodes exist in the cluster");
        }

        Map<String, String> nodeToReason = new TreeMap<>();
        for (Map.Entry<DiscoveryNode, NodeLoad> nodeAndLoad : nodeLoads.entrySet()) {
            Optional<String> reason = explainAssignment(assignmentPlan, nodeAndLoad.getKey(), nodeAndLoad.getValue(), model);
            reason.ifPresent(s -> nodeToReason.put(nodeAndLoad.getKey().getId(), s));
        }

        if (nodeToReason.isEmpty() == false) {
            return Optional.of(
                nodeToReason.entrySet()
                    .stream()
                    .map(entry -> format("Could not assign (more) allocations on node [%s]. Reason: %s", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining("|"))
            );
        }
        return Optional.empty();
    }

    private Optional<String> explainAssignment(
        AssignmentPlan assignmentPlan,
        DiscoveryNode node,
        NodeLoad load,
        AssignmentPlan.Model model
    ) {
        if (Strings.isNullOrEmpty(load.getError()) == false) {
            return Optional.of(load.getError());
        }

        if (model.memoryBytes() > assignmentPlan.getRemainingNodeMemory(node.getId())) {
            // If any ML processes are running on a node we require some space to load the shared libraries.
            // So if none are currently running then this per-node overhead must be added to the requirement.
            // From node load we know if we had any jobs or models assigned before the rebalance.
            // But we should also check if we managed to assign a model during the rebalance for which
            // we check if the node has used up any of its allocated processors.
            boolean isPerNodeOverheadAccountedFor = load.getNumAssignedJobsAndModels() > 0
                || assignmentPlan.getRemainingNodeCores(load.getNodeId()) < getNodeAllocatedProcessors(node).roundUp();
            long requiredMemory = model.memoryBytes() + (isPerNodeOverheadAccountedFor
                ? 0
                : MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes());
            long nodeFreeMemory = assignmentPlan.getRemainingNodeMemory(node.getId()) + (isPerNodeOverheadAccountedFor
                ? 0
                : MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes());
            return Optional.of(
                ParameterizedMessage.format(
                    "This node has insufficient available memory. Available memory for ML [{} ({})], "
                        + "free memory [{} ({})], "
                        + "estimated memory required for this model [{} ({})].",
                    new Object[] {
                        load.getMaxMlMemory(),
                        ByteSizeValue.ofBytes(load.getMaxMlMemory()).toString(),
                        nodeFreeMemory,
                        ByteSizeValue.ofBytes(nodeFreeMemory).toString(),
                        requiredMemory,
                        ByteSizeValue.ofBytes(requiredMemory).toString() }
                )
            );
        }

        if (model.threadsPerAllocation() > assignmentPlan.getRemainingNodeCores(node.getId())) {
            return Optional.of(
                ParameterizedMessage.format(
                    "This node has insufficient allocated processors. Available processors [{}], free processors [{}], "
                        + "processors required for each allocation of this model [{}]",
                    new Object[] {
                        getNodeAllocatedProcessors(node).roundUp(),
                        assignmentPlan.getRemainingNodeCores(node.getId()),
                        model.threadsPerAllocation() }
                )
            );
        }

        return Optional.empty();
    }
}
