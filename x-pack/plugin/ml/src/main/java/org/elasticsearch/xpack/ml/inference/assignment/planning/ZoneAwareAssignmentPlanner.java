/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * An assignment planner that is aware of availability zones and tries to distribute
 * model allocations evenly across zones in order to achieve better resilience in the
 * case nodes in a particular zone become unavailable.
 */
public class ZoneAwareAssignmentPlanner {

    private static final Logger logger = LogManager.getLogger(ZoneAwareAssignmentPlanner.class);

    /**
     * A map from zone attributes to node.
     */
    private final Map<List<String>, List<Node>> nodesByZone;

    private final List<AssignmentPlan.Deployment> deployments;

    public ZoneAwareAssignmentPlanner(Map<List<String>, List<Node>> nodesByZone, List<AssignmentPlan.Deployment> deployments) {
        this.nodesByZone = sortByZone(Objects.requireNonNull(nodesByZone));
        this.deployments = Objects.requireNonNull(deployments);
    }

    private static Map<List<String>, List<Node>> sortByZone(Map<List<String>, List<Node>> nodesByZone) {
        Map<List<String>, List<Node>> sortedByZone = new TreeMap<>(Comparator.comparing(zoneAttributes -> String.join("", zoneAttributes)));
        sortedByZone.putAll(nodesByZone);
        return sortedByZone;
    }

    public AssignmentPlan computePlan() {
        // There is only one zone; we can optimize and compute a plan directly.
        if (nodesByZone.size() == 1) {
            return new AssignmentPlanner(nodesByZone.values().iterator().next(), deployments).computePlan(true);
        }

        // First we try to compute a plan without forcing assigning previously assigned models as this may
        // produce better plans. If that plan has failed to assign previously assigned models we then try
        // again this time prioritizing assigning such models.
        AssignmentPlan plan = computePlan(false);
        if (plan.arePreviouslyAssignedModelsAssigned() == false) {
            plan = computePlan(true);
        }
        return plan;
    }

    private AssignmentPlan computePlan(boolean tryAssigningPreviouslyAssignedModels) {
        logger.debug(
            () -> format(
                "computing plan%s trying to assign previously assigned models",
                tryAssigningPreviouslyAssignedModels ? "" : " without"
            )
        );
        // The idea here is that we solve per zone trying to distribute allocations evenly.
        // After computing a plan for each zone it is possible that there are still unsatisfied allocations
        // that can be allocated, so we solve a final time across all zones preserving the allocations we
        // allocated on the first per zone assignment plans.

        int remainingZones = nodesByZone.size();
        Map<String, Integer> deploymentIdToRemainingAllocations = deployments.stream()
            .collect(Collectors.toMap(AssignmentPlan.Deployment::deploymentId, AssignmentPlan.Deployment::allocations));
        List<AssignmentPlan> plans = new ArrayList<>();
        for (var zoneToNodes : nodesByZone.entrySet()) {
            logger.debug(() -> format("computing plan for availability zone %s", zoneToNodes.getKey()));
            AssignmentPlan plan = computeZonePlan(
                zoneToNodes.getValue(),
                deploymentIdToRemainingAllocations,
                remainingZones,
                tryAssigningPreviouslyAssignedModels
            );
            plan.deployments()
                .forEach(
                    d -> deploymentIdToRemainingAllocations.computeIfPresent(
                        d.deploymentId(),
                        (deploymentId, remainingAllocations) -> remainingAllocations - plan.totalAllocations(d)
                    )
                );
            plans.add(plan);
            remainingZones--;
        }
        AssignmentPlan plan = computePlanAcrossAllNodes(plans);
        logger.debug(() -> "Zone aware plan =\n" + plan.prettyPrint());
        return plan;
    }

    private AssignmentPlan computeZonePlan(
        List<Node> nodes,
        Map<String, Integer> deploymentIdToRemainingAllocations,
        int remainingZones,
        boolean tryAssigningPreviouslyAssignedModels
    ) {
        Map<String, Integer> deploymentIdToTargetAllocationsPerZone = deploymentIdToRemainingAllocations.entrySet()
            .stream()
            .filter(e -> e.getValue() > 0)
            .collect(
                Collectors.toMap(Map.Entry::getKey, e -> 1 + remainingAllocationsPerZoneAfterAssigningOne(remainingZones, e.getValue()))
            );
        // If there was at least one allocation for a deployment, we will apply it to each zone

        List<AssignmentPlan.Deployment> modifiedDeployments = deployments.stream()
            .filter(d -> deploymentIdToTargetAllocationsPerZone.getOrDefault(d.deploymentId(), 0) > 0)
            // filter out deployments with no allocations
            .map(
                d -> new AssignmentPlan.Deployment(
                    // replace each deployment with a new deployment
                    d.deploymentId(),
                    d.memoryBytes(),
                    deploymentIdToTargetAllocationsPerZone.get(d.deploymentId()),
                    d.threadsPerAllocation(),
                    d.currentAllocationsByNodeId(),
                    // (below) Only force assigning at least once previously assigned models that have not had any allocation yet
                    (tryAssigningPreviouslyAssignedModels && deploymentIdToRemainingAllocations.get(d.deploymentId()) == d.allocations())
                        ? d.maxAssignedAllocations()
                        : 0,
                    d.getAdaptiveAllocationsSettings(),
                    d.perDeploymentMemoryBytes(),
                    d.perAllocationMemoryBytes()
                )
            )
            .toList();
        return new AssignmentPlanner(nodes, modifiedDeployments).computePlan(tryAssigningPreviouslyAssignedModels);
    }

    private static int remainingAllocationsPerZoneAfterAssigningOne(int remainingZones, Integer remainingAllocations) {
        if (remainingAllocations == null || remainingZones == 0) {
            // should never happen
            return 0;
        }
        return (remainingAllocations - 1) / remainingZones;
    }

    private AssignmentPlan computePlanAcrossAllNodes(List<AssignmentPlan> plans) {
        logger.debug(() -> "computing plan across all nodes");
        final List<Node> allNodes = new ArrayList<>();
        nodesByZone.values().forEach(allNodes::addAll);

        Map<String, Map<String, Integer>> allocationsByNodeIdByDeploymentId = mergeAllocationsByNodeIdByDeploymentId(plans);

        List<AssignmentPlan.Deployment> modelsAccountingPlans = deployments.stream()
            .map(
                d -> new AssignmentPlan.Deployment(
                    d.deploymentId(),
                    d.memoryBytes(),
                    d.allocations(),
                    d.threadsPerAllocation(),
                    allocationsByNodeIdByDeploymentId.get(d.deploymentId()),
                    d.maxAssignedAllocations(),
                    d.getAdaptiveAllocationsSettings(),
                    d.perDeploymentMemoryBytes(),
                    d.perAllocationMemoryBytes()
                )
            )
            .toList();

        PreserveAllAllocations preserveAllAllocations = new PreserveAllAllocations(allNodes, modelsAccountingPlans);
        List<Node> planNodes = preserveAllAllocations.nodesPreservingAllocations();
        List<AssignmentPlan.Deployment> planDeployments = preserveAllAllocations.modelsPreservingAllocations();
        AssignmentPlan plan = new LinearProgrammingPlanSolver(planNodes, planDeployments).solvePlan(false);
        plan = preserveAllAllocations.mergePreservedAllocations(plan);
        return swapOriginalModelsInPlan(plan, allNodes, modelsAccountingPlans);
    }

    private AssignmentPlan swapOriginalModelsInPlan(
        AssignmentPlan plan,
        List<Node> allNodes,
        List<AssignmentPlan.Deployment> planDeployments
    ) {
        final Map<String, AssignmentPlan.Deployment> originalModelById = deployments.stream()
            .collect(Collectors.toMap(AssignmentPlan.Deployment::deploymentId, Function.identity()));
        final Map<String, Node> originalNodeById = allNodes.stream().collect(Collectors.toMap(Node::id, Function.identity()));
        AssignmentPlan.Builder planBuilder = AssignmentPlan.builder(allNodes, deployments);
        for (AssignmentPlan.Deployment m : planDeployments) {
            AssignmentPlan.Deployment originalDeployment = originalModelById.get(m.deploymentId());
            Map<Node, Integer> nodeAssignments = plan.assignments(m).orElse(Map.of());
            for (Map.Entry<Node, Integer> assignment : nodeAssignments.entrySet()) {
                Node originalNode = originalNodeById.get(assignment.getKey().id());
                planBuilder.assignModelToNode(originalDeployment, originalNode, assignment.getValue());
                // As the node has all its available memory we need to manually account memory of models with
                // current allocations.
                planBuilder.accountMemory(originalDeployment, originalNode);
            }
        }
        return planBuilder.build();
    }

    private Map<String, Map<String, Integer>> mergeAllocationsByNodeIdByDeploymentId(List<AssignmentPlan> plans) {
        Map<String, Map<String, Integer>> allocationsByNodeIdByDeploymentId = new HashMap<>();
        deployments.forEach(d -> allocationsByNodeIdByDeploymentId.put(d.deploymentId(), new HashMap<>()));
        for (AssignmentPlan plan : plans) {
            for (AssignmentPlan.Deployment m : plan.deployments()) {
                Map<String, Integer> nodeIdToAllocations = allocationsByNodeIdByDeploymentId.get(m.deploymentId());
                Optional<Map<Node, Integer>> assignments = plan.assignments(m);
                if (assignments.isPresent()) {
                    for (Map.Entry<Node, Integer> nodeAssignments : assignments.get().entrySet()) {
                        nodeIdToAllocations.compute(
                            nodeAssignments.getKey().id(),
                            (nodeId, existingAllocations) -> existingAllocations == null
                                ? nodeAssignments.getValue()
                                : existingAllocations + nodeAssignments.getValue()
                        );
                    }
                }
            }
        }
        return allocationsByNodeIdByDeploymentId;
    }
}
