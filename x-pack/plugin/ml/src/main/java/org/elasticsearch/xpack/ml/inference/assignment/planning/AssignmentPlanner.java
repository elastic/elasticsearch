/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

/**
 * A planner that computes how allocations for model deployments will be
 * distributed across a set of nodes.
 *
 * Each model deployment requires a number of allocations. Each allocation
 * requires a number of threads. Also, each model has a memory cost that
 * we have to account for when it is assigned to a node.
 *
 * The planner computes an {@link AssignmentPlan} which describes which model
 * is assigned to which node and how many allocations each assignment takes.
 * It does so while respecting the constraints of memory and node CPU cores
 * in order to avoid thread over-subscription which leads to slowdowns.
 *
 * Furthermore, the planner preserves at least one allocation for all existing
 * assignments. This way, the new plan will only have new assignments and the
 * transition can happen with minimal impact on performance of started deployments.
 * However, if previously assigned model deployments do not receive any allocation,
 * then we attempt to find a solution that provides at least one allocation to
 * previously assigned model deployments.
 */
public class AssignmentPlanner {

    private static final Logger logger = LogManager.getLogger(AssignmentPlanner.class);

    private final List<Node> nodes;
    private final List<AssignmentPlan.Deployment> deployments;

    public AssignmentPlanner(List<Node> nodes, List<AssignmentPlan.Deployment> deployments) {
        this.nodes = nodes.stream().sorted(Comparator.comparing(Node::id)).toList();
        this.deployments = deployments.stream()
            .filter(deployment -> deployment.allocations() > 0)
            .sorted(Comparator.comparing(AssignmentPlan.Deployment::deploymentId))
            .toList();
    }

    public AssignmentPlan computePlan() {
        return computePlan(true);
    }

    public AssignmentPlan computePlan(boolean tryAssigningPreviouslyAssignedModels) {
        logger.debug(() -> format("Computing plan for nodes = %s; deployments = %s", nodes, deployments));

        AssignmentPlan bestPlan;
        AssignmentPlan planSatisfyingCurrentAssignments = solveSatisfyingCurrentAssignments();
        logger.debug(() -> "Plan satisfying current assignments =\n" + planSatisfyingCurrentAssignments.prettyPrint());
        if (planSatisfyingCurrentAssignments.arePreviouslyAssignedModelsAssigned() == false && tryAssigningPreviouslyAssignedModels) {
            AssignmentPlan planAllocatingAtLeastOnceModelsThatWerePreviouslyAllocated =
                solveAllocatingAtLeastOnceModelsThatWerePreviouslyAllocated();
            logger.debug(
                () -> "Plan with at least one allocation for previously assigned models =\n"
                    + planAllocatingAtLeastOnceModelsThatWerePreviouslyAllocated.prettyPrint()
            );
            if (planAllocatingAtLeastOnceModelsThatWerePreviouslyAllocated.arePreviouslyAssignedModelsAssigned()) {
                bestPlan = planAllocatingAtLeastOnceModelsThatWerePreviouslyAllocated;
            } else {
                bestPlan = planSatisfyingCurrentAssignments
                    .countPreviouslyAssignedModelsThatAreStillAssigned() >= planAllocatingAtLeastOnceModelsThatWerePreviouslyAllocated
                        .countPreviouslyAssignedModelsThatAreStillAssigned()
                            ? planSatisfyingCurrentAssignments
                            : planAllocatingAtLeastOnceModelsThatWerePreviouslyAllocated;
            }
        } else {
            bestPlan = planSatisfyingCurrentAssignments;
        }

        logger.debug(() -> "Best plan =\n" + bestPlan.prettyPrint());
        logger.debug(() -> prettyPrintOverallStats(bestPlan));
        return bestPlan;
    }

    private AssignmentPlan solveSatisfyingCurrentAssignments() {
        AssignmentPlan bestPlan;
        // First solve preserving one allocation per assignment because that is most flexible
        AssignmentPlan planKeepingOneAllocationOnCurrentAssignments = solveKeepingOneAllocationOnCurrentAssignments();
        if (planKeepingOneAllocationOnCurrentAssignments.satisfiesCurrentAssignments() == false) {
            bestPlan = solvePreservingAllAllocationsOnCurrentAssignments();
        } else if (planKeepingOneAllocationOnCurrentAssignments.satisfiesAllModels() == false) {
            AssignmentPlan planKeepingAllAllocationsOnCurrentAssignments = solvePreservingAllAllocationsOnCurrentAssignments();
            bestPlan = planKeepingAllAllocationsOnCurrentAssignments.compareTo(planKeepingOneAllocationOnCurrentAssignments) >= 0
                ? planKeepingAllAllocationsOnCurrentAssignments
                : planKeepingOneAllocationOnCurrentAssignments;
        } else {
            bestPlan = planKeepingOneAllocationOnCurrentAssignments;
        }
        return bestPlan;
    }

    private AssignmentPlan solveAllocatingAtLeastOnceModelsThatWerePreviouslyAllocated() {
        logger.debug(() -> "Attempting to solve assigning at least one allocation to previously assigned deployments");
        List<AssignmentPlan.Deployment> previouslyAssignedModelsOnly = deployments.stream()
            .filter(m -> m.hasEverBeenAllocated())
            .map(
                m -> new AssignmentPlan.Deployment(
                    m.deploymentId(),
                    m.memoryBytes(),
                    1,
                    m.threadsPerAllocation(),
                    // don't rely on the current allocation
                    new HashMap<>(),
                    m.maxAssignedAllocations(),
                    m.getAdaptiveAllocationsSettings(),
                    m.perDeploymentMemoryBytes(),
                    m.perAllocationMemoryBytes()
                )
            )
            .toList();
        AssignmentPlan planWithSingleAllocationForPreviouslyAssignedModels = new LinearProgrammingPlanSolver(
            nodes,
            previouslyAssignedModelsOnly
        ).solvePlan(true);

        Map<String, String> modelIdToNodeIdWithSingleAllocation = new HashMap<>();
        for (AssignmentPlan.Deployment m : planWithSingleAllocationForPreviouslyAssignedModels.deployments()) {
            Optional<Map<Node, Integer>> assignments = planWithSingleAllocationForPreviouslyAssignedModels.assignments(m);
            Set<Node> nodes = assignments.orElse(Map.of()).keySet();
            if (nodes.isEmpty() == false) {
                assert nodes.size() == 1;
                modelIdToNodeIdWithSingleAllocation.put(m.deploymentId(), nodes.iterator().next().id());
            }
        }

        List<AssignmentPlan.Deployment> planDeployments = deployments.stream().map(m -> {
            Map<String, Integer> currentAllocationsByNodeId = modelIdToNodeIdWithSingleAllocation.containsKey(m.deploymentId())
                ? Map.of(modelIdToNodeIdWithSingleAllocation.get(m.deploymentId()), 1)
                : Map.of();
            return new AssignmentPlan.Deployment(
                m.deploymentId(),
                m.memoryBytes(),
                m.allocations(),
                m.threadsPerAllocation(),
                currentAllocationsByNodeId,
                m.maxAssignedAllocations(),
                m.getAdaptiveAllocationsSettings(),
                m.perDeploymentMemoryBytes(),
                m.perAllocationMemoryBytes()
            );
        }).toList();

        return new AssignmentPlanner(nodes, planDeployments).computePlan(false);
    }

    private AssignmentPlan solveKeepingOneAllocationOnCurrentAssignments() {
        // We do not want to ever completely unassign a model from a node so we
        // can move allocations without having temporary impact on performance.
        logger.trace(() -> format("Solving preserving one allocation on current assignments"));
        return solvePreservingCurrentAssignments(new PreserveOneAllocation(nodes, deployments));
    }

    private AssignmentPlan solvePreservingAllAllocationsOnCurrentAssignments() {
        logger.trace(() -> format("Solving preserving all allocations on current assignments"));
        return solvePreservingCurrentAssignments(new PreserveAllAllocations(nodes, deployments));
    }

    private static AssignmentPlan solvePreservingCurrentAssignments(AbstractPreserveAllocations preserveAllocations) {
        List<Node> planNodes = preserveAllocations.nodesPreservingAllocations();
        List<AssignmentPlan.Deployment> planDeployments = preserveAllocations.modelsPreservingAllocations();
        logger.trace(() -> format("Nodes after applying allocation preserving strategy = %s", planNodes));
        logger.trace(() -> format("Deployments after applying allocation preserving strategy = %s", planDeployments));
        AssignmentPlan assignmentPlan = new LinearProgrammingPlanSolver(planNodes, planDeployments).solvePlan(false);
        return preserveAllocations.mergePreservedAllocations(assignmentPlan);
    }

    private String prettyPrintOverallStats(AssignmentPlan assignmentPlan) {
        int totalAllocationsRequired = 0;
        int totalAllocationsAssigned = 0;
        int totalCoresUsed = 0;
        long totalAvailableMem = nodes.stream().map(Node::availableMemoryBytes).mapToLong(Long::longValue).sum();
        int totalCores = nodes.stream().map(Node::cores).mapToInt(Integer::intValue).sum();
        long totalUsedMem = 0;
        for (AssignmentPlan.Deployment m : deployments) {
            totalAllocationsRequired += m.allocations();
            if (assignmentPlan.assignments(m).isPresent()) {
                int allocations = assignmentPlan.assignments(m).get().values().stream().mapToInt(Integer::intValue).sum();
                totalAllocationsAssigned += allocations;
                totalCoresUsed += allocations * m.threadsPerAllocation();
                totalUsedMem += m.memoryBytes() * assignmentPlan.assignments(m).get().values().size();
            }
        }
        StringBuilder msg = new StringBuilder("Overall Stats: ");
        msg.append("(used memory = ");
        msg.append(ByteSizeValue.ofBytes(totalUsedMem));
        msg.append(") (total available memory = ");
        msg.append(ByteSizeValue.ofBytes(totalAvailableMem));
        msg.append(") (allocations = ");
        msg.append(totalAllocationsAssigned);
        msg.append("/");
        msg.append(totalAllocationsRequired);
        msg.append(") (cores = ");
        msg.append(totalCoresUsed);
        msg.append("/");
        msg.append(totalCores);
        msg.append(")");
        return msg.toString();
    }
}
