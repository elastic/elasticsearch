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
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Model;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 */
public class AssignmentPlanner {

    private static final Logger logger = LogManager.getLogger(AssignmentPlanner.class);

    private final List<Node> nodes;
    private final List<Model> models;

    public AssignmentPlanner(List<Node> nodes, List<Model> models) {
        this.nodes = nodes.stream().sorted(Comparator.comparing(Node::id)).toList();
        this.models = models.stream().sorted(Comparator.comparing(Model::id)).toList();
    }

    public AssignmentPlan computePlan() {
        AssignmentPlan planKeepingOneAllocationOnPreviousAssignments = solveKeepingOneAllocationOnPreviousAssignments();
        AssignmentPlan bestPlan = planKeepingOneAllocationOnPreviousAssignments.satisfiesPreviousAssignments()
            ? planKeepingOneAllocationOnPreviousAssignments
            : solvePreservingAllPreviousAssignments();
        logger.debug(() -> "Best plan =\n" + bestPlan.prettyPrint());
        logger.debug(prettyPrintOverallStats(bestPlan));
        return bestPlan;
    }

    private AssignmentPlan solveKeepingOneAllocationOnPreviousAssignments() {
        // We do not want to ever completely unassign a model from a node.
        // We want to keep at least one allocation where an assignment used to be
        // in order to move allocations without having temporary impact on performance.
        return solvePreservingPreviousAssignments(PreserveAllocations.ONE);
    }

    private AssignmentPlan solvePreservingAllPreviousAssignments() {
        return solvePreservingPreviousAssignments(PreserveAllocations.ALL);
    }

    private AssignmentPlan solvePreservingPreviousAssignments(PreserveAllocations preserveAllocations) {
        List<Node> planNodes = nodes.stream().map(n -> modifyNodePreservingPreviousAssignments(n, preserveAllocations)).toList();
        List<Model> planModels = models.stream().map(m -> modifyModelPreservingPreviousAssignments(m, preserveAllocations)).toList();
        AssignmentPlan assignmentPlan = new LinearProgrammingPlanSolver(planNodes, planModels).solvePlan();
        return mergePreviousAssignments(assignmentPlan, preserveAllocations);
    }

    private Node modifyNodePreservingPreviousAssignments(Node n, PreserveAllocations preserveAllocations) {
        long bytesUsed = 0;
        int coresUsed = 0;
        for (Model m : models) {
            if (m.currentAllocationByNodeId().containsKey(n.id())) {
                bytesUsed += m.memoryBytes();
                switch (preserveAllocations) {
                    case ONE:
                        coresUsed += m.threadsPerAllocation();
                        break;
                    case ALL:
                        coresUsed += m.currentAllocationByNodeId().get(n.id()) * m.threadsPerAllocation();
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }
        }

        return new Node(n.id(), n.availableMemoryBytes() - bytesUsed, n.cores() - coresUsed);
    }

    private Model modifyModelPreservingPreviousAssignments(Model m, PreserveAllocations preserveAllocations) {
        if (m.currentAllocationByNodeId().isEmpty()) {
            return m;
        }

        int preservedAllocations = 0;
        Map<String, Integer> remainingAllocationsToPreservePerNodeId = new HashMap<>();
        switch (preserveAllocations) {
            case ONE:
                preservedAllocations += m.currentAllocationByNodeId().values().stream().filter(v -> v > 0).count();
                m.currentAllocationByNodeId()
                    .entrySet()
                    .forEach(e -> remainingAllocationsToPreservePerNodeId.put(e.getKey(), Math.max(0, e.getValue() - 1)));
                break;
            case ALL:
                preservedAllocations += m.currentAllocationByNodeId().values().stream().mapToInt(Integer::intValue).sum();
                m.currentAllocationByNodeId().keySet().forEach(k -> remainingAllocationsToPreservePerNodeId.put(k, 0));
                break;
            default:
                throw new IllegalStateException();
        }
        return new Model(
            m.id(),
            m.memoryBytes(),
            m.allocations() - preservedAllocations,
            m.threadsPerAllocation(),
            remainingAllocationsToPreservePerNodeId
        );
    }

    private AssignmentPlan mergePreviousAssignments(AssignmentPlan assignmentPlan, PreserveAllocations preserveAllocations) {
        AssignmentPlan.Builder mergedPlanBuilder = AssignmentPlan.builder(nodes, models);
        for (Model m : models) {
            Map<Node, Integer> assignments = assignmentPlan.assignments(m);
            for (Node n : nodes) {
                int allocations = assignments == null ? 0 : assignments.getOrDefault(n, 0);
                if (m.currentAllocationByNodeId().containsKey(n.id())) {
                    switch (preserveAllocations) {
                        case ONE:
                            allocations++;
                            break;
                        case ALL:
                            allocations += m.currentAllocationByNodeId().get(n.id());
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                }
                if (allocations > 0) {
                    mergedPlanBuilder.assignModelToNode(m, n, allocations);
                }
            }
        }
        return mergedPlanBuilder.build();
    }

    private enum PreserveAllocations {
        ONE,
        ALL;
    }

    private String prettyPrintOverallStats(AssignmentPlan assignmentPlan) {
        int totalAllocationsRequired = 0;
        int totalAllocationsAssigned = 0;
        int totalCoresUsed = 0;
        long totalAvailableMem = nodes.stream().map(Node::availableMemoryBytes).mapToLong(Long::longValue).sum();
        int totalCores = nodes.stream().map(Node::cores).mapToInt(Integer::intValue).sum();
        long totalUsedMem = 0;
        for (Model m : models) {
            totalAllocationsRequired += m.allocations();
            if (assignmentPlan.assignments(m) != null) {
                int allocations = assignmentPlan.assignments(m).values().stream().mapToInt(Integer::intValue).sum();
                totalAllocationsAssigned += allocations;
                totalCoresUsed += allocations * m.threadsPerAllocation();
                totalUsedMem += m.memoryBytes() * assignmentPlan.assignments(m).values().size();
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
