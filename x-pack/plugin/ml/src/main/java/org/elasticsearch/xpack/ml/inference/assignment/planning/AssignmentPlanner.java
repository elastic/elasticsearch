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
import java.util.List;

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
        logger.debug(() -> prettyPrintOverallStats(bestPlan));
        return bestPlan;
    }

    private AssignmentPlan solveKeepingOneAllocationOnPreviousAssignments() {
        // We do not want to ever completely unassign a model from a node so we
        // can move allocations without having temporary impact on performance.
        return solvePreservingPreviousAssignments(new PreserveOneAllocation(nodes, models));
    }

    private AssignmentPlan solvePreservingAllPreviousAssignments() {
        return solvePreservingPreviousAssignments(new PreserveAllAllocations(nodes, models));
    }

    private AssignmentPlan solvePreservingPreviousAssignments(AbstractPreserveAllocations preserveAllocations) {
        List<Node> planNodes = preserveAllocations.nodesPreservingAllocations();
        List<Model> planModels = preserveAllocations.modelsPreservingAllocations();
        AssignmentPlan assignmentPlan = new LinearProgrammingPlanSolver(planNodes, planModels).solvePlan();
        return preserveAllocations.mergePreservedAllocations(assignmentPlan);
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
