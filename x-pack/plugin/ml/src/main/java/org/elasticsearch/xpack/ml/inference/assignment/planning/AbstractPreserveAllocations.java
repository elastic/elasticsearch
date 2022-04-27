/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Model;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.List;
import java.util.Map;
import java.util.Objects;

abstract class AbstractPreserveAllocations {

    private final List<Node> nodes;
    private final List<Model> models;

    protected AbstractPreserveAllocations(List<Node> nodes, List<Model> models) {
        this.nodes = Objects.requireNonNull(nodes);
        this.models = Objects.requireNonNull(models);
    }

    List<Node> nodesPreservingAllocations() {
        return nodes.stream().map(n -> modifyNodePreservingAllocations(n)).toList();
    }

    private Node modifyNodePreservingAllocations(Node n) {
        long bytesUsed = 0;
        int coresUsed = 0;
        for (Model m : models) {
            if (m.currentAllocationByNodeId().containsKey(n.id())) {
                bytesUsed += m.memoryBytes();
                coresUsed += calculateUsedCores(n, m);
            }
        }

        return new Node(n.id(), n.availableMemoryBytes() - bytesUsed, n.cores() - coresUsed);
    }

    List<Model> modelsPreservingAllocations() {
        return models.stream().map(m -> modifyModelPreservingPreviousAssignments(m)).toList();
    }

    Model modifyModelPreservingPreviousAssignments(Model m) {
        if (m.currentAllocationByNodeId().isEmpty()) {
            return m;
        }

        return new Model(
            m.id(),
            m.memoryBytes(),
            m.allocations() - calculatePreservedAllocations(m),
            m.threadsPerAllocation(),
            calculateAllocationsPerNodeToPreserve(m)
        );
    }

    AssignmentPlan mergePreservedAllocations(AssignmentPlan assignmentPlan) {
        AssignmentPlan.Builder mergedPlanBuilder = AssignmentPlan.builder(nodes, models);
        for (Model m : models) {
            Map<Node, Integer> assignments = assignmentPlan.assignments(m);
            for (Node n : nodes) {
                int allocations = assignments == null ? 0 : assignments.getOrDefault(n, 0);
                if (m.currentAllocationByNodeId().containsKey(n.id())) {
                    allocations += addPreservedAllocations(n, m);
                }
                if (allocations > 0) {
                    mergedPlanBuilder.assignModelToNode(m, n, allocations);
                }
            }
        }
        return mergedPlanBuilder.build();
    }

    protected abstract int calculateUsedCores(Node n, Model m);

    protected abstract Map<String, Integer> calculateAllocationsPerNodeToPreserve(Model m);

    protected abstract int calculatePreservedAllocations(Model m);

    protected abstract int addPreservedAllocations(Node n, Model m);
}
