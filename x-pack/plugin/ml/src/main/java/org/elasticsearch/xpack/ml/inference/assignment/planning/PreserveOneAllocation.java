/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Deployment;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PreserveOneAllocation extends AbstractPreserveAllocations {

    protected PreserveOneAllocation(List<Node> nodes, List<AssignmentPlan.Deployment> deployments) {
        super(nodes, deployments);
    }

    @Override
    protected int calculateUsedCores(Node n, AssignmentPlan.Deployment m) {
        return m.threadsPerAllocation();
    }

    @Override
    protected Map<String, Integer> calculateAllocationsPerNodeToPreserve(Deployment m) {
        return m.currentAllocationsByNodeId().entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue() - 1));
    }

    @Override
    protected int calculatePreservedAllocations(AssignmentPlan.Deployment m) {
        return (int) m.currentAllocationsByNodeId().values().stream().filter(v -> v > 0).count();
    }

    @Override
    protected int addPreservedAllocations(Node n, AssignmentPlan.Deployment m) {
        return 1;
    }
}
