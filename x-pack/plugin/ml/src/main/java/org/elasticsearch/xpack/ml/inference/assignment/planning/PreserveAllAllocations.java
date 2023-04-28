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

public class PreserveAllAllocations extends AbstractPreserveAllocations {

    protected PreserveAllAllocations(List<Node> nodes, List<Deployment> deployments) {
        super(nodes, deployments);
    }

    @Override
    protected int calculateUsedCores(Node n, Deployment m) {
        return m.currentAllocationsByNodeId().get(n.id()) * m.threadsPerAllocation();
    }

    @Override
    protected Map<String, Integer> calculateAllocationsPerNodeToPreserve(Deployment m) {
        return m.currentAllocationsByNodeId().entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> 0));
    }

    @Override
    protected int calculatePreservedAllocations(Deployment m) {
        return m.currentAllocationsByNodeId().values().stream().mapToInt(Integer::intValue).sum();
    }

    @Override
    protected int addPreservedAllocations(Node n, Deployment m) {
        return m.currentAllocationsByNodeId().get(n.id());
    }
}
