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
import java.util.stream.Collectors;

public class PreserveAllAllocations extends AbstractPreserveAllocations {

    protected PreserveAllAllocations(List<Node> nodes, List<Model> models) {
        super(nodes, models);
    }

    @Override
    protected int calculateUsedCores(Node n, Model m) {
        return m.currentAllocationByNodeId().get(n.id()) * m.threadsPerAllocation();
    }

    @Override
    protected Map<String, Integer> calculateAllocationsPerNodeToPreserve(Model m) {
        return m.currentAllocationByNodeId().entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> 0));
    }

    @Override
    protected int calculatePreservedAllocations(Model m) {
        return m.currentAllocationByNodeId().values().stream().mapToInt(Integer::intValue).sum();
    }

    @Override
    protected int addPreservedAllocations(Node n, Model m) {
        return m.currentAllocationByNodeId().get(n.id());
    }
}
