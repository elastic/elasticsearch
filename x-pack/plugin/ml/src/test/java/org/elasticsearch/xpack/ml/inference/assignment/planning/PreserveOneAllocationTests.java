/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Model;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PreserveOneAllocationTests extends ESTestCase {

    public void testGivenNoPreviousAssignments() {
        Node node1 = new Node("n_1", 100, 4);
        Node node2 = new Node("n_2", 100, 4);
        Model model1 = new Model("m_1", 30, 2, 1, Map.of());
        Model model2 = new Model("m_2", 30, 2, 4, Map.of());
        PreserveOneAllocation preserveOneAllocation = new PreserveOneAllocation(List.of(node1, node2), List.of(model1, model2));

        List<Node> nodesPreservingAllocations = preserveOneAllocation.nodesPreservingAllocations();
        assertThat(nodesPreservingAllocations, hasSize(2));
        assertThat(nodesPreservingAllocations.get(0).id(), equalTo("n_1"));
        assertThat(nodesPreservingAllocations.get(0).availableMemoryBytes(), equalTo(100L));
        assertThat(nodesPreservingAllocations.get(0).cores(), equalTo(4));
        assertThat(nodesPreservingAllocations.get(1).id(), equalTo("n_2"));
        assertThat(nodesPreservingAllocations.get(1).availableMemoryBytes(), equalTo(100L));
        assertThat(nodesPreservingAllocations.get(1).cores(), equalTo(4));

        List<Model> modelsPreservingAllocations = preserveOneAllocation.modelsPreservingAllocations();
        assertThat(modelsPreservingAllocations, hasSize(2));
        assertThat(modelsPreservingAllocations.get(0).id(), equalTo("m_1"));
        assertThat(modelsPreservingAllocations.get(0).memoryBytes(), equalTo(30L));
        assertThat(modelsPreservingAllocations.get(0).allocations(), equalTo(2));
        assertThat(modelsPreservingAllocations.get(0).threadsPerAllocation(), equalTo(1));
        assertThat(modelsPreservingAllocations.get(0).currentAllocationByNodeId(), is(anEmptyMap()));
    }
}
