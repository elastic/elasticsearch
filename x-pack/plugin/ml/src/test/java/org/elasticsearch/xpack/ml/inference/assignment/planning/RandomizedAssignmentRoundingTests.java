/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Deployment;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RandomizedAssignmentRoundingTests extends ESTestCase {

    /**
     * Rounding the relaxed LP solution down leaves deployments short of the allocations they asked
     * for, which is why {@code tryAssigningRemainingCores} exists: it tops them up using whatever
     * capacity is left on nodes that are not yet hosting the deployment.
     *
     * Here a deployment asks for 4 allocations and the rounded solution gives it 1 on the first
     * node. The first node has a single core, so it is fully consumed by that one allocation and
     * {@code assignExcessCores} - which tops up nodes the deployment already runs on - cannot place
     * any more there. The second node is completely idle with ample memory and cores, so the only
     * way to reach the requested 4 allocations is for {@code tryAssigningRemainingCores} to spread
     * the remaining 3 onto it.
     *
     * This only exercises the interesting path for deployments that publish
     * {@code per_deployment_memory_bytes} / {@code per_allocation_memory_bytes}; when both are 0,
     * {@code findOptimalAllocations} returns early and the memory arithmetic is skipped entirely.
     */
    public void testTopUpUsesLeftoverCapacity_GivenDeploymentWithMemoryMetadata() {
        Node node1 = new Node("n_1", ByteSizeValue.ofGb(10).getBytes(), 1);
        Node node2 = new Node("n_2", ByteSizeValue.ofGb(10).getBytes(), 8);

        Deployment deployment = new Deployment(
            "d_1",
            "m_1",
            ByteSizeValue.ofMb(100).getBytes(), // model size
            4,                                  // requested allocations
            1,                                  // threads per allocation
            Map.of(),                           // no pre-existing allocations
            0,
            null,
            ByteSizeValue.ofMb(400).getBytes(), // per deployment memory
            ByteSizeValue.ofMb(200).getBytes()  // per allocation memory
        );

        // Integer values mean there are no soft assignments, so randomized rounding is skipped and
        // the outcome does not depend on the random seed.
        Map<Tuple<Deployment, Node>, Double> allocationVars = Map.of(
            Tuple.tuple(deployment, node1),
            1.0,
            Tuple.tuple(deployment, node2),
            0.0
        );
        Map<Tuple<Deployment, Node>, Double> assignmentVars = Map.of(
            Tuple.tuple(deployment, node1),
            1.0,
            Tuple.tuple(deployment, node2),
            0.0
        );

        AssignmentPlan plan = new RandomizedAssignmentRounding(random(), 1, List.of(node1, node2), List.of(deployment)).computePlan(
            allocationVars,
            assignmentVars
        );

        Map<Node, Integer> assignments = plan.assignments(deployment).orElse(Map.of());
        int totalAllocations = assignments.values().stream().mapToInt(Integer::intValue).sum();

        assertThat(totalAllocations, equalTo(4));
        assertThat(plan.satisfiesAllocations(deployment), is(true));
    }
}
