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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RandomizedAssignmentRoundingTests extends ESTestCase {

    /**
     * Regression test for the leftover-capacity rounding pass ({@code tryAssigningRemainingCores}).
     *
     * <p>That pass calls {@link Deployment#findOptimalAllocations(int, long)} with the node's <em>remaining
     * memory in bytes</em>; it previously (mistakenly) passed the remaining allocation <em>count</em>, so for any
     * deployment reporting per-allocation memory metadata the byte budget was a tiny integer and the pass could
     * never fit a single allocation — silently assigning zero.
     *
     * <p>The direct unit tests in {@code AssignmentPlanTests} cover {@code findOptimalAllocations} in isolation but
     * cannot catch a regression at the call site. In the full LP path the leftover pass is masked because the
     * randomized-rounding stage assigns via a separate (already-correct) call site. To exercise the leftover pass as
     * the sole assignment mechanism we drive the rounding with all-zero soft assignments: there is nothing for the
     * randomized-rounding stage to do, so the entire plan is produced by {@code tryAssigningRemainingCores}. If its
     * memory argument regresses to an allocation count, this test assigns zero allocations and fails.
     */
    public void testLeftoverCapacityPass_AssignsByNodeMemory_ForModelWithMemoryMetadata() {
        Node node = new Node("n_1", ByteSizeValue.ofGb(10).getBytes(), 8);
        // A deployment reporting per-deployment + per-allocation memory metadata, as ML deployments do once observed
        // native memory is threaded in. Four allocations fit comfortably within the node's memory and cores.
        Deployment deployment = new Deployment(
            "m_1",
            "m_1",
            ByteSizeValue.ofMb(100).getBytes(),
            4,
            1,
            Map.of(),
            0,
            null,
            ByteSizeValue.ofMb(400).getBytes(),
            ByteSizeValue.ofMb(500).getBytes()
        );

        List<Node> nodes = List.of(node);
        List<Deployment> deployments = List.of(deployment);

        // All soft assignments are zero, so the randomized-rounding stage contributes nothing and the plan is produced
        // solely by the leftover-capacity pass.
        Map<Tuple<Deployment, Node>, Double> zeroVars = new HashMap<>();
        zeroVars.put(Tuple.tuple(deployment, node), 0.0);

        AssignmentPlan plan = new RandomizedAssignmentRounding(new Random(0), 1, nodes, deployments).computePlan(zeroVars, zeroVars);

        assertThat(plan.assignments(deployment).isPresent(), is(true));
        assertThat(plan.assignments(deployment).get().get(node), equalTo(4));
        assertThat(plan.totalAllocations(deployment), equalTo(4));
    }
}
