/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Deployment;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class AssignmentPlanTests extends ESTestCase {

    public void testBuilderCtor_GivenDuplicateNode() {
        Node n = new Node("n_1", 100, 4);
        AssignmentPlan.Deployment m = new AssignmentPlan.Deployment("m_1", 40, 1, 2, Map.of(), 0, null, 0, 0);

        expectThrows(IllegalArgumentException.class, () -> AssignmentPlan.builder(List.of(n, n), List.of(m)));
    }

    public void testBuilderCtor_GivenDuplicateModel() {
        Node n = new Node("n_1", 100, 4);
        Deployment m = new AssignmentPlan.Deployment("m_1", 40, 1, 2, Map.of(), 0, null, 0, 0);

        expectThrows(IllegalArgumentException.class, () -> AssignmentPlan.builder(List.of(n), List.of(m, m)));
    }

    public void testAssignModelToNode_GivenNoPreviousAssignment() {
        Node n = new Node("n_1", ByteSizeValue.ofMb(350).getBytes(), 4);

        { // old memory format
            AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(40).getBytes(),
                1,
                2,
                Map.of(),
                0,
                null,
                0,
                0
            );

            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

            assertThat(builder.getRemainingCores(n), equalTo(4));
            assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(350).getBytes()));
            assertThat(builder.getRemainingAllocations(m), equalTo(1));
            assertThat(builder.getRemainingThreads(m), equalTo(2));

            builder.assignModelToNode(m, n, 1);

            assertThat(builder.getRemainingCores(n), equalTo(2));
            assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(30).getBytes()));
            assertThat(builder.getRemainingAllocations(m), equalTo(0));
            assertThat(builder.getRemainingThreads(m), equalTo(0));

            AssignmentPlan plan = builder.build();

            assertThat(plan.deployments(), contains(m));
            assertThat(plan.satisfiesCurrentAssignments(), is(true));
            assertThat(plan.assignments(m).get(), equalTo(Map.of(n, 1)));
        }
        { // new memory format
            AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(20).getBytes(),
                1,
                2,
                Map.of(),
                0,
                null,
                ByteSizeValue.ofMb(300).getBytes(),
                ByteSizeValue.ofMb(30).getBytes()
            );

            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

            assertThat(builder.getRemainingCores(n), equalTo(4));
            assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(350).getBytes()));
            assertThat(builder.getRemainingAllocations(m), equalTo(1));
            assertThat(builder.getRemainingThreads(m), equalTo(2));

            builder.assignModelToNode(m, n, 1);

            assertThat(builder.getRemainingCores(n), equalTo(2));
            assertThat(builder.getRemainingMemory(n), equalTo(0L));
            assertThat(builder.getRemainingAllocations(m), equalTo(0));
            assertThat(builder.getRemainingThreads(m), equalTo(0));

            AssignmentPlan plan = builder.build();

            assertThat(plan.deployments(), contains(m));
            assertThat(plan.satisfiesCurrentAssignments(), is(true));
            assertThat(plan.assignments(m).get(), equalTo(Map.of(n, 1)));
        }
    }

    public void testAssignModelToNode_GivenNewPlanSatisfiesCurrentAssignment() {
        Node n = new Node("n_1", ByteSizeValue.ofMb(350).getBytes(), 4);
        {   // old memory format
            AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(30).getBytes(),
                2,
                2,
                Map.of("n_1", 1),
                0,
                null,
                0,
                0
            );

            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

            builder.assignModelToNode(m, n, 1);

            assertThat(builder.getRemainingCores(n), equalTo(2));
            assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(350).getBytes()));
            assertThat(builder.getRemainingAllocations(m), equalTo(1));
            assertThat(builder.getRemainingThreads(m), equalTo(2));

            AssignmentPlan plan = builder.build();

            assertThat(plan.deployments(), contains(m));
            assertThat(plan.satisfiesCurrentAssignments(), is(true));
            assertThat(plan.assignments(m).get(), equalTo(Map.of(n, 1)));
        }
        {   // new memory format
            AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(25).getBytes(),
                2,
                2,
                Map.of("n_1", 1),
                0,
                null,
                ByteSizeValue.ofMb(300).getBytes(),
                ByteSizeValue.ofMb(25).getBytes()
            );

            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

            builder.assignModelToNode(m, n, 1);

            assertThat(builder.getRemainingCores(n), equalTo(2));
            assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(325).getBytes()));
            assertThat(builder.getRemainingAllocations(m), equalTo(1));
            assertThat(builder.getRemainingThreads(m), equalTo(2));

            AssignmentPlan plan = builder.build();

            assertThat(plan.deployments(), contains(m));
            assertThat(plan.satisfiesCurrentAssignments(), is(true));
            assertThat(plan.assignments(m).get(), equalTo(Map.of(n, 1)));

        }
    }

    public void testAssignModelToNode_GivenNewPlanDoesNotSatisfyCurrentAssignment() {
        Node n = new Node("n_1", ByteSizeValue.ofMb(300).getBytes(), 4);
        {
            // old memory format
            Deployment m = new Deployment("m_1", ByteSizeValue.ofMb(30).getBytes(), 2, 2, Map.of("n_1", 2), 0, null, 0, 0);

            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

            builder.assignModelToNode(m, n, 1);

            assertThat(builder.getRemainingCores(n), equalTo(2));
            assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(300).getBytes()));
            assertThat(builder.getRemainingAllocations(m), equalTo(1));
            assertThat(builder.getRemainingThreads(m), equalTo(2));

            AssignmentPlan plan = builder.build();

            assertThat(plan.deployments(), contains(m));
            assertThat(plan.satisfiesCurrentAssignments(), is(false));
            assertThat(plan.assignments(m).get(), equalTo(Map.of(n, 1)));
        }
        {
            // new memory format
            Deployment m = new Deployment(
                "m_1",
                ByteSizeValue.ofMb(25).getBytes(),
                2,
                2,
                Map.of("n_1", 2),
                0,
                null,
                ByteSizeValue.ofMb(250).getBytes(),
                ByteSizeValue.ofMb(25).getBytes()
            );

            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

            builder.assignModelToNode(m, n, 1);

            assertThat(builder.getRemainingCores(n), equalTo(2));
            assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(275).getBytes()));
            assertThat(builder.getRemainingAllocations(m), equalTo(1));
            assertThat(builder.getRemainingThreads(m), equalTo(2));

            AssignmentPlan plan = builder.build();

            assertThat(plan.deployments(), contains(m));
            assertThat(plan.satisfiesCurrentAssignments(), is(false));
            assertThat(plan.assignments(m).get(), equalTo(Map.of(n, 1)));
        }
    }

    public void testAssignModelToNode_GivenPreviouslyUnassignedModelDoesNotFit() {
        Node n = new Node("n_1", ByteSizeValue.ofMb(340 - 1).getBytes(), 4);
        Deployment m = new AssignmentPlan.Deployment("m_1", ByteSizeValue.ofMb(50).getBytes(), 2, 2, Map.of(), 0, null, 0, 0);

        AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
        Exception e = expectThrows(IllegalArgumentException.class, () -> builder.assignModelToNode(m, n, 1));

        assertThat(e.getMessage(), equalTo("not enough memory on node [n_1] to assign [1] allocations to deployment [m_1]"));
    }

    public void testAssignModelToNode_GivenPreviouslyAssignedModelDoesNotFit() {
        { // old memory format
            Node n = new Node("n_1", ByteSizeValue.ofMb(340 - 1).getBytes(), 4);
            AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(50).getBytes(),
                2,
                2,
                Map.of("n_1", 1),
                0,
                null,
                0,
                0
            );

            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

            builder.assignModelToNode(m, n, 2);
            AssignmentPlan plan = builder.build();

            assertThat(plan.deployments(), contains(m));
            assertThat(plan.satisfiesCurrentAssignments(), is(true));
            assertThat(plan.assignments(m).get(), equalTo(Map.of(n, 2)));
        }
        { // new memory format
            Node n = new Node("n_1", ByteSizeValue.ofMb(340 - 1).getBytes(), 4);
            AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(30).getBytes(),
                2,
                2,
                Map.of("n_1", 1),
                0,
                null,
                ByteSizeValue.ofMb(300).getBytes(),
                ByteSizeValue.ofMb(5).getBytes()
            );

            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

            builder.assignModelToNode(m, n, 2);
            AssignmentPlan plan = builder.build();

            assertThat(plan.deployments(), contains(m));
            assertThat(plan.satisfiesCurrentAssignments(), is(true));
            assertThat(plan.assignments(m).get(), equalTo(Map.of(n, 2)));
        }
    }

    public void testAssignModelToNode_GivenNotEnoughCores_AndSingleThreadPerAllocation() {
        Node n = new Node("n_1", ByteSizeValue.ofMb(500).getBytes(), 4);
        Deployment m = new AssignmentPlan.Deployment("m_1", ByteSizeValue.ofMb(100).getBytes(), 5, 1, Map.of(), 0, null, 0, 0);

        AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
        Exception e = expectThrows(IllegalArgumentException.class, () -> builder.assignModelToNode(m, n, 5));

        assertThat(
            e.getMessage(),
            equalTo("not enough cores on node [n_1] to assign [5] allocations to deployment [m_1]; required threads per allocation [1]")
        );
    }

    public void testAssignModelToNode_GivenNotEnoughCores_AndMultipleThreadsPerAllocation() {
        Node n = new Node("n_1", ByteSizeValue.ofMb(500).getBytes(), 5);
        AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
            "m_1",
            ByteSizeValue.ofMb(100).getBytes(),
            3,
            2,
            Map.of(),
            0,
            null,
            0,
            0
        );

        AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
        Exception e = expectThrows(IllegalArgumentException.class, () -> builder.assignModelToNode(m, n, 3));

        assertThat(
            e.getMessage(),
            equalTo("not enough cores on node [n_1] to assign [3] allocations to deployment [m_1]; required threads per allocation [2]")
        );
    }

    public void testAssignModelToNode_GivenSameModelAssignedTwice() {
        Node n = new Node("n_1", ByteSizeValue.ofMb(1000).getBytes(), 8);
        Deployment m = new AssignmentPlan.Deployment(
            "m_1",
            ByteSizeValue.ofMb(50).getBytes(),
            4,
            2,
            Map.of(),
            0,
            null,
            ByteSizeValue.ofMb(300).getBytes(),
            ByteSizeValue.ofMb(50).getBytes()
        );

        AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

        assertThat(builder.getRemainingCores(n), equalTo(8));
        assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(1000).getBytes()));
        assertThat(builder.getRemainingAllocations(m), equalTo(4));
        assertThat(builder.getRemainingThreads(m), equalTo(8));
        assertThat(builder.canAssign(m, n, 1), is(true));

        builder.assignModelToNode(m, n, 1);

        assertThat(builder.getRemainingCores(n), equalTo(6));
        assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(600).getBytes()));
        assertThat(builder.getRemainingAllocations(m), equalTo(3));
        assertThat(builder.getRemainingThreads(m), equalTo(6));
        assertThat(builder.canAssign(m, n, 2), is(true));

        builder.assignModelToNode(m, n, 2);

        assertThat(builder.getRemainingCores(n), equalTo(2));
        assertThat(builder.getRemainingMemory(n), equalTo(ByteSizeValue.ofMb(500).getBytes()));
        assertThat(builder.getRemainingAllocations(m), equalTo(1));
        assertThat(builder.getRemainingThreads(m), equalTo(2));

        AssignmentPlan plan = builder.build();

        assertThat(plan.deployments(), contains(m));
        assertThat(plan.satisfiesCurrentAssignments(), is(true));
        assertThat(plan.assignments(m).get(), equalTo(Map.of(n, 3)));
    }

    public void testCanAssign_GivenPreviouslyUnassignedModelDoesNotFit() {
        Node n = new Node("n_1", 100, 5);
        AssignmentPlan.Deployment m = new AssignmentPlan.Deployment("m_1", 101, 1, 1, Map.of(), 0, null, 0, 0);

        AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

        assertThat(builder.canAssign(m, n, 1), is(false));
    }

    public void testCanAssign_GivenPreviouslyAssignedModelDoesNotFit() {
        Node n = new Node("n_1", ByteSizeValue.ofMb(300).getBytes(), 5);
        {
            // old memory format
            Deployment m = new Deployment("m_1", ByteSizeValue.ofMb(31).getBytes(), 1, 1, Map.of("n_1", 1), 0, null, 0, 0);
            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
            assertThat(builder.canAssign(m, n, 1), is(true));
        }
        {
            // new memory format
            Deployment m = new Deployment(
                "m_1",
                ByteSizeValue.ofMb(25).getBytes(),
                1,
                1,
                Map.of("n_1", 1),
                0,
                null,
                ByteSizeValue.ofMb(300).getBytes(),
                ByteSizeValue.ofMb(10).getBytes()
            );
            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
            assertThat(builder.canAssign(m, n, 1), is(true));
        }
    }

    public void testCanAssign_GivenEnoughMemory() {
        Node n = new Node("n_1", ByteSizeValue.ofMb(440).getBytes(), 5);
        AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
            "m_1",
            ByteSizeValue.ofMb(100).getBytes(),
            3,
            2,
            Map.of(),
            0,
            null,
            0,
            0
        );

        AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));

        assertThat(builder.canAssign(m, n, 1), is(true));
        assertThat(builder.canAssign(m, n, 2), is(true));
        assertThat(builder.canAssign(m, n, 3), is(false));
    }

    public void testCompareTo_GivenDifferenceInPreviousAssignments() {
        AssignmentPlan planSatisfyingPreviousAssignments;
        AssignmentPlan planNotSatisfyingPreviousAssignments;
        Node n = new Node("n_1", ByteSizeValue.ofMb(300).getBytes(), 5);

        {
            Deployment m = new AssignmentPlan.Deployment("m_1", ByteSizeValue.ofMb(30).getBytes(), 3, 2, Map.of("n_1", 2), 0, null, 0, 0);
            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
            builder.assignModelToNode(m, n, 2);
            planSatisfyingPreviousAssignments = builder.build();
        }
        {
            AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(30).getBytes(),
                3,
                2,
                Map.of("n_1", 3),
                0,
                null,
                0,
                0
            );
            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
            builder.assignModelToNode(m, n, 2);
            planNotSatisfyingPreviousAssignments = builder.build();
        }

        assertThat(planSatisfyingPreviousAssignments.compareTo(planNotSatisfyingPreviousAssignments), greaterThan(0));
        assertThat(planNotSatisfyingPreviousAssignments.compareTo(planSatisfyingPreviousAssignments), lessThan(0));
    }

    public void testCompareTo_GivenDifferenceInAllocations() {
        AssignmentPlan planWithMoreAllocations;
        AssignmentPlan planWithFewerAllocations;
        Node n = new Node("n_1", ByteSizeValue.ofMb(300).getBytes(), 5);
        AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
            "m_1",
            ByteSizeValue.ofMb(30).getBytes(),
            3,
            2,
            Map.of("n_1", 1),
            0,
            null,
            0,
            0
        );

        {
            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
            builder.assignModelToNode(m, n, 2);
            planWithMoreAllocations = builder.build();
        }
        {
            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
            builder.assignModelToNode(m, n, 1);
            planWithFewerAllocations = builder.build();
        }

        assertThat(planWithMoreAllocations.compareTo(planWithFewerAllocations), greaterThan(0));
        assertThat(planWithFewerAllocations.compareTo(planWithMoreAllocations), lessThan(0));
    }

    public void testCompareTo_GivenDifferenceInMemory() {
        AssignmentPlan planUsingMoreMemory;
        AssignmentPlan planUsingLessMemory;
        Node n = new Node("n_1", ByteSizeValue.ofMb(300).getBytes(), 5);

        {
            Deployment m = new AssignmentPlan.Deployment("m_1", ByteSizeValue.ofMb(30).getBytes(), 3, 2, Map.of("n_1", 1), 0, null, 0, 0);
            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
            builder.assignModelToNode(m, n, 2);
            planUsingMoreMemory = builder.build();
        }
        {
            AssignmentPlan.Deployment m = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(29).getBytes(),
                3,
                2,
                Map.of("n_1", 1),
                0,
                null,
                0,
                0
            );
            AssignmentPlan.Builder builder = AssignmentPlan.builder(List.of(n), List.of(m));
            builder.assignModelToNode(m, n, 2);
            planUsingLessMemory = builder.build();
        }

        assertThat(planUsingLessMemory.compareTo(planUsingMoreMemory), greaterThan(0));
        assertThat(planUsingMoreMemory.compareTo(planUsingLessMemory), lessThan(0));
    }

    public void testSatisfiesAllModels_GivenAllDeploymentsAreSatisfied() {
        Node node1 = new Node("n_1", ByteSizeValue.ofMb(1000).getBytes(), 4);
        Node node2 = new Node("n_2", ByteSizeValue.ofMb(1000).getBytes(), 4);
        {
            // old memory format
            AssignmentPlan.Deployment deployment1 = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(50).getBytes(),
                1,
                2,
                Map.of(),
                0,
                null,
                0,
                0
            );
            AssignmentPlan.Deployment deployment2 = new AssignmentPlan.Deployment(
                "m_2",
                ByteSizeValue.ofMb(30).getBytes(),
                2,
                1,
                Map.of(),
                0,
                null,
                0,
                0
            );
            AssignmentPlan.Deployment deployment3 = new AssignmentPlan.Deployment(
                "m_3",
                ByteSizeValue.ofMb(20).getBytes(),
                4,
                1,
                Map.of(),
                0,
                null,
                0,
                0
            );
            AssignmentPlan plan = AssignmentPlan.builder(List.of(node1, node2), List.of(deployment1, deployment2, deployment3))
                .assignModelToNode(deployment1, node1, 1)
                .assignModelToNode(deployment2, node2, 2)
                .assignModelToNode(deployment3, node1, 2)
                .assignModelToNode(deployment3, node2, 2)
                .build();
            assertThat(plan.satisfiesAllModels(), is(true));
        }
        {
            // new memory format
            AssignmentPlan.Deployment deployment1 = new AssignmentPlan.Deployment(
                "m_1",
                ByteSizeValue.ofMb(50).getBytes(),
                1,
                2,
                Map.of(),
                0,
                null,
                ByteSizeValue.ofMb(300).getBytes(),
                ByteSizeValue.ofMb(10).getBytes()
            );
            AssignmentPlan.Deployment deployment2 = new AssignmentPlan.Deployment(
                "m_2",
                ByteSizeValue.ofMb(30).getBytes(),
                2,
                1,
                Map.of(),
                0,
                null,
                ByteSizeValue.ofMb(300).getBytes(),
                ByteSizeValue.ofMb(10).getBytes()
            );
            AssignmentPlan.Deployment deployment3 = new AssignmentPlan.Deployment(
                "m_3",
                ByteSizeValue.ofMb(20).getBytes(),
                4,
                1,
                Map.of(),
                0,
                null,
                ByteSizeValue.ofMb(300).getBytes(),
                ByteSizeValue.ofMb(10).getBytes()
            );
            AssignmentPlan plan = AssignmentPlan.builder(List.of(node1, node2), List.of(deployment1, deployment2, deployment3))
                .assignModelToNode(deployment1, node1, 1)
                .assignModelToNode(deployment2, node2, 2)
                .assignModelToNode(deployment3, node1, 2)
                .assignModelToNode(deployment3, node2, 2)
                .build();
            assertThat(plan.satisfiesAllModels(), is(true));
        }
    }

    public void testSatisfiesAllDeployments_GivenOneModelHasOneAllocationLess() {
        Node node1 = new Node("n_1", ByteSizeValue.ofMb(1000).getBytes(), 4);
        Node node2 = new Node("n_2", ByteSizeValue.ofMb(1000).getBytes(), 4);
        Deployment deployment1 = new Deployment("m_1", ByteSizeValue.ofMb(50).getBytes(), 1, 2, Map.of(), 0, null, 0, 0);
        Deployment deployment2 = new Deployment("m_2", ByteSizeValue.ofMb(30).getBytes(), 2, 1, Map.of(), 0, null, 0, 0);
        Deployment deployment3 = new Deployment("m_3", ByteSizeValue.ofMb(20).getBytes(), 4, 1, Map.of(), 0, null, 0, 0);
        AssignmentPlan plan = AssignmentPlan.builder(List.of(node1, node2), List.of(deployment1, deployment2, deployment3))
            .assignModelToNode(deployment1, node1, 1)
            .assignModelToNode(deployment2, node2, 2)
            .assignModelToNode(deployment3, node1, 1)
            .assignModelToNode(deployment3, node2, 2)
            .build();
        assertThat(plan.satisfiesAllModels(), is(false));
    }

    public void testArePreviouslyAssignedDeploymentsAssigned_GivenTrue() {
        Node node1 = new Node("n_1", ByteSizeValue.ofMb(1000).getBytes(), 4);
        Node node2 = new Node("n_2", ByteSizeValue.ofMb(1000).getBytes(), 4);
        Deployment deployment1 = new Deployment("m_1", ByteSizeValue.ofMb(50).getBytes(), 1, 2, Map.of(), 3, null, 0, 0);
        Deployment deployment2 = new Deployment("m_2", ByteSizeValue.ofMb(30).getBytes(), 2, 1, Map.of(), 4, null, 0, 0);
        Deployment deployment3 = new Deployment("m_3", ByteSizeValue.ofMb(20).getBytes(), 4, 1, Map.of(), 0, null, 0, 0);
        AssignmentPlan plan = AssignmentPlan.builder(List.of(node1, node2), List.of(deployment1, deployment2, deployment3))
            .assignModelToNode(deployment1, node1, 1)
            .assignModelToNode(deployment2, node2, 1)
            .build();
        assertThat(plan.arePreviouslyAssignedModelsAssigned(), is(true));
    }

    public void testArePreviouslyAssignedDeploymentsAssigned_GivenFalse() {
        Node node1 = new Node("n_1", ByteSizeValue.ofMb(1000).getBytes(), 4);
        Node node2 = new Node("n_2", ByteSizeValue.ofMb(1000).getBytes(), 4);
        Deployment deployment1 = new Deployment("m_1", ByteSizeValue.ofMb(50).getBytes(), 1, 2, Map.of(), 3, null, 0, 0);
        Deployment deployment2 = new Deployment("m_2", ByteSizeValue.ofMb(30).getBytes(), 2, 1, Map.of(), 4, null, 0, 0);
        AssignmentPlan plan = AssignmentPlan.builder(List.of(node1, node2), List.of(deployment1, deployment2))
            .assignModelToNode(deployment1, node1, 1)
            .build();
        assertThat(plan.arePreviouslyAssignedModelsAssigned(), is(false));
    }

    public void testCountPreviouslyAssignedThatAreStillAssigned() {
        Node node1 = new Node("n_1", ByteSizeValue.ofMb(1000).getBytes(), 4);
        Node node2 = new Node("n_2", ByteSizeValue.ofMb(1000).getBytes(), 4);
        Deployment deployment1 = new AssignmentPlan.Deployment("m_1", ByteSizeValue.ofMb(50).getBytes(), 1, 2, Map.of(), 3, null, 0, 0);
        AssignmentPlan.Deployment deployment2 = new AssignmentPlan.Deployment(
            "m_2",
            ByteSizeValue.ofMb(30).getBytes(),
            2,
            1,
            Map.of(),
            4,
            null,
            0,
            0
        );
        AssignmentPlan.Deployment deployment3 = new AssignmentPlan.Deployment(
            "m_3",
            ByteSizeValue.ofMb(20).getBytes(),
            4,
            1,
            Map.of(),
            1,
            null,
            0,
            0
        );
        AssignmentPlan.Deployment deployment4 = new AssignmentPlan.Deployment(
            "m_4",
            ByteSizeValue.ofMb(20).getBytes(),
            4,
            1,
            Map.of(),
            0,
            null,
            0,
            0
        );
        AssignmentPlan plan = AssignmentPlan.builder(List.of(node1, node2), List.of(deployment1, deployment2, deployment3, deployment4))
            .assignModelToNode(deployment1, node1, 1)
            .assignModelToNode(deployment2, node2, 1)
            .build();
        assertThat(plan.countPreviouslyAssignedModelsThatAreStillAssigned(), equalTo(2L));
    }
}
