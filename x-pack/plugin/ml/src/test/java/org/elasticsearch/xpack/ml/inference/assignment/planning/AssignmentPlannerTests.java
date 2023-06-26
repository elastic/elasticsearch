/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Deployment;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AssignmentPlannerTests extends ESTestCase {

    public void testModelThatDoesNotFitInMemory() {
        List<Node> nodes = List.of(new Node("n_1", 100, 4));
        Deployment deployment = new AssignmentPlan.Deployment("m_1", 101, 4, 1, Map.of(), 0);
        AssignmentPlan plan = new AssignmentPlanner(nodes, List.of(deployment)).computePlan();
        assertThat(plan.assignments(deployment).isEmpty(), is(true));
    }

    public void testModelWithThreadsPerAllocationNotFittingOnAnyNode() {
        List<Node> nodes = List.of(new Node("n_1", 100, 4), new Node("n_2", 100, 5));
        Deployment deployment = new AssignmentPlan.Deployment("m_1", 1, 1, 6, Map.of(), 0);
        AssignmentPlan plan = new AssignmentPlanner(nodes, List.of(deployment)).computePlan();
        assertThat(plan.assignments(deployment).isEmpty(), is(true));
    }

    public void testSingleModelThatFitsFullyOnSingleNode() {
        {
            Node node = new Node("n_1", 100, 4);
            Deployment deployment = new AssignmentPlan.Deployment("m_1", 100, 1, 1, Map.of(), 0);
            AssignmentPlan plan = new AssignmentPlanner(List.of(node), List.of(deployment)).computePlan();
            assertModelFullyAssignedToNode(plan, deployment, node);
        }
        {
            Node node = new Node("n_1", 1000, 8);
            Deployment deployment = new Deployment("m_1", 1000, 8, 1, Map.of(), 0);
            AssignmentPlan plan = new AssignmentPlanner(List.of(node), List.of(deployment)).computePlan();
            assertModelFullyAssignedToNode(plan, deployment, node);
        }
        {
            Node node = new Node("n_1", 10000, 16);
            AssignmentPlan.Deployment deployment = new AssignmentPlan.Deployment("m_1", 10000, 1, 16, Map.of(), 0);
            AssignmentPlan plan = new AssignmentPlanner(List.of(node), List.of(deployment)).computePlan();
            assertModelFullyAssignedToNode(plan, deployment, node);
        }
    }

    public void testSingleModelThatFitsFullyOnSingleNode_GivenTwoNodes_ShouldBeFullyAssignedOnOneNode() {
        Node node1 = new Node("n_1", 100, 4);
        Node node2 = new Node("n_2", 100, 4);
        AssignmentPlan.Deployment deployment = new Deployment("m_1", 100, 4, 1, Map.of(), 0);

        AssignmentPlan plan = new AssignmentPlanner(List.of(node1, node2), List.of(deployment)).computePlan();

        Map<Node, Integer> assignments = plan.assignments(deployment).get();
        if (assignments.get(node1) > 0) {
            assertThat(assignments.get(node1), equalTo(4));
        } else {
            assertThat(assignments.get(node2), equalTo(4));
        }
    }

    public void testModelWithMoreAllocationsThanAvailableCores_GivenSingleThreadPerAllocation() {
        AssignmentPlan.Deployment deployment = new Deployment("m_1", 30, 10, 1, Map.of(), 0);
        // Single node
        {
            Node node = new Node("n_1", 100, 4);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node), List.of(deployment)).computePlan();
            assertThat(assignmentPlan.assignments(deployment).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(deployment).get();
            assertThat(assignments.get(node), equalTo(4));
        }
        // Two nodes
        {
            Node node1 = new Node("n_1", 100, 4);
            Node node2 = new Node("n_2", 100, 2);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2), List.of(deployment)).computePlan();
            assertThat(assignmentPlan.assignments(deployment).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(deployment).get();
            assertThat(assignments.get(node1), equalTo(4));
            assertThat(assignments.get(node2), equalTo(2));
        }
        // Three nodes
        {
            Node node1 = new Node("n_1", 100, 4);
            Node node2 = new Node("n_2", 100, 2);
            Node node3 = new Node("n_3", 100, 3);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2, node3), List.of(deployment)).computePlan();
            assertThat(assignmentPlan.assignments(deployment).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(deployment).get();
            assertThat(assignments.get(node1), equalTo(4));
            assertThat(assignments.get(node2), equalTo(2));
            assertThat(assignments.get(node3), equalTo(3));
        }
    }

    public void testMultipleModelsAndNodesWithSingleSolution() {
        Node node1 = new Node("n_1", 100, 7);
        Node node2 = new Node("n_2", 100, 7);
        Node node3 = new Node("n_3", 100, 2);
        Node node4 = new Node("n_4", 100, 2);
        Deployment deployment1 = new Deployment("m_1", 50, 2, 4, Map.of(), 0);
        AssignmentPlan.Deployment deployment2 = new Deployment("m_2", 50, 2, 3, Map.of(), 0);
        Deployment deployment3 = new AssignmentPlan.Deployment("m_3", 50, 1, 2, Map.of(), 0);
        AssignmentPlan.Deployment deployment4 = new AssignmentPlan.Deployment("m_4", 50, 2, 1, Map.of(), 0);

        AssignmentPlan plan = new AssignmentPlanner(
            List.of(node1, node2, node3, node4),
            List.of(deployment1, deployment2, deployment3, deployment4)
        ).computePlan();

        {
            assertThat(plan.assignments(deployment1).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(deployment1).get();
            assertThat(assignments.get(node1), equalTo(1));
            assertThat(assignments.get(node2), equalTo(1));
            assertThat(assignments.get(node3), is(nullValue()));
            assertThat(assignments.get(node4), is(nullValue()));
        }
        {
            assertThat(plan.assignments(deployment2).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(deployment2).get();
            assertThat(assignments.get(node1), equalTo(1));
            assertThat(assignments.get(node2), equalTo(1));
            assertThat(assignments.get(node3), is(nullValue()));
            assertThat(assignments.get(node4), is(nullValue()));
        }
        {
            assertThat(plan.assignments(deployment3).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(deployment3).get();
            assertThat(assignments.get(node1), is(nullValue()));
            assertThat(assignments.get(node2), is(nullValue()));
            // Will either be on node 3 or 4
            Node assignedNode = assignments.get(node3) != null ? node3 : node4;
            Node otherNode = assignedNode.equals(node3) ? node4 : node3;
            assertThat(assignments.get(assignedNode), equalTo(1));
            assertThat(assignments.get(otherNode), is(nullValue()));
        }
        {
            assertThat(plan.assignments(deployment4).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(deployment4).get();
            assertThat(assignments.get(node1), is(nullValue()));
            assertThat(assignments.get(node2), is(nullValue()));
            // Will either be on node 3 or 4
            Node assignedNode = assignments.get(node3) != null ? node3 : node4;
            Node otherNode = assignedNode.equals(node3) ? node4 : node3;
            assertThat(assignments.get(assignedNode), equalTo(2));
            assertThat(assignments.get(otherNode), is(nullValue()));
        }
    }

    public void testModelWithMoreAllocationsThanAvailableCores_GivenThreeThreadsPerAllocation() {
        Deployment deployment = new AssignmentPlan.Deployment("m_1", 30, 10, 3, Map.of(), 0);
        // Single node
        {
            Node node = new Node("n_1", 100, 4);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node), List.of(deployment)).computePlan();
            assertThat(assignmentPlan.assignments(deployment).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(deployment).get();
            assertThat(assignments.get(node), equalTo(1));
        }
        // Two nodes
        {
            Node node1 = new Node("n_1", 100, 4);
            Node node2 = new Node("n_2", 100, 8);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2), List.of(deployment)).computePlan();
            assertThat(assignmentPlan.assignments(deployment).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(deployment).get();
            assertThat(assignments.get(node1), equalTo(1));
            assertThat(assignments.get(node2), equalTo(2));
        }
        // Three nodes
        {
            Node node1 = new Node("n_1", 100, 4);
            Node node2 = new Node("n_2", 100, 7);
            Node node3 = new Node("n_3", 100, 15);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2, node3), List.of(deployment)).computePlan();
            assertThat(assignmentPlan.assignments(deployment).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(deployment).get();
            assertThat(assignments.get(node1), equalTo(1));
            assertThat(assignments.get(node2), equalTo(2));
            assertThat(assignments.get(node3), equalTo(5));
        }
    }

    public void testModelWithPreviousAssignmentAndNoMoreCoresAvailable() {
        Node node = new Node("n_1", 100, 4);
        AssignmentPlan.Deployment deployment = new AssignmentPlan.Deployment("m_1", 30, 4, 1, Map.of("n_1", 4), 0);
        AssignmentPlan plan = new AssignmentPlanner(List.of(node), List.of(deployment)).computePlan();

        assertThat(plan.assignments(deployment).isPresent(), is(true));
        assertThat(plan.assignments(deployment).get(), equalTo(Map.of(node, 4)));
    }

    public void testFullCoreUtilization_GivenModelsWithSingleThreadPerAllocation() {
        List<Node> nodes = List.of(
            new Node("n_1", ByteSizeValue.ofGb(6).getBytes(), 8),
            new Node("n_2", ByteSizeValue.ofGb(6).getBytes(), 8),
            new Node("n_3", ByteSizeValue.ofGb(6).getBytes(), 8),
            new Node("n_4", ByteSizeValue.ofGb(6).getBytes(), 8),
            new Node("n_5", ByteSizeValue.ofGb(16).getBytes(), 16),
            new Node("n_6", ByteSizeValue.ofGb(8).getBytes(), 16)
        );
        List<Deployment> deployments = List.of(
            new Deployment("m_1", ByteSizeValue.ofGb(4).getBytes(), 10, 1, Map.of("n_1", 5), 0),
            new AssignmentPlan.Deployment("m_2", ByteSizeValue.ofGb(2).getBytes(), 3, 1, Map.of("n_3", 2), 0),
            new AssignmentPlan.Deployment("m_3", ByteSizeValue.ofGb(3).getBytes(), 3, 1, Map.of(), 0),
            new Deployment("m_4", ByteSizeValue.ofGb(1).getBytes(), 4, 1, Map.of("n_3", 2), 0),
            new Deployment("m_5", ByteSizeValue.ofGb(6).getBytes(), 2, 1, Map.of(), 0),
            new Deployment("m_6", ByteSizeValue.ofGb(1).getBytes(), 12, 1, Map.of(), 0),
            new AssignmentPlan.Deployment("m_7", ByteSizeValue.ofGb(1).getBytes() / 2, 12, 1, Map.of("n_2", 6), 0),
            new Deployment("m_8", ByteSizeValue.ofGb(2).getBytes(), 4, 1, Map.of(), 0),
            new Deployment("m_9", ByteSizeValue.ofGb(1).getBytes(), 4, 1, Map.of(), 0),
            new AssignmentPlan.Deployment("m_10", ByteSizeValue.ofGb(7).getBytes(), 7, 1, Map.of(), 0),
            new Deployment("m_11", ByteSizeValue.ofGb(2).getBytes(), 3, 1, Map.of(), 0),
            new Deployment("m_12", ByteSizeValue.ofGb(1).getBytes(), 10, 1, Map.of(), 0)
        );

        AssignmentPlan assignmentPlan = new AssignmentPlanner(nodes, deployments).computePlan();

        int usedCores = 0;
        for (AssignmentPlan.Deployment m : deployments) {
            Map<Node, Integer> assignments = assignmentPlan.assignments(m).orElse(Map.of());
            usedCores += assignments.values().stream().mapToInt(Integer::intValue).sum();
        }
        assertThat(usedCores, equalTo(64));

        assertPreviousAssignmentsAreSatisfied(deployments, assignmentPlan);
    }

    public void testTooManyNodesAndModels_DoesNotThrowOOM_GivenNodesJustUnderLimit() {
        runTooManyNodesAndModels(3161, 1);
    }

    public void testTooManyNodesAndModels_DoesNotThrowOOM_GivenNodesJustOverLimit() {
        runTooManyNodesAndModels(3162, 1);
    }

    public void testTooManyNodesAndModels_DoesNotThrowOOM_GivenModelsJustUnderLimit() {
        runTooManyNodesAndModels(1, 3161);
    }

    public void testTooManyNodesAndModels_DoesNotThrowOOM_GivenModelsJustOverLimit() {
        runTooManyNodesAndModels(1, 3162);
    }

    public void testTooManyNodesAndModels_DoesNotThrowOOM_GivenComboJustUnderLimit() {
        runTooManyNodesAndModels(170, 171);
    }

    public void testTooManyNodesAndModels_DoesNotThrowOOM_GivenComboJustOverLimit() {
        runTooManyNodesAndModels(171, 171);
    }

    public void testTooManyNodesAndModels_DoesNotThrowOOM_GivenComboWayOverLimit() {
        runTooManyNodesAndModels(1000, 1000);
    }

    public void testRandomBenchmark() {
        List<Long> times = new ArrayList<>();
        List<Integer> nodeSizes = new ArrayList<>();
        List<Integer> modelSizes = new ArrayList<>();
        List<Double> coreUtilization = new ArrayList<>();
        List<Double> allocationPercent = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int scale = randomIntBetween(0, 10);
            double load = randomDoubleBetween(0.1, 1.0, true);
            List<Node> nodes = randomNodes(scale);
            List<Deployment> deployments = randomModels(scale, load);
            nodeSizes.add(nodes.size());
            modelSizes.add(deployments.size());
            logger.debug("Nodes = " + nodes.size() + "; Models = " + deployments.size());
            AssignmentPlanner solver = new AssignmentPlanner(nodes, deployments);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            AssignmentPlan assignmentPlan = solver.computePlan();
            stopWatch.stop();

            Quality quality = computeQuality(nodes, deployments, assignmentPlan);
            if (quality.coreUtilization > 0) {
                coreUtilization.add(quality.coreUtilization);
            }
            if (quality.allocationRate > 0) {
                allocationPercent.add(quality.allocationRate);
            }

            times.add(stopWatch.totalTime().millis());
        }
        double avgCoreUtilization = coreUtilization.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
        logger.debug("BC time = " + times.stream().mapToLong(Long::longValue).min().getAsLong() + " ms");
        logger.debug("WC time = " + times.stream().mapToLong(Long::longValue).max().getAsLong() + " ms");
        logger.debug("Avg time = " + times.stream().mapToLong(Long::longValue).average().getAsDouble() + " ms");
        logger.debug("Avg nodes = " + nodeSizes.stream().mapToLong(Integer::intValue).average().getAsDouble());
        logger.debug("Avg models = " + modelSizes.stream().mapToLong(Integer::intValue).average().getAsDouble());
        logger.debug("Avg core utilization = " + avgCoreUtilization);
        logger.debug("WC core utilization = " + coreUtilization.stream().mapToDouble(Double::doubleValue).min().getAsDouble());
        logger.debug("Avg allocation % = " + allocationPercent.stream().mapToDouble(Double::doubleValue).average().getAsDouble());
        logger.debug("WC allocation % = " + allocationPercent.stream().mapToDouble(Double::doubleValue).min().getAsDouble());
        assertThat(avgCoreUtilization, greaterThanOrEqualTo(0.95));
    }

    public void testPreviousAssignmentsGetAtLeastAsManyAllocationsAfterAddingNewModel() {
        int scale = randomIntBetween(0, 10);
        double load = randomDoubleBetween(0.1, 1.0, true);
        List<Node> nodes = randomNodes(scale);
        List<AssignmentPlan.Deployment> deployments = randomModels(scale, load);
        AssignmentPlan originalPlan = new AssignmentPlanner(nodes, deployments).computePlan();

        List<Deployment> previousModelsPlusNew = new ArrayList<>(deployments.size() + 1);
        for (Deployment m : deployments) {
            Map<Node, Integer> assignments = originalPlan.assignments(m).orElse(Map.of());
            Map<String, Integer> previousAssignments = assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().id(), Map.Entry::getValue));
            previousModelsPlusNew.add(
                new AssignmentPlan.Deployment(m.id(), m.memoryBytes(), m.allocations(), m.threadsPerAllocation(), previousAssignments, 0)
            );
        }
        previousModelsPlusNew.add(randomModel("new"));

        AssignmentPlan assignmentPlan = new AssignmentPlanner(nodes, previousModelsPlusNew).computePlan();

        assertPreviousAssignmentsAreSatisfied(previousModelsPlusNew, assignmentPlan);
    }

    public void testGivenLargerModelWithPreviousAssignmentsAndSmallerModelWithoutAssignments() {
        Node node1 = new Node("n_1", ByteSizeValue.ofGb(2).getBytes(), 2);
        Node node2 = new Node("n_2", ByteSizeValue.ofGb(2).getBytes(), 2);
        Node node3 = new Node("n_3", ByteSizeValue.ofGb(2).getBytes(), 2);
        Deployment deployment1 = new AssignmentPlan.Deployment(
            "m_1",
            ByteSizeValue.ofMb(1200).getBytes(),
            3,
            1,
            Map.of("n_1", 2, "n_2", 1),
            0
        );
        Deployment deployment2 = new Deployment("m_2", ByteSizeValue.ofMb(1100).getBytes(), 2, 1, Map.of(), 0);
        AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2, node3), List.of(deployment1, deployment2))
            .computePlan();
        {
            assertThat(assignmentPlan.assignments(deployment1).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(deployment1).get();
            assertThat(assignments.get(node1), equalTo(2));
            assertThat(assignments.get(node2), equalTo(1));
            assertThat(assignments.get(node3), is(nullValue()));
        }
        {
            assertThat(assignmentPlan.assignments(deployment2).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(deployment2).get();
            assertThat(assignments.get(node1), is(nullValue()));
            assertThat(assignments.get(node2), is(nullValue()));
            assertThat(assignments.get(node3), equalTo(2));
        }
    }

    public void testModelWithoutCurrentAllocationsGetsAssignedIfAllocatedPreviously() {
        Node node1 = new Node("n_1", ByteSizeValue.ofGb(4).getBytes(), 2);
        Node node2 = new Node("n_2", ByteSizeValue.ofGb(4).getBytes(), 2);
        AssignmentPlan.Deployment deployment1 = new Deployment(
            "m_1",
            ByteSizeValue.ofMb(1200).getBytes(),
            3,
            1,
            Map.of("n_1", 2, "n_2", 1),
            3
        );
        AssignmentPlan.Deployment deployment2 = new AssignmentPlan.Deployment(
            "m_2",
            ByteSizeValue.ofMb(1100).getBytes(),
            1,
            2,
            Map.of(),
            1
        );

        AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2), List.of(deployment1, deployment2)).computePlan();

        Map<String, Map<String, Integer>> indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1", "m_2"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));
        assertThat(indexedBasedPlan.get("m_2"), equalTo(Map.of("n_2", 1)));
    }

    public void testGivenPreviouslyAssignedModels_CannotAllBeAllocated() {
        Node node1 = new Node("n_1", ByteSizeValue.ofGb(2).getBytes(), 2);
        AssignmentPlan.Deployment deployment1 = new Deployment("m_1", ByteSizeValue.ofMb(1200).getBytes(), 1, 1, Map.of(), 1);
        AssignmentPlan.Deployment deployment2 = new Deployment("m_2", ByteSizeValue.ofMb(1100).getBytes(), 1, 1, Map.of(), 1);

        AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1), List.of(deployment1, deployment2)).computePlan();

        assertThat(assignmentPlan.countPreviouslyAssignedModelsThatAreStillAssigned(), equalTo(1L));
    }

    public void testGivenClusterResize_ShouldAllocateEachModelAtLeastOnce() {
        Node node1 = new Node("n_1", ByteSizeValue.ofMb(1200).getBytes(), 2);
        Node node2 = new Node("n_2", ByteSizeValue.ofMb(1200).getBytes(), 2);
        Deployment deployment1 = new Deployment("m_1", ByteSizeValue.ofMb(800).getBytes(), 2, 1, Map.of(), 0);
        Deployment deployment2 = new AssignmentPlan.Deployment("m_2", ByteSizeValue.ofMb(800).getBytes(), 1, 1, Map.of(), 0);
        Deployment deployment3 = new Deployment("m_3", ByteSizeValue.ofMb(250).getBytes(), 4, 1, Map.of(), 0);

        // First only start m_1
        AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2), List.of(deployment1)).computePlan();

        Map<String, Map<String, Integer>> indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));

        // Then start m_2
        assignmentPlan = new AssignmentPlanner(
            List.of(node1, node2),
            Stream.concat(createModelsFromPlan(assignmentPlan).stream(), Stream.of(deployment2)).toList()
        ).computePlan();

        indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1", "m_2"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));
        assertThat(indexedBasedPlan.get("m_2"), equalTo(Map.of("n_2", 1)));

        // Then start m_3
        assignmentPlan = new AssignmentPlanner(
            List.of(node1, node2),
            Stream.concat(createModelsFromPlan(assignmentPlan).stream(), Stream.of(deployment3)).toList()
        ).computePlan();

        indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1", "m_2", "m_3"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));
        assertThat(indexedBasedPlan.get("m_2"), equalTo(Map.of("n_2", 1)));
        assertThat(indexedBasedPlan.get("m_3"), equalTo(Map.of("n_2", 1)));

        // Now the cluster starts getting resized.
        Node node3 = new Node("n_3", ByteSizeValue.ofMb(2400).getBytes(), 2);
        Node node4 = new Node("n_4", ByteSizeValue.ofMb(2400).getBytes(), 2);

        // First, one node goes away.
        assignmentPlan = new AssignmentPlanner(List.of(node1), createModelsFromPlan(assignmentPlan)).computePlan();

        // Then, a node double in memory size is added.
        assignmentPlan = new AssignmentPlanner(List.of(node1, node3), createModelsFromPlan(assignmentPlan)).computePlan();
        // And another.
        assignmentPlan = new AssignmentPlanner(List.of(node1, node3, node4), createModelsFromPlan(assignmentPlan)).computePlan();
        // Finally, the remaining smaller node is removed
        assignmentPlan = new AssignmentPlanner(List.of(node3, node4), createModelsFromPlan(assignmentPlan)).computePlan();

        indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1", "m_2", "m_3"));
        assertThat(indexedBasedPlan.get("m_1").values().stream().mapToInt(Integer::intValue).sum(), greaterThanOrEqualTo(1));
        assertThat(indexedBasedPlan.get("m_2").values().stream().mapToInt(Integer::intValue).sum(), greaterThanOrEqualTo(1));
        assertThat(indexedBasedPlan.get("m_3").values().stream().mapToInt(Integer::intValue).sum(), greaterThanOrEqualTo(1));

        // Assert that all cores are utilized
        assertThat(assignmentPlan.getRemainingNodeCores("n_1"), equalTo(0));
        assertThat(assignmentPlan.getRemainingNodeCores("n_2"), equalTo(0));
    }

    public static List<Deployment> createModelsFromPlan(AssignmentPlan plan) {
        List<Deployment> deployments = new ArrayList<>();
        for (Deployment m : plan.models()) {
            Optional<Map<Node, Integer>> assignments = plan.assignments(m);
            Map<String, Integer> currentAllocations = Map.of();
            if (assignments.isPresent()) {
                currentAllocations = new HashMap<>();
                for (Map.Entry<Node, Integer> nodeAssignments : assignments.get().entrySet()) {
                    currentAllocations.put(nodeAssignments.getKey().id(), nodeAssignments.getValue());
                }
            }
            int totalAllocations = currentAllocations.values().stream().mapToInt(Integer::intValue).sum();
            deployments.add(
                new Deployment(
                    m.id(),
                    m.memoryBytes(),
                    m.allocations(),
                    m.threadsPerAllocation(),
                    currentAllocations,
                    Math.max(m.maxAssignedAllocations(), totalAllocations)
                )
            );
        }
        return deployments;
    }

    public static Map<String, Map<String, Integer>> convertToIdIndexed(AssignmentPlan plan) {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        for (AssignmentPlan.Deployment m : plan.models()) {
            Optional<Map<Node, Integer>> assignments = plan.assignments(m);
            Map<String, Integer> allocationsPerNodeId = assignments.isPresent() ? new HashMap<>() : Map.of();
            for (Map.Entry<Node, Integer> nodeAssignments : assignments.orElse(Map.of()).entrySet()) {
                allocationsPerNodeId.put(nodeAssignments.getKey().id(), nodeAssignments.getValue());
            }
            result.put(m.id(), allocationsPerNodeId);
        }
        return result;
    }

    public static void assertModelFullyAssignedToNode(AssignmentPlan plan, Deployment m, Node n) {
        Optional<Map<Node, Integer>> assignments = plan.assignments(m);
        assertThat(assignments.isPresent(), is(true));
        assertThat(assignments.get().size(), equalTo(1));
        assertThat(assignments.get().get(n), equalTo(m.allocations()));
    }

    public static List<Node> randomNodes(int scale) {
        return randomNodes(scale, "");
    }

    public static List<Node> randomNodes(int scale, String nodeIdPrefix) {
        Long[] memBytesPerCoreValues = {
            ByteSizeValue.ofGb(1).getBytes() / 2,
            ByteSizeValue.ofGb(1).getBytes(),
            ByteSizeValue.ofGb(2).getBytes(),
            ByteSizeValue.ofGb(3).getBytes(),
            ByteSizeValue.ofGb(4).getBytes() };

        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 1 + 3 * scale; i++) {
            int cores = randomIntBetween(2, 32);
            long memBytesPerCore = randomFrom(memBytesPerCoreValues);
            nodes.add(new Node(nodeIdPrefix + "n_" + i, cores * memBytesPerCore, cores));
        }
        return nodes;
    }

    public static List<Deployment> randomModels(int scale, double load) {
        List<Deployment> deployments = new ArrayList<>();
        for (int i = 0; i < Math.max(2, Math.round(load * (1 + 8 * scale))); i++) {
            deployments.add(randomModel(String.valueOf(i)));
        }
        return deployments;
    }

    public static Deployment randomModel(String idSuffix) {
        int allocations = randomIntBetween(1, 32);
        return new Deployment(
            "m_" + idSuffix,
            randomLongBetween(ByteSizeValue.ofMb(100).getBytes(), ByteSizeValue.ofGb(10).getBytes()),
            randomIntBetween(1, 32),
            randomIntBetween(1, 4),
            Map.of(),
            0
        );
    }

    public static void assertPreviousAssignmentsAreSatisfied(List<AssignmentPlan.Deployment> deployments, AssignmentPlan assignmentPlan) {
        for (Deployment m : deployments.stream().filter(m -> m.currentAllocationsByNodeId().isEmpty() == false).toList()) {
            Map<Node, Integer> assignments = assignmentPlan.assignments(m).get();
            Set<String> assignedNodeIds = new HashSet<>();
            int allocations = 0;
            for (Map.Entry<Node, Integer> e : assignments.entrySet()) {
                assignedNodeIds.add(e.getKey().id());
                if (m.currentAllocationsByNodeId().containsKey(e.getKey().id())) {
                    assertThat(e.getValue(), greaterThanOrEqualTo(1));
                }
                allocations += e.getValue();
            }
            assertThat(m.currentAllocationsByNodeId().keySet(), everyItem(in(assignedNodeIds)));
            assertThat(allocations, greaterThanOrEqualTo(m.getCurrentAssignedAllocations()));
        }
    }

    private void runTooManyNodesAndModels(int nodesSize, int modelsSize) {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < nodesSize; i++) {
            nodes.add(new Node("n_" + i, ByteSizeValue.ofGb(6).getBytes(), 100));
        }
        List<Deployment> deployments = new ArrayList<>();
        for (int i = 0; i < modelsSize; i++) {
            deployments.add(new Deployment("m_" + i, ByteSizeValue.ofMb(200).getBytes(), 2, 1, Map.of(), 0));
        }

        // Check plan is computed without OOM exception
        new AssignmentPlanner(nodes, deployments).computePlan();
    }

    private static Quality computeQuality(List<Node> nodes, List<Deployment> deployments, AssignmentPlan assignmentPlan) {
        final int totalCores = nodes.stream().map(Node::cores).mapToInt(Integer::intValue).sum();
        final int totalAllocationRequired = deployments.stream()
            .map(AssignmentPlan.Deployment::allocations)
            .mapToInt(Integer::intValue)
            .sum();
        int usedCores = 0;
        int assignedAllocations = 0;
        for (Deployment m : deployments) {
            for (Map.Entry<Node, Integer> assignment : assignmentPlan.assignments(m).orElse(Map.of()).entrySet()) {
                assignedAllocations += assignment.getValue();
                usedCores += assignment.getValue() * m.threadsPerAllocation();
            }
        }
        return new Quality(
            (double) usedCores / Math.min(totalCores, deployments.stream().mapToInt(m -> m.allocations() * m.threadsPerAllocation()).sum()),
            (double) assignedAllocations / totalAllocationRequired
        );
    }

    private record Quality(double coreUtilization, double allocationRate) {}
}
