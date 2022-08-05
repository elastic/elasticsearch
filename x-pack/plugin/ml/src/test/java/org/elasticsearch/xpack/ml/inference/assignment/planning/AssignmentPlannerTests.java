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
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Model;
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
        Model model = new Model("m_1", 101, 4, 1, Map.of(), 0);
        AssignmentPlan plan = new AssignmentPlanner(nodes, List.of(model)).computePlan();
        assertThat(plan.assignments(model).isEmpty(), is(true));
    }

    public void testModelWithThreadsPerAllocationNotFittingOnAnyNode() {
        List<Node> nodes = List.of(new Node("n_1", 100, 4), new Node("n_2", 100, 5));
        Model model = new Model("m_1", 1, 1, 6, Map.of(), 0);
        AssignmentPlan plan = new AssignmentPlanner(nodes, List.of(model)).computePlan();
        assertThat(plan.assignments(model).isEmpty(), is(true));
    }

    public void testSingleModelThatFitsFullyOnSingleNode() {
        {
            Node node = new Node("n_1", 100, 4);
            Model model = new Model("m_1", 100, 1, 1, Map.of(), 0);
            AssignmentPlan plan = new AssignmentPlanner(List.of(node), List.of(model)).computePlan();
            assertModelFullyAssignedToNode(plan, model, node);
        }
        {
            Node node = new Node("n_1", 1000, 8);
            Model model = new Model("m_1", 1000, 8, 1, Map.of(), 0);
            AssignmentPlan plan = new AssignmentPlanner(List.of(node), List.of(model)).computePlan();
            assertModelFullyAssignedToNode(plan, model, node);
        }
        {
            Node node = new Node("n_1", 10000, 16);
            Model model = new Model("m_1", 10000, 1, 16, Map.of(), 0);
            AssignmentPlan plan = new AssignmentPlanner(List.of(node), List.of(model)).computePlan();
            assertModelFullyAssignedToNode(plan, model, node);
        }
    }

    public void testSingleModelThatFitsFullyOnSingleNode_GivenTwoNodes_ShouldBeFullyAssignedOnOneNode() {
        Node node1 = new Node("n_1", 100, 4);
        Node node2 = new Node("n_2", 100, 4);
        Model model = new Model("m_1", 100, 4, 1, Map.of(), 0);

        AssignmentPlan plan = new AssignmentPlanner(List.of(node1, node2), List.of(model)).computePlan();

        Map<Node, Integer> assignments = plan.assignments(model).get();
        if (assignments.get(node1) > 0) {
            assertThat(assignments.get(node1), equalTo(4));
        } else {
            assertThat(assignments.get(node2), equalTo(4));
        }
    }

    public void testModelWithMoreAllocationsThanAvailableCores_GivenSingleThreadPerAllocation() {
        Model model = new Model("m_1", 30, 10, 1, Map.of(), 0);
        // Single node
        {
            Node node = new Node("n_1", 100, 4);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node), List.of(model)).computePlan();
            assertThat(assignmentPlan.assignments(model).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(model).get();
            assertThat(assignments.get(node), equalTo(4));
        }
        // Two nodes
        {
            Node node1 = new Node("n_1", 100, 4);
            Node node2 = new Node("n_2", 100, 2);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2), List.of(model)).computePlan();
            assertThat(assignmentPlan.assignments(model).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(model).get();
            assertThat(assignments.get(node1), equalTo(4));
            assertThat(assignments.get(node2), equalTo(2));
        }
        // Three nodes
        {
            Node node1 = new Node("n_1", 100, 4);
            Node node2 = new Node("n_2", 100, 2);
            Node node3 = new Node("n_3", 100, 3);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2, node3), List.of(model)).computePlan();
            assertThat(assignmentPlan.assignments(model).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(model).get();
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
        Model model1 = new Model("m_1", 50, 2, 4, Map.of(), 0);
        Model model2 = new Model("m_2", 50, 2, 3, Map.of(), 0);
        Model model3 = new Model("m_3", 50, 1, 2, Map.of(), 0);
        Model model4 = new Model("m_4", 50, 2, 1, Map.of(), 0);

        AssignmentPlan plan = new AssignmentPlanner(List.of(node1, node2, node3, node4), List.of(model1, model2, model3, model4))
            .computePlan();

        {
            assertThat(plan.assignments(model1).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(model1).get();
            assertThat(assignments.get(node1), equalTo(1));
            assertThat(assignments.get(node2), equalTo(1));
            assertThat(assignments.get(node3), is(nullValue()));
            assertThat(assignments.get(node4), is(nullValue()));
        }
        {
            assertThat(plan.assignments(model2).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(model2).get();
            assertThat(assignments.get(node1), equalTo(1));
            assertThat(assignments.get(node2), equalTo(1));
            assertThat(assignments.get(node3), is(nullValue()));
            assertThat(assignments.get(node4), is(nullValue()));
        }
        {
            assertThat(plan.assignments(model3).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(model3).get();
            assertThat(assignments.get(node1), is(nullValue()));
            assertThat(assignments.get(node2), is(nullValue()));
            // Will either be on node 3 or 4
            Node assignedNode = assignments.get(node3) != null ? node3 : node4;
            Node otherNode = assignedNode.equals(node3) ? node4 : node3;
            assertThat(assignments.get(assignedNode), equalTo(1));
            assertThat(assignments.get(otherNode), is(nullValue()));
        }
        {
            assertThat(plan.assignments(model4).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(model4).get();
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
        Model model = new Model("m_1", 30, 10, 3, Map.of(), 0);
        // Single node
        {
            Node node = new Node("n_1", 100, 4);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node), List.of(model)).computePlan();
            assertThat(assignmentPlan.assignments(model).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(model).get();
            assertThat(assignments.get(node), equalTo(1));
        }
        // Two nodes
        {
            Node node1 = new Node("n_1", 100, 4);
            Node node2 = new Node("n_2", 100, 8);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2), List.of(model)).computePlan();
            assertThat(assignmentPlan.assignments(model).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(model).get();
            assertThat(assignments.get(node1), equalTo(1));
            assertThat(assignments.get(node2), equalTo(2));
        }
        // Three nodes
        {
            Node node1 = new Node("n_1", 100, 4);
            Node node2 = new Node("n_2", 100, 7);
            Node node3 = new Node("n_3", 100, 15);
            AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2, node3), List.of(model)).computePlan();
            assertThat(assignmentPlan.assignments(model).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(model).get();
            assertThat(assignments.get(node1), equalTo(1));
            assertThat(assignments.get(node2), equalTo(2));
            assertThat(assignments.get(node3), equalTo(5));
        }
    }

    public void testModelWithPreviousAssignmentAndNoMoreCoresAvailable() {
        Node node = new Node("n_1", 100, 4);
        Model model = new Model("m_1", 30, 4, 1, Map.of("n_1", 4), 0);
        AssignmentPlan plan = new AssignmentPlanner(List.of(node), List.of(model)).computePlan();

        assertThat(plan.assignments(model).isPresent(), is(true));
        assertThat(plan.assignments(model).get(), equalTo(Map.of(node, 4)));
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
        List<Model> models = List.of(
            new Model("m_1", ByteSizeValue.ofGb(4).getBytes(), 10, 1, Map.of("n_1", 5), 0),
            new Model("m_2", ByteSizeValue.ofGb(2).getBytes(), 3, 1, Map.of("n_3", 2), 0),
            new Model("m_3", ByteSizeValue.ofGb(3).getBytes(), 3, 1, Map.of(), 0),
            new Model("m_4", ByteSizeValue.ofGb(1).getBytes(), 4, 1, Map.of("n_3", 2), 0),
            new Model("m_5", ByteSizeValue.ofGb(6).getBytes(), 2, 1, Map.of(), 0),
            new Model("m_6", ByteSizeValue.ofGb(1).getBytes(), 12, 1, Map.of(), 0),
            new Model("m_7", ByteSizeValue.ofGb(1).getBytes() / 2, 12, 1, Map.of("n_2", 6), 0),
            new Model("m_8", ByteSizeValue.ofGb(2).getBytes(), 4, 1, Map.of(), 0),
            new Model("m_9", ByteSizeValue.ofGb(1).getBytes(), 4, 1, Map.of(), 0),
            new Model("m_10", ByteSizeValue.ofGb(7).getBytes(), 7, 1, Map.of(), 0),
            new Model("m_11", ByteSizeValue.ofGb(2).getBytes(), 3, 1, Map.of(), 0),
            new Model("m_12", ByteSizeValue.ofGb(1).getBytes(), 10, 1, Map.of(), 0)
        );

        AssignmentPlan assignmentPlan = new AssignmentPlanner(nodes, models).computePlan();

        int usedCores = 0;
        for (Model m : models) {
            Map<Node, Integer> assignments = assignmentPlan.assignments(m).orElse(Map.of());
            usedCores += assignments.values().stream().mapToInt(Integer::intValue).sum();
        }
        assertThat(usedCores, equalTo(64));

        assertPreviousAssignmentsAreSatisfied(models, assignmentPlan);
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
            List<Model> models = randomModels(scale, load, nodes);
            nodeSizes.add(nodes.size());
            modelSizes.add(models.size());
            logger.debug("Nodes = " + nodes.size() + "; Models = " + models.size());
            AssignmentPlanner solver = new AssignmentPlanner(nodes, models);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            AssignmentPlan assignmentPlan = solver.computePlan();
            stopWatch.stop();

            Quality quality = computeQuality(nodes, models, assignmentPlan);
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
        List<Model> models = randomModels(scale, load, nodes);
        AssignmentPlan originalPlan = new AssignmentPlanner(nodes, models).computePlan();

        List<Model> previousModelsPlusNew = new ArrayList<>(models.size() + 1);
        for (Model m : models) {
            Map<Node, Integer> assignments = originalPlan.assignments(m).orElse(Map.of());
            Map<String, Integer> previousAssignments = assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().id(), Map.Entry::getValue));
            previousModelsPlusNew.add(
                new Model(m.id(), m.memoryBytes(), m.allocations(), m.threadsPerAllocation(), previousAssignments, 0)
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
        Model model1 = new Model("m_1", ByteSizeValue.ofMb(1200).getBytes(), 3, 1, Map.of("n_1", 2, "n_2", 1), 0);
        Model model2 = new Model("m_2", ByteSizeValue.ofMb(1100).getBytes(), 2, 1, Map.of(), 0);
        AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2, node3), List.of(model1, model2)).computePlan();
        {
            assertThat(assignmentPlan.assignments(model1).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(model1).get();
            assertThat(assignments.get(node1), equalTo(2));
            assertThat(assignments.get(node2), equalTo(1));
            assertThat(assignments.get(node3), is(nullValue()));
        }
        {
            assertThat(assignmentPlan.assignments(model2).isPresent(), is(true));
            Map<Node, Integer> assignments = assignmentPlan.assignments(model2).get();
            assertThat(assignments.get(node1), is(nullValue()));
            assertThat(assignments.get(node2), is(nullValue()));
            assertThat(assignments.get(node3), equalTo(2));
        }
    }

    public void testModelWithoutCurrentAllocationsGetsAssignedIfAllocatedPreviously() {
        Node node1 = new Node("n_1", ByteSizeValue.ofGb(4).getBytes(), 2);
        Node node2 = new Node("n_2", ByteSizeValue.ofGb(4).getBytes(), 2);
        Model model1 = new Model("m_1", ByteSizeValue.ofMb(1200).getBytes(), 3, 1, Map.of("n_1", 2, "n_2", 1), 3);
        Model model2 = new Model("m_2", ByteSizeValue.ofMb(1100).getBytes(), 1, 2, Map.of(), 1);

        AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2), List.of(model1, model2)).computePlan();

        Map<String, Map<String, Integer>> indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1", "m_2"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));
        assertThat(indexedBasedPlan.get("m_2"), equalTo(Map.of("n_2", 1)));
    }

    public void testGivenPreviouslyAssignedModels_CannotAllBeAllocated() {
        Node node1 = new Node("n_1", ByteSizeValue.ofGb(2).getBytes(), 2);
        Model model1 = new Model("m_1", ByteSizeValue.ofMb(1200).getBytes(), 1, 1, Map.of(), 1);
        Model model2 = new Model("m_2", ByteSizeValue.ofMb(1100).getBytes(), 1, 1, Map.of(), 1);

        AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1), List.of(model1, model2)).computePlan();

        assertThat(assignmentPlan.countPreviouslyAssignedModelsThatAreStillAssigned(), equalTo(1L));
    }

    public void testGivenClusterResize_ShouldAllocateEachModelAtLeastOnce() {
        Node node1 = new Node("n_1", ByteSizeValue.ofMb(1200).getBytes(), 2);
        Node node2 = new Node("n_2", ByteSizeValue.ofMb(1200).getBytes(), 2);
        Model model1 = new Model("m_1", ByteSizeValue.ofMb(800).getBytes(), 2, 1, Map.of(), 0);
        Model model2 = new Model("m_2", ByteSizeValue.ofMb(800).getBytes(), 1, 1, Map.of(), 0);
        Model model3 = new Model("m_3", ByteSizeValue.ofMb(250).getBytes(), 4, 1, Map.of(), 0);

        // First only start m_1
        AssignmentPlan assignmentPlan = new AssignmentPlanner(List.of(node1, node2), List.of(model1)).computePlan();

        Map<String, Map<String, Integer>> indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));

        // Then start m_2
        assignmentPlan = new AssignmentPlanner(
            List.of(node1, node2),
            Stream.concat(createModelsFromPlan(assignmentPlan).stream(), Stream.of(model2)).toList()
        ).computePlan();

        indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1", "m_2"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));
        assertThat(indexedBasedPlan.get("m_2"), equalTo(Map.of("n_2", 1)));

        // Then start m_3
        assignmentPlan = new AssignmentPlanner(
            List.of(node1, node2),
            Stream.concat(createModelsFromPlan(assignmentPlan).stream(), Stream.of(model3)).toList()
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

    private static List<Model> createModelsFromPlan(AssignmentPlan plan) {
        List<Model> models = new ArrayList<>();
        for (Model m : plan.models()) {
            Optional<Map<Node, Integer>> assignments = plan.assignments(m);
            Map<String, Integer> currentAllocations = Map.of();
            if (assignments.isPresent()) {
                currentAllocations = new HashMap<>();
                for (Map.Entry<Node, Integer> nodeAssignments : assignments.get().entrySet()) {
                    currentAllocations.put(nodeAssignments.getKey().id(), nodeAssignments.getValue());
                }
            }
            int totalAllocations = currentAllocations.values().stream().mapToInt(Integer::intValue).sum();
            models.add(
                new Model(
                    m.id(),
                    m.memoryBytes(),
                    m.allocations(),
                    m.threadsPerAllocation(),
                    currentAllocations,
                    Math.max(m.maxAssignedAllocations(), totalAllocations)
                )
            );
        }
        return models;
    }

    private static Map<String, Map<String, Integer>> convertToIdIndexed(AssignmentPlan plan) {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        for (Model m : plan.models()) {
            Optional<Map<Node, Integer>> assignments = plan.assignments(m);
            Map<String, Integer> allocationsPerNodeId = assignments.isPresent() ? new HashMap<>() : Map.of();
            for (Map.Entry<Node, Integer> nodeAssignments : assignments.orElse(Map.of()).entrySet()) {
                allocationsPerNodeId.put(nodeAssignments.getKey().id(), nodeAssignments.getValue());
            }
            result.put(m.id(), allocationsPerNodeId);
        }
        return result;
    }

    private static void assertModelFullyAssignedToNode(AssignmentPlan plan, Model m, Node n) {
        Optional<Map<Node, Integer>> assignments = plan.assignments(m);
        assertThat(assignments.isPresent(), is(true));
        assertThat(assignments.get().size(), equalTo(1));
        assertThat(assignments.get().get(n), equalTo(m.allocations()));
    }

    private List<Node> randomNodes(int scale) {
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
            nodes.add(new Node("n_" + i, cores * memBytesPerCore, cores));
        }
        return nodes;
    }

    private List<Model> randomModels(int scale, double load, List<Node> nodes) {
        List<Model> models = new ArrayList<>();
        for (int i = 0; i < Math.max(2, Math.round(load * (1 + 8 * scale))); i++) {
            models.add(randomModel(String.valueOf(i)));
        }
        return models;
    }

    private static Model randomModel(String idSuffix) {
        int allocations = randomIntBetween(1, 32);
        return new Model(
            "m_" + idSuffix,
            randomLongBetween(ByteSizeValue.ofMb(100).getBytes(), ByteSizeValue.ofGb(10).getBytes()),
            randomIntBetween(1, 32),
            randomIntBetween(1, 4),
            Map.of(),
            0
        );
    }

    private static void assertPreviousAssignmentsAreSatisfied(List<Model> models, AssignmentPlan assignmentPlan) {
        for (Model m : models.stream().filter(m -> m.currentAllocationsByNodeId().isEmpty() == false).toList()) {
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
        List<Model> models = new ArrayList<>();
        for (int i = 0; i < modelsSize; i++) {
            models.add(new Model("m_" + i, ByteSizeValue.ofMb(200).getBytes(), 2, 1, Map.of(), 0));
        }

        // Check plan is computed without OOM exception
        new AssignmentPlanner(nodes, models).computePlan();
    }

    private static Quality computeQuality(List<Node> nodes, List<Model> models, AssignmentPlan assignmentPlan) {
        final int totalCores = nodes.stream().map(Node::cores).mapToInt(Integer::intValue).sum();
        final int totalAllocationRequired = models.stream().map(Model::allocations).mapToInt(Integer::intValue).sum();
        int usedCores = 0;
        int assignedAllocations = 0;
        for (Model m : models) {
            for (Map.Entry<Node, Integer> assignment : assignmentPlan.assignments(m).orElse(Map.of()).entrySet()) {
                assignedAllocations += assignment.getValue();
                usedCores += assignment.getValue() * m.threadsPerAllocation();
            }
        }
        return new Quality(
            (double) usedCores / Math.min(totalCores, models.stream().mapToInt(m -> m.allocations() * m.threadsPerAllocation()).sum()),
            (double) assignedAllocations / totalAllocationRequired
        );
    }

    private record Quality(double coreUtilization, double allocationRate) {}
}
