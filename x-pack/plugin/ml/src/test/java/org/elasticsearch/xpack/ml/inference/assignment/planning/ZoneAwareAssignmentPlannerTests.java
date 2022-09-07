/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Model;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlannerTests.assertModelFullyAssignedToNode;
import static org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlannerTests.assertPreviousAssignmentsAreSatisfied;
import static org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlannerTests.convertToIdIndexed;
import static org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlannerTests.createModelsFromPlan;
import static org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlannerTests.randomModel;
import static org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlannerTests.randomModels;
import static org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlannerTests.randomNodes;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ZoneAwareAssignmentPlannerTests extends ESTestCase {

    public void testGivenOneModel_OneNode_OneZone_DoesNotFit() {
        Node node = new Node("n_1", 100, 1);
        Model model = new Model("m_1", 100, 1, 2, Map.of(), 0);

        AssignmentPlan plan = new ZoneAwareAssignmentPlanner(Map.of(List.of(), List.of(node)), List.of(model)).computePlan();

        assertThat(plan.assignments(model).isEmpty(), is(true));
    }

    public void testGivenOneModel_OneNode_OneZone_FullyFits() {
        Node node = new Node("n_1", 100, 4);
        Model model = new Model("m_1", 100, 2, 2, Map.of(), 0);

        AssignmentPlan plan = new ZoneAwareAssignmentPlanner(Map.of(List.of(), List.of(node)), List.of(model)).computePlan();

        assertModelFullyAssignedToNode(plan, model, node);
    }

    public void testGivenOneModel_OneNode_OneZone_PartiallyFits() {
        Node node = new Node("n_1", 100, 5);
        Model model = new Model("m_1", 100, 3, 2, Map.of(), 0);

        AssignmentPlan plan = new ZoneAwareAssignmentPlanner(Map.of(List.of(), List.of(node)), List.of(model)).computePlan();

        Map<String, Map<String, Integer>> indexedBasedPlan = convertToIdIndexed(plan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));
    }

    public void testGivenOneModelWithSingleAllocation_OneNode_TwoZones() {
        Node node1 = new Node("n_1", 100, 4);
        Node node2 = new Node("n_2", 100, 4);
        Model model = new Model("m_1", 100, 1, 2, Map.of(), 0);

        AssignmentPlan plan = new ZoneAwareAssignmentPlanner(
            Map.of(List.of("z1"), List.of(node1), List.of("z2"), List.of(node2)),
            List.of(model)
        ).computePlan();

        assertThat(plan.satisfiesAllModels(), is(true));

        assertThat(plan.assignments(model).isPresent(), is(true));
        Map<Node, Integer> assignments = plan.assignments(model).get();
        assertThat(assignments.keySet(), hasSize(1));
        assertThat(assignments.get(assignments.keySet().iterator().next()), equalTo(1));
    }

    public void testGivenOneModel_OneNodePerZone_TwoZones_FullyFits() {
        Node node1 = new Node("n_1", 100, 4);
        Node node2 = new Node("n_2", 100, 4);
        Model model = new Model("m_1", 100, 2, 2, Map.of(), 0);

        AssignmentPlan plan = new ZoneAwareAssignmentPlanner(
            Map.of(List.of("z_1"), List.of(node1), List.of("z_2"), List.of(node2)),
            List.of(model)
        ).computePlan();

        assertThat(plan.satisfiesAllModels(), is(true));

        Map<String, Map<String, Integer>> indexedBasedPlan = convertToIdIndexed(plan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 1, "n_2", 1)));
    }

    public void testGivenOneModel_OneNodePerZone_TwoZones_PartiallyFits() {
        Node node1 = new Node("n_1", 100, 4);
        Node node2 = new Node("n_2", 100, 4);
        Model model = new Model("m_1", 100, 3, 3, Map.of(), 0);

        AssignmentPlan plan = new ZoneAwareAssignmentPlanner(
            Map.of(List.of("z_1"), List.of(node1), List.of("z_2"), List.of(node2)),
            List.of(model)
        ).computePlan();

        Map<String, Map<String, Integer>> indexedBasedPlan = convertToIdIndexed(plan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 1, "n_2", 1)));
    }

    public void testGivenThreeModels_TwoNodesPerZone_ThreeZones_FullyFit() {
        Node node1 = new Node("n_1", 100, 4);
        Node node2 = new Node("n_2", 100, 4);
        Node node3 = new Node("n_3", 100, 4);
        Node node4 = new Node("n_4", 100, 4);
        Node node5 = new Node("n_5", 100, 4);
        Node node6 = new Node("n_6", 100, 4);
        Model model1 = new Model("m_1", 25, 4, 1, Map.of(), 0);
        Model model2 = new Model("m_2", 25, 6, 2, Map.of(), 0);
        Model model3 = new Model("m_3", 25, 2, 3, Map.of(), 0);

        Map<List<String>, List<Node>> nodesByZone = Map.of(
            List.of("z_1"),
            List.of(node1, node2),
            List.of("z_2"),
            List.of(node3, node4),
            List.of("z_3"),
            List.of(node5, node6)
        );

        AssignmentPlan plan = new ZoneAwareAssignmentPlanner(nodesByZone, List.of(model1, model2, model3)).computePlan();

        assertThat(plan.satisfiesAllModels(), is(true));

        {
            assertThat(plan.assignments(model1).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(model1).get();
            for (List<Node> zoneNodes : nodesByZone.values()) {
                assertThat(Sets.haveNonEmptyIntersection(assignments.keySet(), zoneNodes.stream().collect(Collectors.toSet())), is(true));
            }
        }
        {
            assertThat(plan.assignments(model2).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(model2).get();
            for (List<Node> zoneNodes : nodesByZone.values()) {
                assertThat(Sets.haveNonEmptyIntersection(assignments.keySet(), zoneNodes.stream().collect(Collectors.toSet())), is(true));
            }
        }
        {
            assertThat(plan.assignments(model3).isPresent(), is(true));
            Map<Node, Integer> assignments = plan.assignments(model3).get();
            int zonesWithAllocations = 0;
            for (List<Node> zoneNodes : nodesByZone.values()) {
                if (Sets.haveNonEmptyIntersection(assignments.keySet(), zoneNodes.stream().collect(Collectors.toSet()))) {
                    zonesWithAllocations++;
                }
            }
            assertThat(zonesWithAllocations, equalTo(2));
        }
    }

    public void testGivenTwoModelsWithSingleAllocation_OneNode_ThreeZones() {
        Node node1 = new Node("n_1", 100, 4);
        Node node2 = new Node("n_2", 100, 4);
        Node node3 = new Node("n_3", 100, 4);
        Model model1 = new Model("m_1", 25, 1, 1, Map.of(), 0);
        Model model2 = new Model("m_2", 25, 1, 1, Map.of(), 0);

        AssignmentPlan plan = new ZoneAwareAssignmentPlanner(
            Map.of(List.of("z1"), List.of(node1), List.of("z2"), List.of(node2), List.of("z3"), List.of(node3)),
            List.of(model1, model2)
        ).computePlan();

        assertThat(plan.satisfiesAllModels(), is(true));
    }

    public void testPreviousAssignmentsGetAtLeastAsManyAllocationsAfterAddingNewModel() {
        int scale = randomIntBetween(0, 10);
        double load = randomDoubleBetween(0.1, 1.0, true);
        Map<List<String>, List<Node>> nodesByZone = Map.of(
            List.of("z_1"),
            randomNodes(scale, "z_1_"),
            List.of("z_2"),
            randomNodes(scale, "z_2_"),
            List.of("z_3"),
            randomNodes(scale, "z_3_")
        );
        List<Model> models = randomModels(scale, load);
        AssignmentPlan originalPlan = new ZoneAwareAssignmentPlanner(nodesByZone, models).computePlan();

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

        AssignmentPlan assignmentPlan = new ZoneAwareAssignmentPlanner(nodesByZone, previousModelsPlusNew).computePlan();

        assertPreviousAssignmentsAreSatisfied(previousModelsPlusNew, assignmentPlan);
    }

    public void testGivenClusterResize_GivenOneZone_ShouldAllocateEachModelAtLeastOnce() {
        Node node1 = new Node("n_1", ByteSizeValue.ofMb(1200).getBytes(), 2);
        Node node2 = new Node("n_2", ByteSizeValue.ofMb(1200).getBytes(), 2);
        Model model1 = new Model("m_1", ByteSizeValue.ofMb(800).getBytes(), 2, 1, Map.of(), 0);
        Model model2 = new Model("m_2", ByteSizeValue.ofMb(800).getBytes(), 1, 1, Map.of(), 0);
        Model model3 = new Model("m_3", ByteSizeValue.ofMb(250).getBytes(), 4, 1, Map.of(), 0);

        // First only start m_1
        AssignmentPlan assignmentPlan = new ZoneAwareAssignmentPlanner(Map.of(List.of(), List.of(node1, node2)), List.of(model1))
            .computePlan();

        Map<String, Map<String, Integer>> indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));

        // Then start m_2
        assignmentPlan = new ZoneAwareAssignmentPlanner(
            Map.of(List.of(), List.of(node1, node2)),
            Stream.concat(createModelsFromPlan(assignmentPlan).stream(), Stream.of(model2)).toList()
        ).computePlan();

        indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1", "m_2"));
        assertThat(indexedBasedPlan.get("m_1"), equalTo(Map.of("n_1", 2)));
        assertThat(indexedBasedPlan.get("m_2"), equalTo(Map.of("n_2", 1)));

        // Then start m_3
        assignmentPlan = new ZoneAwareAssignmentPlanner(
            Map.of(List.of(), List.of(node1, node2)),
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
        assignmentPlan = new ZoneAwareAssignmentPlanner(Map.of(List.of(), List.of(node1)), createModelsFromPlan(assignmentPlan))
            .computePlan();

        // Then, a node double in memory size is added.
        assignmentPlan = new ZoneAwareAssignmentPlanner(Map.of(List.of(), List.of(node1, node3)), createModelsFromPlan(assignmentPlan))
            .computePlan();
        // And another.
        assignmentPlan = new ZoneAwareAssignmentPlanner(
            Map.of(List.of(), List.of(node1, node3, node4)),
            createModelsFromPlan(assignmentPlan)
        ).computePlan();
        // Finally, the remaining smaller node is removed
        assignmentPlan = new ZoneAwareAssignmentPlanner(Map.of(List.of(), List.of(node3, node4)), createModelsFromPlan(assignmentPlan))
            .computePlan();

        indexedBasedPlan = convertToIdIndexed(assignmentPlan);
        assertThat(indexedBasedPlan.keySet(), hasItems("m_1", "m_2", "m_3"));
        assertThat(indexedBasedPlan.get("m_1").values().stream().mapToInt(Integer::intValue).sum(), greaterThanOrEqualTo(1));
        assertThat(indexedBasedPlan.get("m_2").values().stream().mapToInt(Integer::intValue).sum(), greaterThanOrEqualTo(1));
        assertThat(indexedBasedPlan.get("m_3").values().stream().mapToInt(Integer::intValue).sum(), greaterThanOrEqualTo(1));

        // Assert that all cores are utilized
        assertThat(assignmentPlan.getRemainingNodeCores("n_1"), equalTo(0));
        assertThat(assignmentPlan.getRemainingNodeCores("n_2"), equalTo(0));
    }
}
