/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A plan describing how models should be assigned to nodes.
 */
public class AssignmentPlan implements Comparable<AssignmentPlan> {

    public record Model(
        String id,
        long memoryBytes,
        int allocations,
        int threadsPerAllocation,
        Map<String, Integer> currentAllocationsByNodeId,
        int maxAssignedAllocations
    ) {

        int getCurrentAssignedAllocations() {
            return currentAllocationsByNodeId.values().stream().mapToInt(Integer::intValue).sum();
        }

        boolean hasEverBeenAllocated() {
            return maxAssignedAllocations > 0;
        }

        @Override
        public String toString() {
            return id
                + " (mem = "
                + ByteSizeValue.ofBytes(memoryBytes)
                + ") (allocations = "
                + allocations
                + ") (threads_per_allocation = "
                + threadsPerAllocation
                + ") (current_allocations = "
                + currentAllocationsByNodeId
                + ") (max_assigned_allocations = "
                + maxAssignedAllocations
                + ")";
        }
    };

    public record Node(String id, long availableMemoryBytes, int cores) {

        @Override
        public String toString() {
            return id + " (mem = " + ByteSizeValue.ofBytes(availableMemoryBytes) + ") (cores = " + cores + ")";
        }
    };

    /**
     * The model assignments to each node. Each assignment
     * is a {@link Map<Node, Integer>} where the value is the number
     * of allocations for each node.
     */
    private final Map<Model, Map<Node, Integer>> assignments;

    private final Map<String, Long> remainingNodeMemory;
    private final Map<String, Integer> remainingNodeCores;
    private final Map<Model, Integer> remainingModelAllocations;

    private AssignmentPlan(
        Map<Model, Map<Node, Integer>> assignments,
        Map<Node, Long> remainingNodeMemory,
        Map<Node, Integer> remainingNodeCores,
        Map<Model, Integer> remainingModelAllocations
    ) {
        this.assignments = Objects.requireNonNull(assignments);
        this.remainingNodeMemory = remainingNodeMemory.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey().id(), e -> e.getValue()));
        this.remainingNodeCores = remainingNodeCores.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().id(), e -> e.getValue()));
        this.remainingModelAllocations = Objects.requireNonNull(remainingModelAllocations);
    }

    public Set<Model> models() {
        return assignments.keySet();
    }

    /**
     * The assignments of that model to each node. Each assignment
     * is a {@link Map<Node, Integer>} where the value is the number
     * of allocations for each node.
     * @param model the model for which assignments are returned
     * @return the model assignments per node. The Optional will be empty if the model has no assignments.
     */
    public Optional<Map<Node, Integer>> assignments(Model model) {
        Map<Node, Integer> modelAssignments = assignments.get(model);
        return (modelAssignments == null || modelAssignments.isEmpty()) ? Optional.empty() : Optional.of(modelAssignments);
    }

    @Override
    public int compareTo(AssignmentPlan o) {
        return Comparator.comparing(AssignmentPlan::computeQuality).compare(this, o);
    }

    public boolean satisfiesCurrentAssignments() {
        return models().stream().allMatch(this::isSatisfyingCurrentAssignmentsForModel);
    }

    private boolean isSatisfyingCurrentAssignmentsForModel(Model m) {
        if (m.currentAllocationsByNodeId().isEmpty()) {
            return true;
        }
        Map<Node, Integer> nodeAssignments = assignments.get(m);
        int currentAllocations = nodeAssignments.values().stream().mapToInt(Integer::intValue).sum();
        return currentAllocations >= m.getCurrentAssignedAllocations();
    }

    public boolean satisfiesAllocations(Model m) {
        return remainingModelAllocations.getOrDefault(m, 0) == 0;
    }

    public boolean satisfiesAllModels() {
        return models().stream().allMatch(this::satisfiesAllocations);
    }

    public boolean arePreviouslyAssignedModelsAssigned() {
        return models().stream()
            .filter(Model::hasEverBeenAllocated)
            .map(this::totalAllocations)
            .allMatch(totalAllocations -> totalAllocations > 0);
    }

    public long countPreviouslyAssignedModelsThatAreStillAssigned() {
        return models().stream()
            .filter(Model::hasEverBeenAllocated)
            .map(this::totalAllocations)
            .filter(totalAllocations -> totalAllocations > 0)
            .count();
    }

    public int getRemainingNodeCores(String nodeId) {
        return remainingNodeCores.getOrDefault(nodeId, 0);
    }

    public long getRemainingNodeMemory(String nodeId) {
        return remainingNodeMemory.getOrDefault(nodeId, 0L);
    }

    public int totalAllocations(Model m) {
        if (assignments.containsKey(m) == false) {
            return 0;
        }
        return assignments.get(m).values().stream().mapToInt(Integer::intValue).sum();
    }

    private Quality computeQuality() {
        boolean isSatisfyingPreviousAssignments = true;
        double weighedAllocationsScore = 0;
        double memoryScore = 0;

        for (Map.Entry<Model, Map<Node, Integer>> entry : assignments.entrySet()) {
            Model m = entry.getKey();
            isSatisfyingPreviousAssignments = isSatisfyingPreviousAssignments && isSatisfyingCurrentAssignmentsForModel(m);
            Map<Node, Integer> modelAssignments = entry.getValue();
            if (modelAssignments != null) {
                for (Map.Entry<Node, Integer> nodeAllocations : modelAssignments.entrySet()) {
                    Node n = nodeAllocations.getKey();
                    weighedAllocationsScore += (1 + 0.1 * (m.currentAllocationsByNodeId().containsKey(n.id()) ? 1 : 0)) * modelAssignments
                        .get(n);
                    memoryScore -= (nodeAllocations.getValue() > 0 ? m.memoryBytes() : 0);
                }
            }
        }
        return new Quality(isSatisfyingPreviousAssignments, weighedAllocationsScore, memoryScore);
    }

    public String prettyPrint() {
        if (assignments.isEmpty()) {
            return "Empty plan";
        }

        Map<Node, List<Tuple<Model, Integer>>> nodeToModel = new HashMap<>();
        for (Model m : assignments.keySet()) {
            for (Node n : assignments.get(m).keySet()) {
                List<Tuple<Model, Integer>> allocationsPerModel = nodeToModel.containsKey(n) ? nodeToModel.get(n) : new ArrayList<>();
                allocationsPerModel.add(Tuple.tuple(m, assignments.get(m).get(n)));
                nodeToModel.put(n, allocationsPerModel);
            }
        }

        StringBuilder msg = new StringBuilder();
        List<Node> nodes = nodeToModel.keySet().stream().sorted(Comparator.comparing(Node::id)).collect(Collectors.toList());
        for (int i = 0; i < nodes.size(); i++) {
            Node n = nodes.get(i);
            msg.append(n);
            msg.append(" ->");
            for (Tuple<Model, Integer> modelAllocations : nodeToModel.get(n)
                .stream()
                .sorted(Comparator.comparing(x -> x.v1().id()))
                .collect(Collectors.toList())) {
                if (modelAllocations.v2() > 0) {
                    msg.append(" ");
                    msg.append(modelAllocations.v1().id());
                    msg.append(" (mem = ");
                    msg.append(ByteSizeValue.ofBytes(modelAllocations.v1().memoryBytes()));
                    msg.append(")");
                    msg.append(" (allocations = ");
                    msg.append(modelAllocations.v2());
                    msg.append("/");
                    msg.append(modelAllocations.v1().allocations());
                    msg.append(")");
                    msg.append(" (threads_per_allocation = ");
                    msg.append(modelAllocations.v1().threadsPerAllocation());
                    msg.append(")");
                }
            }
            if (i < nodes.size() - 1) {
                msg.append('\n');
            }
        }
        return msg.toString();
    }

    public static Builder builder(Collection<Node> nodes, Collection<Model> models) {
        return new Builder(nodes, models);
    }

    static class Builder {

        private final Map<Model, Map<Node, Integer>> assignments;
        private final Map<Node, Long> remainingNodeMemory;
        private final Map<Node, Integer> remainingNodeCores;
        private final Map<Model, Integer> remainingModelAllocations;

        private Builder(Collection<Node> nodes, Collection<Model> models) {
            if (nodes.stream().collect(Collectors.toSet()).size() != nodes.size()) {
                throw new IllegalArgumentException("there should be no duplicate nodes");
            }
            if (models.stream().collect(Collectors.toSet()).size() != models.size()) {
                throw new IllegalArgumentException("there should be no duplicate models");
            }

            assignments = Maps.newHashMapWithExpectedSize(nodes.size() * models.size());
            remainingNodeMemory = Maps.newHashMapWithExpectedSize(nodes.size());
            remainingNodeCores = Maps.newHashMapWithExpectedSize(nodes.size());
            remainingModelAllocations = Maps.newHashMapWithExpectedSize(models.size());

            nodes.forEach(n -> {
                remainingNodeMemory.put(n, n.availableMemoryBytes());
                remainingNodeCores.put(n, n.cores());
            });

            for (Model m : models) {
                Map<Node, Integer> nodeAssignments = new HashMap<>();
                for (Node n : nodes) {
                    nodeAssignments.put(n, 0);
                }
                assignments.put(m, nodeAssignments);
                remainingModelAllocations.put(m, m.allocations());
            }
        }

        int getRemainingCores(Node n) {
            return remainingNodeCores.get(n);
        }

        long getRemainingMemory(Node n) {
            return remainingNodeMemory.get(n);
        }

        int getRemainingThreads(Model m) {
            return remainingModelAllocations.get(m) * m.threadsPerAllocation();
        }

        int getRemainingAllocations(Model m) {
            return remainingModelAllocations.get(m);
        }

        boolean canAssign(Model model, Node node, int allocations) {
            return (isAlreadyAssigned(model, node) || model.memoryBytes() <= remainingNodeMemory.get(node))
                && allocations * model.threadsPerAllocation() <= remainingNodeCores.get(node);
        }

        Builder assignModelToNode(Model model, Node node, int allocations) {
            if (allocations <= 0) {
                return this;
            }
            if (isAlreadyAssigned(model, node) == false && model.memoryBytes() > remainingNodeMemory.get(node)) {
                throw new IllegalArgumentException("not enough memory on node [" + node.id() + "] to assign model [" + model.id() + "]");
            }
            if (allocations * model.threadsPerAllocation() > remainingNodeCores.get(node)) {
                throw new IllegalArgumentException(
                    "not enough cores on node ["
                        + node.id()
                        + "] to assign ["
                        + allocations
                        + "] allocations to model ["
                        + model.id()
                        + "]; required threads per allocation ["
                        + model.threadsPerAllocation()
                        + "]"
                );
            }

            long additionalModelMemory = isAlreadyAssigned(model, node) ? 0 : model.memoryBytes;
            assignments.get(model).compute(node, (n, remAllocations) -> remAllocations + allocations);
            remainingNodeMemory.compute(node, (n, remMemory) -> remMemory - additionalModelMemory);
            remainingNodeCores.compute(node, (n, remCores) -> remCores - allocations * model.threadsPerAllocation());
            remainingModelAllocations.compute(model, (m, remModelThreads) -> remModelThreads - allocations);
            return this;
        }

        private boolean isAlreadyAssigned(Model model, Node node) {
            return model.currentAllocationsByNodeId().containsKey(node.id()) || assignments.get(model).get(node) > 0;
        }

        void accountMemory(Model m, Node n) {
            remainingNodeMemory.computeIfPresent(n, (k, v) -> v - m.memoryBytes());
        }

        AssignmentPlan build() {
            Map<Model, Map<Node, Integer>> finalAssignments = new HashMap<>();
            for (Model m : assignments.keySet()) {
                Map<Node, Integer> allocationsPerNode = new HashMap<>();
                for (Map.Entry<Node, Integer> entry : assignments.get(m).entrySet()) {
                    if (entry.getValue() > 0) {
                        allocationsPerNode.put(entry.getKey(), entry.getValue());
                    }
                }
                finalAssignments.put(m, allocationsPerNode);
            }
            return new AssignmentPlan(finalAssignments, remainingNodeMemory, remainingNodeCores, remainingModelAllocations);
        }
    }

    private record Quality(boolean satisfiesPreviousAssignments, double allocationsScore, double memoryScore)
        implements
            Comparable<Quality> {

        private int previousAssignmentScore() {
            return satisfiesPreviousAssignments ? 1 : 0;
        }

        @Override
        public int compareTo(Quality o) {
            return Comparator.comparingInt(Quality::previousAssignmentScore)
                .thenComparingDouble(Quality::allocationsScore)
                .thenComparingDouble(Quality::memoryScore)
                .compare(this, o);
        }
    }
}
