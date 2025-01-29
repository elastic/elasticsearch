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
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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

    /**
     *
     * @param deploymentId
     * @param memoryBytes
     * @param allocations
     * @param threadsPerAllocation
     * @param currentAllocationsByNodeId
     * @param maxAssignedAllocations this value is used by the ZoneAwareAssignmentPlan and AssignmentPlanner to keep track of the
     *                               maximum number of allocations which have been assigned. It is mainly for assigning over AZs.
     * @param adaptiveAllocationsSettings
     * @param priority
     * @param perDeploymentMemoryBytes
     * @param perAllocationMemoryBytes
     */
    public record Deployment(
        String deploymentId,
        long memoryBytes,
        int allocations,
        int threadsPerAllocation,
        Map<String, Integer> currentAllocationsByNodeId,
        int maxAssignedAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        Priority priority,
        long perDeploymentMemoryBytes,
        long perAllocationMemoryBytes
    ) {
        public Deployment(
            String deploymentId,
            long modelBytes,
            int allocations,
            int threadsPerAllocation,
            Map<String, Integer> currentAllocationsByNodeId,
            int maxAssignedAllocations,
            AdaptiveAllocationsSettings adaptiveAllocationsSettings,
            long perDeploymentMemoryBytes,
            long perAllocationMemoryBytes
        ) {
            this(
                deploymentId,
                modelBytes,
                allocations,
                threadsPerAllocation,
                currentAllocationsByNodeId,
                maxAssignedAllocations,
                adaptiveAllocationsSettings,
                Priority.NORMAL,
                perDeploymentMemoryBytes,
                perAllocationMemoryBytes
            );
        }

        public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
            return adaptiveAllocationsSettings;
        }

        int getCurrentAssignedAllocations() {
            return currentAllocationsByNodeId.values().stream().mapToInt(Integer::intValue).sum();
        }

        boolean hasEverBeenAllocated() {
            return maxAssignedAllocations > 0;
        }

        public long estimateMemoryUsageBytes(int allocations) {
            return StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
                deploymentId,
                memoryBytes,
                perDeploymentMemoryBytes,
                perAllocationMemoryBytes,
                allocations
            );
        }

        long estimateAdditionalMemoryUsageBytes(int allocationsOld, int allocationsNew) {
            return StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
                deploymentId,
                memoryBytes,
                perDeploymentMemoryBytes,
                perAllocationMemoryBytes,
                allocationsNew
            ) - StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
                deploymentId,
                memoryBytes,
                perDeploymentMemoryBytes,
                perAllocationMemoryBytes,
                allocationsOld
            );

        }

        long minimumMemoryRequiredBytes() {
            return StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
                deploymentId,
                memoryBytes,
                perDeploymentMemoryBytes,
                perAllocationMemoryBytes,
                1
            );
        }

        int findOptimalAllocations(int maxAllocations, long availableMemoryBytes) {
            if (perDeploymentMemoryBytes > 0 && perAllocationMemoryBytes > 0) {
                return (int) Math.max(
                    Math.min(maxAllocations, Math.floorDiv(availableMemoryBytes - estimateMemoryUsageBytes(0), perAllocationMemoryBytes)),
                    0
                );
            }
            return maxAllocations;
        }

        int findExcessAllocations(int maxAllocations, long availableMemoryBytes) {
            if (perDeploymentMemoryBytes > 0 && perAllocationMemoryBytes > 0) {
                return (int) Math.min(maxAllocations, Math.floorDiv(availableMemoryBytes, perAllocationMemoryBytes));
            }
            return maxAllocations;
        }

        @Override
        public String toString() {
            return deploymentId
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
                + ") (memory_usage = "
                + ByteSizeValue.ofBytes(estimateMemoryUsageBytes(allocations))
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
    private final Map<Deployment, Map<Node, Integer>> assignments;

    private final Map<String, Long> remainingNodeMemory;
    private final Map<String, Integer> remainingNodeCores;
    private final Map<Deployment, Integer> remainingModelAllocations;

    private AssignmentPlan(
        Map<Deployment, Map<Node, Integer>> assignments,
        Map<Node, Long> remainingNodeMemory,
        Map<Node, Integer> remainingNodeCores,
        Map<Deployment, Integer> remainingModelAllocations
    ) {
        this.assignments = Objects.requireNonNull(assignments);
        this.remainingNodeMemory = remainingNodeMemory.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey().id(), e -> e.getValue()));
        this.remainingNodeCores = remainingNodeCores.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().id(), e -> e.getValue()));
        this.remainingModelAllocations = Objects.requireNonNull(remainingModelAllocations);
    }

    public Set<Deployment> deployments() {
        return assignments.keySet();
    }

    /**
     * The assignments of that model to each node. Each assignment
     * is a {@link Map<Node, Integer>} where the value is the number
     * of allocations for each node.
     * @param deployment the model for which assignments are returned
     * @return the model assignments per node. The Optional will be empty if the model has no assignments.
     */
    public Optional<Map<Node, Integer>> assignments(Deployment deployment) {
        Map<Node, Integer> modelAssignments = assignments.get(deployment);
        return (modelAssignments == null || modelAssignments.isEmpty()) ? Optional.empty() : Optional.of(modelAssignments);
    }

    @Override
    public int compareTo(AssignmentPlan o) {
        return Comparator.comparing(AssignmentPlan::computeQuality).compare(this, o);
    }

    public boolean satisfiesCurrentAssignments() {
        return deployments().stream().allMatch(this::isSatisfyingCurrentAssignmentsForModel);
    }

    private boolean isSatisfyingCurrentAssignmentsForModel(Deployment m) {
        if (m.currentAllocationsByNodeId().isEmpty()) {
            return true;
        }
        Map<Node, Integer> nodeAssignments = assignments.get(m);
        int currentAllocations = nodeAssignments.values().stream().mapToInt(Integer::intValue).sum();
        return currentAllocations >= m.getCurrentAssignedAllocations();
    }

    public boolean satisfiesAllocations(Deployment m) {
        return remainingModelAllocations.getOrDefault(m, 0) == 0;
    }

    public boolean satisfiesAllModels() {
        return deployments().stream().allMatch(this::satisfiesAllocations);
    }

    public boolean arePreviouslyAssignedModelsAssigned() {
        return deployments().stream()
            .filter(Deployment::hasEverBeenAllocated)
            .map(this::totalAllocations)
            .allMatch(totalAllocations -> totalAllocations > 0);
    }

    public long countPreviouslyAssignedModelsThatAreStillAssigned() {
        return deployments().stream()
            .filter(Deployment::hasEverBeenAllocated)
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

    public int totalAllocations(Deployment m) {
        if (assignments.containsKey(m) == false) {
            return 0;
        }
        return assignments.get(m).values().stream().mapToInt(Integer::intValue).sum();
    }

    private Quality computeQuality() {
        boolean isSatisfyingPreviousAssignments = true;
        double weighedAllocationsScore = 0;
        double memoryScore = 0;

        for (Map.Entry<Deployment, Map<Node, Integer>> entry : assignments.entrySet()) {
            Deployment m = entry.getKey();
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

        Map<Node, List<Tuple<Deployment, Integer>>> nodeToModel = new HashMap<>();
        for (Deployment m : assignments.keySet()) {
            for (Node n : assignments.get(m).keySet()) {
                List<Tuple<Deployment, Integer>> allocationsPerModel = nodeToModel.containsKey(n) ? nodeToModel.get(n) : new ArrayList<>();
                allocationsPerModel.add(Tuple.tuple(m, assignments.get(m).get(n)));
                nodeToModel.put(n, allocationsPerModel);
            }
        }

        StringBuilder msg = new StringBuilder();
        List<Node> nodes = nodeToModel.keySet().stream().sorted(Comparator.comparing(Node::id)).toList();
        for (int i = 0; i < nodes.size(); i++) {
            Node n = nodes.get(i);
            msg.append(n);
            msg.append(" ->");
            for (Tuple<Deployment, Integer> modelAllocations : nodeToModel.get(n)
                .stream()
                .sorted(Comparator.comparing(x -> x.v1().deploymentId()))
                .toList()) {
                if (modelAllocations.v2() > 0) {
                    msg.append(" ");
                    msg.append(modelAllocations.v1().deploymentId());
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

    public static Builder builder(Collection<Node> nodes, Collection<Deployment> deployments) {
        return new Builder(nodes, deployments);
    }

    public static class Builder {

        private final Map<Deployment, Map<Node, Integer>> assignments;
        private final Map<Node, Long> remainingNodeMemory;
        private final Map<Node, Integer> remainingNodeCores;
        private final Map<Deployment, Integer> remainingModelAllocations;

        private Builder(Collection<Node> nodes, Collection<Deployment> deployments) {
            if (new HashSet<>(nodes).size() != nodes.size()) {
                throw new IllegalArgumentException("there should be no duplicate nodes");
            }
            if (new HashSet<>(deployments).size() != deployments.size()) {
                throw new IllegalArgumentException("there should be no duplicate models");
            }

            assignments = Maps.newHashMapWithExpectedSize(nodes.size() * deployments.size());
            remainingNodeMemory = Maps.newHashMapWithExpectedSize(nodes.size());
            remainingNodeCores = Maps.newHashMapWithExpectedSize(nodes.size());
            remainingModelAllocations = Maps.newHashMapWithExpectedSize(deployments.size());

            nodes.forEach(n -> {
                remainingNodeMemory.put(n, n.availableMemoryBytes());
                remainingNodeCores.put(n, n.cores());
            });

            for (Deployment m : deployments) {
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

        int getRemainingThreads(Deployment m) {
            return remainingModelAllocations.get(m) * m.threadsPerAllocation();
        }

        int getRemainingAllocations(Deployment m) {
            return remainingModelAllocations.get(m);
        }

        boolean canAssign(Deployment deployment, Node node, int allocations) {
            long requiredMemory = getDeploymentMemoryRequirement(deployment, node, allocations);
            return canAssign(deployment, node, allocations, requiredMemory);
        }

        boolean canAssign(Deployment deployment, Node node, int allocations, long requiredMemory) {
            return (requiredMemory <= remainingNodeMemory.get(node))
                && (deployment.priority == Priority.LOW || allocations * deployment.threadsPerAllocation() <= remainingNodeCores.get(node));
        }

        public long getDeploymentMemoryRequirement(Deployment deployment, Node node, int newAllocations) {
            int assignedAllocations = getAssignedAllocations(deployment, node);

            if (assignedAllocations > 0) {
                return deployment.estimateAdditionalMemoryUsageBytes(assignedAllocations, assignedAllocations + newAllocations);
            }
            return deployment.estimateMemoryUsageBytes(newAllocations);
        }

        public Builder assignModelToNode(Deployment deployment, Node node, int allocations) {
            return assignModelToNode(deployment, node, allocations, getDeploymentMemoryRequirement(deployment, node, allocations));
        }

        public Builder assignModelToNode(Deployment deployment, Node node, int allocations, long requiredMemory) {
            if (allocations <= 0) {
                return this;
            }
            if (requiredMemory > remainingNodeMemory.get(node)) {
                throw new IllegalArgumentException(
                    "not enough memory on node ["
                        + node.id()
                        + "] to assign ["
                        + allocations
                        + "] allocations to deployment ["
                        + deployment.deploymentId()
                        + "]"
                );
            }
            if (deployment.priority == Priority.NORMAL && allocations * deployment.threadsPerAllocation() > remainingNodeCores.get(node)) {
                throw new IllegalArgumentException(
                    "not enough cores on node ["
                        + node.id()
                        + "] to assign ["
                        + allocations
                        + "] allocations to deployment ["
                        + deployment.deploymentId()
                        + "]; required threads per allocation ["
                        + deployment.threadsPerAllocation()
                        + "]"
                );
            }

            assignments.get(deployment).compute(node, (n, remAllocations) -> remAllocations + allocations);
            accountMemory(deployment, node, requiredMemory);

            if (deployment.priority == Priority.NORMAL) {
                remainingNodeCores.compute(node, (n, remCores) -> remCores - allocations * deployment.threadsPerAllocation());
            }
            remainingModelAllocations.compute(deployment, (m, remModelThreads) -> remModelThreads - allocations);
            return this;
        }

        private int getAssignedAllocations(Deployment deployment, Node node) {
            int currentAllocations = getCurrentAllocations(deployment, node);
            int assignmentAllocations = assignments.get(deployment).get(node);
            return currentAllocations + assignmentAllocations;
        }

        private static int getCurrentAllocations(Deployment m, Node n) {
            return m.currentAllocationsByNodeId.containsKey(n.id()) ? m.currentAllocationsByNodeId.get(n.id()) : 0;
        }

        public void accountMemory(Deployment m, Node n) {
            if (m.currentAllocationsByNodeId().containsKey(n.id())) {
                int allocations = m.currentAllocationsByNodeId().get(n.id());
                long requiredMemory = m.estimateMemoryUsageBytes(allocations);
                accountMemory(m, n, requiredMemory);
            }
        }

        private void accountMemory(Deployment m, Node n, long requiredMemory) {
            remainingNodeMemory.computeIfPresent(n, (k, v) -> v - requiredMemory);
            if (remainingNodeMemory.containsKey(n) && remainingNodeMemory.get(n) < 0) {
                throw new IllegalArgumentException("not enough memory on node [" + n.id() + "] to assign model [" + m.deploymentId() + "]");
            }
        }

        public AssignmentPlan build() {
            Map<Deployment, Map<Node, Integer>> finalAssignments = new HashMap<>();
            for (Deployment m : assignments.keySet()) {
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
