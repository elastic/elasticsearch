/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Model;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * Solving allocation distribution using linear programming requires relaxing the allocation
 * and assignment variables. This means that even though in reality they take discrete integer
 * values, we allow the solver to provide solutions that are real numbers. This is a common
 * technique used in LP. A common way to convert the relaxed solution back in integer values
 * is to apply randomized rounding. This class performs randomized rounding expecting
 * double values for the number of allocations each model gets on each node, as well as a double
 * value in [0, 1] for whether there is an assignment for each model to each node.
 */
class RandomizedAssignmentRounding {

    private static final Logger logger = LogManager.getLogger(RandomizedAssignmentRounding.class);

    private static final double EPS = 1e-6;

    private final Random random;
    private final int rounds;
    private final Collection<Node> nodes;
    private final Collection<Model> models;
    private final AssignmentHolder assignmentHolder;

    RandomizedAssignmentRounding(Random random, int rounds, Collection<Node> nodes, Collection<Model> models) {
        if (rounds <= 0) {
            throw new IllegalArgumentException("rounds must be > 0");
        }
        this.random = Objects.requireNonNull(random);
        this.rounds = rounds;
        this.nodes = Objects.requireNonNull(nodes);
        this.models = Objects.requireNonNull(models);
        this.assignmentHolder = new AssignmentHolder();
    }

    AssignmentPlan computePlan(Map<Tuple<Model, Node>, Double> allocationVars, Map<Tuple<Model, Node>, Double> assignmentVars) {
        AssignmentPlan bestPlan = assignmentHolder.toPlan();

        assignmentHolder.initializeAssignments(allocationVars, assignmentVars);
        assignmentHolder.assignUnderSubscribedNodes();
        List<Tuple<Model, Node>> softAssignmentQueue = assignmentHolder.createSoftAssignmentQueue();

        if (softAssignmentQueue.isEmpty() == false) {
            logger.debug(() -> new ParameterizedMessage("Random assignment rounding across [{}] rounds", rounds));
            for (int i = 0; i < rounds; i++) {
                AssignmentHolder randomizedAssignments = new AssignmentHolder(assignmentHolder);
                randomizedAssignments.doRandomizedRounding(softAssignmentQueue);
                AssignmentPlan randomizedPlan = randomizedAssignments.toPlan();
                if (randomizedPlan.compareTo(bestPlan) > 0) {
                    bestPlan = randomizedPlan;
                }
            }
        } else {
            AssignmentPlan plan = assignmentHolder.toPlan();
            if (plan.compareTo(bestPlan) > 0) {
                bestPlan = plan;
            }
        }

        return bestPlan;
    }

    private class AssignmentHolder {
        private final Map<Tuple<Model, Node>, Double> assignments = new HashMap<>();
        private final Map<Tuple<Model, Node>, Double> allocations = new HashMap<>();
        private final ResourceTracker resourceTracker;

        private AssignmentHolder() {
            resourceTracker = new ResourceTracker(nodes, models);
        }

        private AssignmentHolder(AssignmentHolder holder) {
            assignments.putAll(holder.assignments);
            allocations.putAll(holder.allocations);
            resourceTracker = new ResourceTracker(holder.resourceTracker);
        }

        private void initializeAssignments(Map<Tuple<Model, Node>, Double> allocationVars, Map<Tuple<Model, Node>, Double> assignmentVars) {
            for (Node n : nodes) {
                for (Model m : models) {
                    Tuple<Model, Node> index = Tuple.tuple(m, n);
                    double assignment = assignmentVars.get(index);
                    double allocations = allocationVars.get(index);

                    if (assignment == 1.0 && isInteger(allocations)) {
                        resourceTracker.assign(m, n, (int) Math.rint(allocations));
                    }
                    assignments.put(index, assignment);
                    this.allocations.put(index, allocations);
                }
            }
        }

        private void assignUnderSubscribedNodes() {
            assignUnderSubscribedNodes(nodes);
        }

        private void assignUnderSubscribedNodes(Collection<Node> nodeSelection) {
            // Snap to one any non-zero assignments on nodes where all the soft assigned models fit.
            for (Node n : nodeSelection.stream().sorted(Comparator.comparingDouble(this::decreasingQualityNodeOrder)).toList()) {
                List<Model> assignedModels = new ArrayList<>();
                long totalModelMemory = 0;
                int maxTotalThreads = 0;
                for (Model m : models) {
                    Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                    if (assignments.get(assignment) > 0) {
                        totalModelMemory += m.memoryBytes();
                        maxTotalThreads += (int) Math.ceil(allocations.get(assignment)) * m.threadsPerAllocation();
                        assignedModels.add(m);
                    }
                }
                if (totalModelMemory <= n.availableMemoryBytes() && maxTotalThreads <= n.cores()) {
                    for (Model m : assignedModels) {
                        Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                        if (assignments.get(assignment) > 0 && assignments.get(assignment) < 1) {
                            assignModelToNode(m, n, allocationsToAssign(assignment));
                        }
                    }
                    assignExcessCores(n);
                }
            }
        }

        private int allocationsToAssign(Tuple<Model, Node> assignment) {
            if (isInteger(allocations.get(assignment))) {
                // We round this separately because if we used ceil and the value was just about the
                // integer value we'll use one additional allocation when we shouldn't.
                return (int) Math.rint(allocations.get(assignment));
            }
            return (int) Math.ceil(allocations.get(assignment));
        }

        private void assignModelToNode(Model m, Node n, int allocations) {
            Tuple<Model, Node> assignment = Tuple.tuple(m, n);
            int assignedAllocations = Math.min(allocations, resourceTracker.remainingModelAllocations.get(m));
            assignments.put(assignment, 1.0);
            this.allocations.put(assignment, (double) assignedAllocations);
            resourceTracker.assign(m, n, assignedAllocations);
        }

        private double decreasingQualityNodeOrder(Node n) {
            double quality = 0.0;
            for (Model m : models) {
                Tuple<Model, Node> index = Tuple.tuple(m, n);
                if (allocations.get(index) > 0) {
                    quality += (1 + (m.currentAllocationByNodeId().containsKey(n.id()) ? 1 : 0)) * allocations.get(index) * m
                        .threadsPerAllocation();
                }
            }
            return quality;
        }

        private void assignExcessCores(Node n) {
            if (resourceTracker.remainingNodeCores.get(n) == 0) {
                return;
            }

            if (hasSoftAssignments(n)) {
                return;
            }

            // We know the models on this node are definitely assigned thus we can also
            // assign any extra cores this node has to the models in descending size order.
            for (Model m : models.stream()
                .filter(m -> assignments.get(Tuple.tuple(m, n)) == 1 && resourceTracker.remainingModelAllocations.get(m) > 0)
                .sorted(Comparator.comparingDouble(this::remainingModelOrder))
                .toList()) {
                if (resourceTracker.remainingNodeCores.get(n) <= 0) {
                    break;
                }
                int extraAllocations = Math.min(
                    resourceTracker.remainingNodeCores.get(n) / m.threadsPerAllocation(),
                    resourceTracker.remainingModelAllocations.get(m)
                );
                allocations.compute(Tuple.tuple(m, n), (k, v) -> v + extraAllocations);
                resourceTracker.assign(m, n, extraAllocations);
            }

            zeroSoftAssignmentsOfSatisfiedModels();
        }

        private double remainingModelOrder(Model m) {
            return (m.currentAllocationByNodeId().isEmpty() ? 1 : 2) * -m.memoryBytes();
        }

        private boolean hasSoftAssignments(Node n) {
            return models.stream().anyMatch(m -> isSoftAssignment(m, n));
        }

        private boolean isSoftAssignment(Model m, Node n) {
            Tuple<Model, Node> index = Tuple.tuple(m, n);
            return (assignments.get(index) > 0 && assignments.get(index) < 1) || isInteger(allocations.get(index)) == false;
        }

        private void zeroSoftAssignmentsOfSatisfiedModels() {
            for (Model m : models) {
                if (resourceTracker.remainingModelAllocations.get(m) <= 0) {
                    for (Node n : nodes) {
                        if (isSoftAssignment(m, n)) {
                            unassign(Tuple.tuple(m, n));
                        }
                    }
                }
            }
        }

        private void unassign(Tuple<Model, Node> assignment) {
            assignments.put(assignment, 0.0);
            allocations.put(assignment, 0.0);
        }

        private List<Tuple<Model, Node>> createSoftAssignmentQueue() {
            List<Tuple<Model, Node>> queue = new ArrayList<>();
            models.forEach(m -> nodes.forEach(n -> {
                if (isSoftAssignment(m, n)) {
                    queue.add(Tuple.tuple(m, n));
                }
            }));
            queue.sort(
                Comparator.comparingDouble(this::assignmentDistanceFromZeroOrOneOrder)
                    .thenComparingDouble(this::assignmentMostRemainingThreadsOrder)
            );
            return queue;
        }

        private double assignmentDistanceFromZeroOrOneOrder(Tuple<Model, Node> assignment) {
            return Math.min(assignments.get(assignment), 1 - assignments.get(assignment));
        }

        private double assignmentMostRemainingThreadsOrder(Tuple<Model, Node> assignment) {
            return -allocations.get(assignment) * assignment.v1().threadsPerAllocation();
        }

        private void doRandomizedRounding(List<Tuple<Model, Node>> softAssignmentQueue) {
            for (Tuple<Model, Node> assignment : softAssignmentQueue) {
                // Other operations can snap assignments in the queue thus we check whether the assignment remains soft.
                if (isSoftAssignment(assignment.v1(), assignment.v2()) == false) {
                    continue;
                }
                Model m = assignment.v1();
                Node n = assignment.v2();

                double roundUpProbability = allocations.get(assignment) - Math.floor(allocations.get(assignment));
                int roundedAllocations = random.nextDouble() < roundUpProbability
                    ? (int) Math.ceil(allocations.get(assignment))
                    : (int) Math.floor(allocations.get(assignment));

                if (m.memoryBytes() > resourceTracker.remainingNodeMemory.get(n)
                    || m.threadsPerAllocation() > resourceTracker.remainingNodeCores.get(n)
                    || roundedAllocations == 0
                    || random.nextDouble() > assignments.get(assignment)) {
                    unassign(assignment);
                    assignUnderSubscribedNodes(Set.of(n));
                } else {
                    roundedAllocations = Math.min(roundedAllocations, resourceTracker.remainingNodeCores.get(n) / m.threadsPerAllocation());
                    assignModelToNode(m, n, roundedAllocations);
                    unassignOversizedModels(n);
                    assignExcessCores(n);
                }
            }
        }

        private void unassignOversizedModels(Node n) {
            for (Model m : models) {
                Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                if (assignments.get(assignment) < 1.0 && m.memoryBytes() > resourceTracker.remainingNodeMemory.get(n)) {
                    unassign(assignment);
                }
            }
        }

        private AssignmentPlan toPlan() {
            AssignmentPlan.Builder builder = AssignmentPlan.builder(nodes, models);
            for (Map.Entry<Tuple<Model, Node>, Integer> assignment : tryAssigningRemainingCores().entrySet()) {
                builder.assignModelToNode(assignment.getKey().v1(), assignment.getKey().v2(), assignment.getValue());
            }
            return builder.build();
        }

        private Map<Tuple<Model, Node>, Integer> tryAssigningRemainingCores() {
            // Eagerly assign allocations to models with larger size first on the first node
            // where the model fits.
            //
            // This is a trivial way to improve solution quality since increasing
            // used allocations always improves our quality measure and we may be able to
            // add a job, which doesn't have its quota of allocations, to the allocation
            // random rounding finds.

            Map<Tuple<Model, Node>, Integer> resultAllocations = new HashMap<>();

            ResourceTracker resourceTracker = new ResourceTracker(nodes, models);

            for (Model m : models) {
                for (Node n : nodes) {
                    Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                    int allocations = (int) Math.floor(this.allocations.getOrDefault(assignment, 0.0));
                    resultAllocations.put(assignment, allocations);
                    if (allocations > 0) {
                        resourceTracker.assign(m, n, allocations);
                    }
                }
            }

            for (Model m : models.stream()
                .filter(m -> resourceTracker.remainingModelAllocations.get(m) > 0)
                .sorted(Comparator.comparingDouble(this::remainingModelOrder))
                .toList()) {
                for (Node n : nodes.stream()
                    .filter(
                        n -> resourceTracker.remainingNodeMemory.get(n) >= m.memoryBytes()
                            && resourceTracker.remainingNodeCores.get(n) >= m.threadsPerAllocation()
                            && resultAllocations.get(Tuple.tuple(m, n)) == 0
                    )
                    .sorted(
                        Comparator.comparingDouble(
                            n -> remainingNodeOrder(
                                n,
                                m,
                                resourceTracker.remainingNodeCores.get(n),
                                resourceTracker.remainingNodeMemory.get(n),
                                resourceTracker.remainingModelAllocations.get(m)
                            )
                        )
                    )
                    .toList()) {

                    int assigningAllocations = Math.min(
                        resourceTracker.remainingNodeCores.get(n) / m.threadsPerAllocation(),
                        resourceTracker.remainingModelAllocations.get(m)
                    );
                    resourceTracker.assign(m, n, assigningAllocations);
                    resultAllocations.put(Tuple.tuple(m, n), assigningAllocations);
                    if (resourceTracker.remainingModelAllocations.get(m) == 0) {
                        break;
                    }
                }
            }
            return resultAllocations;
        }

        private double remainingNodeOrder(
            Node n,
            Model m,
            int remainingNodeCores,
            long remainingNodeMemory,
            int remainingModelAllocations
        ) {
            return (m.currentAllocationByNodeId().containsKey(n.id()) ? 0 : 1) + (remainingNodeCores <= remainingModelAllocations * m
                .threadsPerAllocation() ? 0 : 0.5) + (0.01 * distance(
                    remainingNodeCores,
                    remainingModelAllocations * m.threadsPerAllocation()
                )) + (0.01 * remainingNodeMemory);
        }
    }

    @SuppressForbidden(reason = "Math#abs(int) is safe here as we protect against MIN_VALUE")
    private static int distance(int x, int y) {
        int distance = x - y;
        return distance == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(distance);
    }

    private static class ResourceTracker {

        final Set<Tuple<Model, Node>> assignments = new HashSet<>();
        final Map<Node, Long> remainingNodeMemory;
        final Map<Node, Integer> remainingNodeCores;
        final Map<Model, Integer> remainingModelAllocations;

        ResourceTracker(Collection<Node> nodes, Collection<Model> models) {
            remainingNodeMemory = Maps.newHashMapWithExpectedSize(nodes.size());
            remainingNodeCores = Maps.newHashMapWithExpectedSize(nodes.size());
            remainingModelAllocations = Maps.newHashMapWithExpectedSize(models.size());

            nodes.forEach(n -> {
                remainingNodeMemory.put(n, n.availableMemoryBytes());
                remainingNodeCores.put(n, n.cores());
            });

            for (Model m : models) {
                for (Node n : nodes) {
                    if (m.currentAllocationByNodeId().containsKey(n.id())) {
                        assignments.add(Tuple.tuple(m, n));
                    }
                }
                remainingModelAllocations.put(m, m.allocations());
            }
        }

        ResourceTracker(ResourceTracker copy) {
            assignments.addAll(copy.assignments);
            remainingNodeMemory = new HashMap<>(copy.remainingNodeMemory);
            remainingNodeCores = new HashMap<>(copy.remainingNodeCores);
            remainingModelAllocations = new HashMap<>(copy.remainingModelAllocations);
        }

        void assign(Model m, Node n, int allocations) {
            if (assignments.contains(Tuple.tuple(m, n)) == false) {
                assignments.add(Tuple.tuple(m, n));
                remainingNodeMemory.compute(n, (k, v) -> v - m.memoryBytes());
            }
            remainingNodeCores.compute(n, (k, v) -> v - allocations * m.threadsPerAllocation());
            remainingModelAllocations.compute(m, (k, v) -> v - allocations);
        }
    }

    private static boolean isInteger(double value) {
        // it is possible that the solver results in values that are really close to an int, we should treat those as ints
        return Double.isFinite(value) && Math.abs(value - Math.rint(value)) < EPS;
    }
}
