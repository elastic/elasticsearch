/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Model;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AssignmentPlan.Node;
import org.ojalgo.optimisation.ExpressionsBasedModel;
import org.ojalgo.optimisation.Optimisation;
import org.ojalgo.optimisation.Variable;
import org.ojalgo.structure.Access1D;
import org.ojalgo.type.CalendarDateDuration;
import org.ojalgo.type.CalendarDateUnit;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An allocation plan solver based on linear programming.
 * This solver uses the linear programming solver from the ojalgo library.
 */
class LinearProgrammingPlanSolver {

    private static final Logger logger = LogManager.getLogger(LinearProgrammingPlanSolver.class);

    private static final long RANDOMIZATION_SEED = 738921734L;
    private static final double L1 = 0.9; // Must be a number < 1. 0.9 gave good results during testing.
    private static final double INITIAL_W = 0.2;
    private static final int RANDOMIZED_ROUNDING_ROUNDS = 20; // more rounds do not seem to improve things.

    /**
     * Ojalgo solver may throw a OOM exception if the problem is too large.
     * When the solver is dense, a 2D double array is created with size of complexity {@link #memoryComplexity()}.
     * We empirically determine a threshold for complexity after which we switch the solver to sparse mode
     * which consumes significantly less memory at the cost of speed.
     */
    private static final int MEMORY_COMPLEXITY_SPARSE_THRESHOLD = 4_000_000;

    /**
     * Ojalgo solver may throw a OOM exception if the problem is too large.
     * When the solver is dense, a 2D double array is created with size of complexity {@link #memoryComplexity()}.
     * Using the same function for when the solver is sparse we empirically determine a threshold after which
     * we do not invoke the solver at all and fall back to our bin packing solution.
     */
    private static final int MEMORY_COMPLEXITY_LIMIT = 10_000_000;

    private final Random random = new Random(RANDOMIZATION_SEED);

    private final List<Node> nodes;
    private final List<Model> models;
    private final Map<Node, Double> normalizedMemoryPerNode;
    private final Map<Node, Integer> coresPerNode;
    private final Map<Model, Double> normalizedMemoryPerModel;

    private final int maxNodeCores;
    private final long maxModelMemoryBytes;

    LinearProgrammingPlanSolver(List<Node> nodes, List<Model> models) {
        this.nodes = nodes;
        maxNodeCores = this.nodes.stream().map(Node::cores).max(Integer::compareTo).orElse(0);

        long maxNodeMemory = nodes.stream().map(Node::availableMemoryBytes).max(Long::compareTo).orElse(0L);
        this.models = models.stream()
            // Filter out models that are not already assigned and do not fit on any node
            .filter(m -> m.currentAllocationsByNodeId().isEmpty() == false || m.memoryBytes() <= maxNodeMemory)
            // Also filter out models whose threads per allocation are more than the max node cores
            .filter(m -> m.threadsPerAllocation() <= maxNodeCores)
            .toList();

        maxModelMemoryBytes = this.models.stream().map(Model::memoryBytes).max(Long::compareTo).orElse(1L);
        normalizedMemoryPerNode = this.nodes.stream()
            .collect(Collectors.toMap(Function.identity(), n -> n.availableMemoryBytes() / (double) maxModelMemoryBytes));
        coresPerNode = this.nodes.stream().collect(Collectors.toMap(Function.identity(), Node::cores));
        normalizedMemoryPerModel = this.models.stream()
            .collect(Collectors.toMap(Function.identity(), m -> m.memoryBytes() / (double) maxModelMemoryBytes));
    }

    AssignmentPlan solvePlan(boolean useBinPackingOnly) {
        if (models.isEmpty() || maxNodeCores == 0) {
            return AssignmentPlan.builder(nodes, models).build();
        }

        Tuple<Map<Tuple<Model, Node>, Double>, AssignmentPlan> weightsAndBinPackingPlan = calculateWeightsAndBinPackingPlan();

        if (useBinPackingOnly) {
            return weightsAndBinPackingPlan.v2();
        }

        Map<Tuple<Model, Node>, Double> allocationValues = new HashMap<>();
        Map<Tuple<Model, Node>, Double> assignmentValues = new HashMap<>();
        if (solveLinearProgram(weightsAndBinPackingPlan.v1(), allocationValues, assignmentValues) == false) {
            return weightsAndBinPackingPlan.v2();
        }

        RandomizedAssignmentRounding randomizedRounding = new RandomizedAssignmentRounding(
            random,
            RANDOMIZED_ROUNDING_ROUNDS,
            nodes,
            models
        );
        AssignmentPlan assignmentPlan = randomizedRounding.computePlan(allocationValues, assignmentValues);
        AssignmentPlan binPackingPlan = weightsAndBinPackingPlan.v2();
        if (binPackingPlan.compareTo(assignmentPlan) > 0) {
            assignmentPlan = binPackingPlan;
            logger.debug(() -> "Best plan is from bin packing");
        } else {
            logger.debug(() -> "Best plan is from LP solver");
        }

        return assignmentPlan;
    }

    private double weightForAllocationVar(Model m, Node n, Map<Tuple<Model, Node>, Double> weights) {
        return (1 + weights.get(Tuple.tuple(m, n)) - (m.memoryBytes() > n.availableMemoryBytes() ? 10 : 0)) - L1 * normalizedMemoryPerModel
            .get(m) / maxNodeCores;
    }

    private Tuple<Map<Tuple<Model, Node>, Double>, AssignmentPlan> calculateWeightsAndBinPackingPlan() {
        logger.debug(() -> "Calculating weights and bin packing plan");

        double w = INITIAL_W;
        double dw = w / nodes.size() / models.size();

        Map<Tuple<Model, Node>, Double> weights = new HashMap<>();
        AssignmentPlan.Builder assignmentPlan = AssignmentPlan.builder(nodes, models);

        for (Model m : models.stream().sorted(Comparator.comparingDouble(this::descendingSizeAnyFitsModelOrder)).toList()) {
            double lastW;
            do {
                lastW = w;
                List<Node> orderedNodes = nodes.stream()
                    .sorted(Comparator.comparingDouble(n -> descendingSizeAnyFitsNodeOrder(n, m, assignmentPlan)))
                    .toList();
                for (Node n : orderedNodes) {
                    int allocations = Math.min(
                        assignmentPlan.getRemainingCores(n) / m.threadsPerAllocation(),
                        assignmentPlan.getRemainingAllocations(m)
                    );
                    if (allocations > 0 && assignmentPlan.canAssign(m, n, allocations)) {
                        assignmentPlan.assignModelToNode(m, n, allocations);
                        weights.put(Tuple.tuple(m, n), w);
                        w -= dw;
                        break;
                    }
                }
            } while (lastW != w && assignmentPlan.getRemainingAllocations(m) > 0);
        }

        final double finalW = w;
        for (Model m : models) {
            for (Node n : nodes) {
                weights.computeIfAbsent(Tuple.tuple(m, n), key -> random.nextDouble(minWeight(m, n, finalW), maxWeight(m, n, finalW)));
            }
        }

        logger.trace(() -> "Weights = " + weights);
        AssignmentPlan binPackingPlan = assignmentPlan.build();
        logger.debug(() -> "Bin packing plan =\n" + binPackingPlan.prettyPrint());

        return Tuple.tuple(weights, binPackingPlan);
    }

    private double descendingSizeAnyFitsModelOrder(Model m) {
        return (m.currentAllocationsByNodeId().isEmpty() ? 1 : 2) * -normalizedMemoryPerModel.get(m) * m.threadsPerAllocation();
    }

    private double descendingSizeAnyFitsNodeOrder(Node n, Model m, AssignmentPlan.Builder assignmentPlan) {
        return (m.currentAllocationsByNodeId().containsKey(n.id()) ? 0 : 1) + (assignmentPlan.getRemainingCores(n) >= assignmentPlan
            .getRemainingThreads(m) ? 0 : 1) + (0.01 * distance(assignmentPlan.getRemainingCores(n), assignmentPlan.getRemainingThreads(m)))
            - (0.01 * normalizedMemoryPerNode.get(n));
    }

    @SuppressForbidden(reason = "Math#abs(int) is safe here as we protect against MIN_VALUE")
    private static int distance(int x, int y) {
        int distance = x - y;
        return distance == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(distance);
    }

    private double minWeight(Model m, Node n, double w) {
        return m.currentAllocationsByNodeId().containsKey(n.id()) ? w / 2 : 0;
    }

    private double maxWeight(Model m, Node n, double w) {
        return m.currentAllocationsByNodeId().containsKey(n.id()) ? w : w / 2;
    }

    private boolean solveLinearProgram(
        Map<Tuple<Model, Node>, Double> weights,
        Map<Tuple<Model, Node>, Double> allocationValues,
        Map<Tuple<Model, Node>, Double> assignmentValues
    ) {
        if (memoryComplexity() > MEMORY_COMPLEXITY_LIMIT) {
            logger.debug(() -> "Problem size to big to solve with linear programming; falling back to bin packing solution");
            return false;
        }

        // We formulate the allocation problem as a linear program.
        //
        // Let us define a_i_j as the number of allocations model i is assigned to node j.
        //
        // We then have the following constraints:
        // 1. We should not have more allocations per model than required
        // 2. For previously assigned models, we should have at least as many allocations as we did before
        // 3. We should not use more cores on each node than its available cores
        // 4. We should not use more memory on each node than its available memory
        //
        // In order to capture the memory constraint we introduce a second variable, y_i_j (assignment).
        // These encode whether a model i is assigned (at least 1 allocation) to a node j or not: 1 indicates it
        // is assigned, 0 that it is not. Then we can express the memory constraint as: sum(m_i * y_i_j) <= M_j
        // where m_i is the memory used by model i, M_j is the available memory of node j.
        // We also know that if a model is not assigned to a node then it cannot have any allocations on that node.
        // Since the maximum number of cores a model assignment can use is N_j, we can capture this as:
        // a_i_j * t_i <= N_j * y_i_j, where t_i represents threads per allocation for model i.
        //
        // Now let us focus on the objective function.
        // First we would like to maximize the sum of a_i_j for all possible assignments.
        // Furthermore, we assign a weight from bin packing to break ties in assigning models to nodes
        // since this yields higher quality solutions.
        // Finally, we want to penalize solutions that use more memory.
        // We express the objective function as: maximize sum(a_i_j * w_i_j) - L1' * sum(m_i * y_i_j / t_i).
        // Note we divide the memory penalty term by t_i to maximize allocations, not threads.
        //
        // Both our a_i_j and y_i_j variables are integer variables. This means the problem is a mixed integer linear program (MILP).
        // However, we don't want to solve a MILP because it is expensive and often leads to infeasible solutions. In addition,
        // ojalgo's MILP functionality makes calls to forbidden APIs that we cannot grant permissions for.
        // Thus, we relax our variables to be real numbers. After we obtain a solution we then apply randomized rounding in
        // order to snap the variable values to an integer.
        //
        // Adding the y variables to the model results in memory complexity explosion. However, we can substitute y.
        // In particular, a_i_j * t_i <= N_j * y_i_j is an equality because the objective always increase if we decrease y_i_j.
        // Thus, we can express: y_i_j = (a_i_j * t_i) / N_j
        //
        // The memory constraint is then rewritten as: sum(m_i * a_i_j * t_i / N_j) <= M_j
        // The objective function becomes: maximize sum(a_i_j * w_i_j) - L1' * sum(m_i * a_i_j * t_i / N_j).
        // We want to choose L1' in such a way that we will always assign models to a node if there is capacity.
        // Since w_i_j can be equal to 1 and m_i <= 1, for each node it has to be that L1' * t_i / N_j < 1.
        // The max value for t_i is C, where C is the max number of cores across nodes. Thus, we can write L1' * C / N_j < 1.
        // We define L1 so that L1 = L1' * C / N_j < 1. Solving for L1', we get L1' = L1 * N_j / C.
        // Replacing L1' with L1 in the objective function we get: maximize sum(a_i_j * w_i_j) - L1 * sum(m_i * a_i_j / C).

        Optimisation.Options options = new Optimisation.Options().abort(new CalendarDateDuration(10, CalendarDateUnit.SECOND));
        if (memoryComplexity() > MEMORY_COMPLEXITY_SPARSE_THRESHOLD) {
            logger.debug(() -> "Problem size is large enough to switch to sparse solver");
            options.sparse = true;
        }
        ExpressionsBasedModel model = new ExpressionsBasedModel(options);

        Map<Tuple<Model, Node>, Variable> allocationVars = new HashMap<>();

        for (Model m : models) {
            for (Node n : nodes) {
                Variable allocationVar = model.addVariable("allocations_of_model_" + m.id() + "_on_node_" + n.id())
                    .integer(false) // We relax the program to non-integer as the integer solver is much slower and can often lead to
                                    // infeasible solutions
                    .lower(0.0) // It is important not to set an upper bound here as it impacts memory negatively
                    .weight(weightForAllocationVar(m, n, weights));
                allocationVars.put(Tuple.tuple(m, n), allocationVar);
            }
        }

        for (Model m : models) {
            // Each model should not get more allocations than is required.
            // Also, if the model has previous assignments, it should get at least as many allocations as it did before.
            model.addExpression("allocations_of_model_" + m.id() + "_not_more_than_required")
                .lower(m.getCurrentAssignedAllocations())
                .upper(m.allocations())
                .setLinearFactorsSimple(varsForModel(m, allocationVars));
        }

        double[] threadsPerAllocationPerModel = models.stream().mapToDouble(m -> m.threadsPerAllocation()).toArray();
        for (Node n : nodes) {
            // Allocations should not use more cores than the node has.
            // We multiply the allocation variables with the threads per allocation for each model to find the total number of cores used.
            model.addExpression("threads_on_node_" + n.id() + "_not_more_than_cores")
                .upper(coresPerNode.get(n))
                .setLinearFactors(varsForNode(n, allocationVars), Access1D.wrap(threadsPerAllocationPerModel));
        }

        for (Node n : nodes) {
            // No more memory should be used than the node has available.
            // This is the m_i * a_i_j * t_i / N_j constraint.
            List<Variable> allocations = new ArrayList<>();
            List<Double> modelMemories = new ArrayList<>();
            models.stream().filter(m -> m.currentAllocationsByNodeId().containsKey(n.id()) == false).forEach(m -> {
                allocations.add(allocationVars.get(Tuple.tuple(m, n)));
                modelMemories.add(normalizedMemoryPerModel.get(m) * m.threadsPerAllocation() / (double) coresPerNode.get(n));
            });
            model.addExpression("used_memory_on_node_" + n.id() + "_not_more_than_available")
                .upper(normalizedMemoryPerNode.get(n))
                .setLinearFactors(allocations, Access1D.wrap(modelMemories));
        }

        Optimisation.Result result = privilegedModelMaximise(model);

        if (result.getState().isFeasible() == false) {
            logger.debug("Linear programming solution state [{}] is not feasible", result.getState());
            return false;
        }

        for (Model m : models) {
            for (Node n : nodes) {
                Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                allocationValues.put(assignment, allocationVars.get(assignment).getValue().doubleValue());
                assignmentValues.put(
                    assignment,
                    allocationVars.get(assignment).getValue().doubleValue() * m.threadsPerAllocation() / (double) coresPerNode.get(n)
                );

            }
        }
        logger.debug(() -> "LP solver result =\n" + prettyPrintSolverResult(assignmentValues, allocationValues));
        return true;
    }

    @SuppressWarnings("removal")
    private static Optimisation.Result privilegedModelMaximise(ExpressionsBasedModel model) {
        return AccessController.doPrivileged((PrivilegedAction<Optimisation.Result>) () -> model.maximise());
    }

    private int memoryComplexity() {
        // Looking at the internals of ojalgo, to solve the problem a 2D double array is created with
        // dimensions of approximately (n + m) * n * m, where n is the number of nodes and m the number of models.
        return (nodes.size() + models.size()) * nodes.size() * models.size();
    }

    private List<Variable> varsForModel(Model m, Map<Tuple<Model, Node>, Variable> vars) {
        return nodes.stream().map(n -> vars.get(Tuple.tuple(m, n))).toList();
    }

    private List<Variable> varsForNode(Node n, Map<Tuple<Model, Node>, Variable> vars) {
        return models.stream().map(m -> vars.get(Tuple.tuple(m, n))).toList();
    }

    private String prettyPrintSolverResult(Map<Tuple<Model, Node>, Double> assignmentValues, Map<Tuple<Model, Node>, Double> threadValues) {
        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < nodes.size(); i++) {
            Node n = nodes.get(i);
            msg.append(n + " ->");
            for (Model m : models) {
                if (threadValues.get(Tuple.tuple(m, n)) > 0) {
                    msg.append(" ");
                    msg.append(m.id());
                    msg.append(" (mem = ");
                    msg.append(ByteSizeValue.ofBytes(m.memoryBytes()));
                    msg.append(") (allocations = ");
                    msg.append(threadValues.get(Tuple.tuple(m, n)));
                    msg.append("/");
                    msg.append(m.allocations());
                    msg.append(") (y = ");
                    msg.append(assignmentValues.get(Tuple.tuple(m, n)));
                    msg.append(")");
                }
            }
            if (i < nodes.size() - 1) {
                msg.append('\n');
            }
        }
        return msg.toString();
    }
}
