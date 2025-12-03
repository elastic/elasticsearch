/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * SORT cannot be executed without a LIMIT, as ES|QL doesn't support unbounded sort (yet).
 * <p>
 * The planner tries to push down LIMIT close to a SORT and transform the two into a TopN (which is executable).
 * In some cases this is not possible though, eg.
 * <p>
 * from test | sort x | lookup join lookup on x | sort y
 * <p>
 * from test | sort x | mv_expand x | sort y
 * <p>
 * "sort y" will become a TopN due to the addition of the default LIMIT, but "sort x" will remain unbounded,
 * so the query could not be executed.
 * <p>
 * In some cases though, a command following SORT can make it redundant, either because the command is a SORT itself,
 * or because the command will reduce/"scramble" the data (like STATS).
 * <p>
 * This rule finds and prunes redundant SORTs, attempting to thus keep the plan executable.
 */
public class PruneRedundantOrderBy extends OptimizerRules.OptimizerRule<LogicalPlan> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        // Note that these aren't the only node types that can make a SORT useless: some generative commands like MV_EXPAND and LOOKUP JOIN
        // can have a similar effect. I.e., the data produced by these nodes no longer respects the original SORT. But while we don't offer
        // explicit sorting guarantees after such nodes, we do attempt to keep the apparent isolation and sequential execution of the
        // commands, in general.
        if (plan instanceof OrderBy || plan instanceof TopN || plan instanceof Aggregate) {
            Set<OrderBy> redundant = findRedundantSort(((UnaryPlan) plan).child());
            plan = redundant.isEmpty() ? plan : plan.transformDown(OrderBy.class, p -> redundant.contains(p) ? p.child() : p);
        }
        return plan;
    }

    /**
     * breadth-first recursion to find redundant SORTs in the children tree.
     * Returns an identity set (we need to compare and prune the exact instances)
     */
    private Set<OrderBy> findRedundantSort(LogicalPlan plan) {
        Set<OrderBy> result = Collections.newSetFromMap(new IdentityHashMap<>());

        Deque<LogicalPlan> toCheck = new ArrayDeque<>();
        toCheck.push(plan);

        while (toCheck.isEmpty() == false) {
            LogicalPlan p = toCheck.pop();
            if (p instanceof OrderBy ob) {
                result.add(ob);
                toCheck.add(ob.child());
            } else if (isRedundantSortAgnostic(p)) {
                toCheck.addAll(p.children());
            }
        }

        return result;
    }

    /**
     * Returns {@code true} if a first SORT in a pattern like {@code ...| SORT a | COMMAND | SORT b |...} can be dropped. I.e. the plan
     * can be simplifed to the equivalent of {@code ...| COMMAND | SORT b |...}.
     * <br>
     * That means that the execution of COMMAND doesn't depend on the order of the input data.
     * <br>
     * It does not mean that the order of the commands can be swapped, though.
     */
    protected static boolean isRedundantSortAgnostic(LogicalPlan plan) {
        return switch (plan) {
            // We cannot drop a SORT that precedes a LIMIT, since that would change the results this LIMIT accumulates.
            case Limit l -> false;
            default -> true;
        };
    }
}
