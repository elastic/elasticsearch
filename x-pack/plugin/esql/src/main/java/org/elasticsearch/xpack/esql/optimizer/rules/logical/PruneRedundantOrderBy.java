/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.SortAgnostic;
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
 * The planner tries to push down LIMIT and transform all the unbounded sorts into a TopN.
 * In some cases it's not possible though, eg.
 * <p>
 * from test | sort x | lookup join lookup on x | sort y
 * <p>
 * from test | sort x | mv_expand x | sort y
 * <p>
 * "sort y" will become a TopN due to the addition of the default Limit, but "sort x" will remain unbounded,
 * so the query could not be executed.
 * <p>
 * In most cases though, following commands can make the previous SORTs redundant,
 * because it will re-sort previously sorted results (eg. if there is another SORT)
 * or because the order will be scrambled by another command (eg. a STATS)
 * <p>
 * This rule finds and prunes redundant SORTs, attempting to make the plan executable.
 */
public class PruneRedundantOrderBy extends OptimizerRules.OptimizerRule<LogicalPlan> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        if (plan instanceof OrderBy || plan instanceof TopN || plan instanceof Aggregate) {
            Set<OrderBy> redundant = findRedundantSort(((UnaryPlan) plan).child());
            if (redundant.isEmpty()) {
                return plan;
            }
            return plan.transformDown(p -> redundant.contains(p) ? ((UnaryPlan) p).child() : p);
        } else {
            return plan;
        }
    }

    /**
     * breadth-first recursion to find redundant SORTs in the children tree.
     * Returns an identity set (we need to compare and prune the exact instances)
     */
    private Set<OrderBy> findRedundantSort(LogicalPlan plan) {
        Set<OrderBy> result = Collections.newSetFromMap(new IdentityHashMap<>());

        Deque<LogicalPlan> toCheck = new ArrayDeque<>();
        toCheck.push(plan);

        while (true) {
            if (toCheck.isEmpty()) {
                return result;
            }
            LogicalPlan p = toCheck.pop();
            if (p instanceof OrderBy ob) {
                result.add(ob);
                toCheck.push(ob.child());
            } else if (p instanceof SortAgnostic) {
                for (LogicalPlan child : p.children()) {
                    toCheck.push(child);
                }
            }
        }
    }
}
