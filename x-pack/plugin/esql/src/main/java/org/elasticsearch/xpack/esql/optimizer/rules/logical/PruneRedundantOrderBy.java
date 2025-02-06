/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

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
            IdentityHashMap<OrderBy, Void> redundant = findRedundantSort(((UnaryPlan) plan).child());
            if (redundant.isEmpty()) {
                return plan;
            }
            return plan.transformUp(p -> {
                if (redundant.containsKey(p)) {
                    return ((OrderBy) p).child();
                }
                return p;
            });
        } else {
            return plan;
        }
    }

    private IdentityHashMap<OrderBy, Void> findRedundantSort(LogicalPlan plan) {
        List<LogicalPlan> toCheck = new ArrayList<>();
        toCheck.add(plan);

        IdentityHashMap<OrderBy, Void> result = new IdentityHashMap<>();
        LogicalPlan p = null;
        while (true) {
            if (p == null) {
                if (toCheck.isEmpty()) {
                    return result;
                } else {
                    p = toCheck.remove(0);
                }
            } else if (p instanceof OrderBy ob) {
                result.put(ob, null);
                p = ob.child();
            } else if (p instanceof UnaryPlan unary) {
                if (unary instanceof Project
                    || unary instanceof Drop
                    || unary instanceof Rename
                    || unary instanceof MvExpand
                    || unary instanceof Enrich
                    || unary instanceof RegexExtract
                    || unary instanceof InlineStats
                    || unary instanceof Lookup
                // IMPORTANT
                // If we introduce window functions or order-sensitive aggs (eg. STREAMSTATS),
                // the previous sort could actually become relevant
                // so we have to be careful with plans that could use them, ie. the following
                    || unary instanceof Filter
                    || unary instanceof Eval
                    || unary instanceof Aggregate) {
                    p = unary.child();
                } else {
                    // stop here, other unary plans could be sensitive to SORT
                    p = null;
                }
            } else if (p instanceof Join lj) {
                toCheck.add(lj.left());
                toCheck.add(lj.right());
                p = null;
            } else {
                // stop here, other unary plans could be sensitive to SORT
                p = null;
            }
        }
    }
}
