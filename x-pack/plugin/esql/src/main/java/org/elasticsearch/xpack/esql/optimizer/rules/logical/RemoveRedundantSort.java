/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

/**
 * SORT cannot be executed without a LIMIT, as ES|QL doesn't support unbounded sort (yet).
 *
 * The planner tries to push down LIMIT and transform all the unbounded sorts into a TopN.
 * In some cases it's not possible though, eg.
 *
 * from test | sort x | lookup join lookup on x | sort y
 *
 * from test | sort x | mv_expand x | sort y
 *
 * "sort y" will become a TopN, but "sort x" will remain unbounded, so the query could not be executed.
 *
 * In most cases though, last SORT make the previous SORTs redundant,
 * ie. it will re-sort previously sorted results
 * often with a different order.
 *
 * This rule finds and removes redundant SORTs, making the plan executable.
 */
public class RemoveRedundantSort extends OptimizerRules.OptimizerRule<TopN> {

    @Override
    protected LogicalPlan rule(TopN plan) {
        OrderBy redundant = findRedundantSort(plan);
        if (redundant == null) {
            return plan;
        }
        return plan.transformDown(p -> {
            if (p == redundant) {
                return redundant.child();
            }
            return p;
        });
    }

    private OrderBy findRedundantSort(TopN plan) {
        LogicalPlan p = plan.child();
        while (true) {
            if (p instanceof OrderBy ob) {
                return ob;
            }
            if (p instanceof UnaryPlan unary) {
                if (unary instanceof Filter
                    || unary instanceof Project
                    || unary instanceof Rename
                    || unary instanceof MvExpand
                    || unary instanceof Enrich
                    || unary instanceof Grok
                    || unary instanceof Dissect
                // If we introduce window functions, the previous sort could actually become relevant
                // so to be sure we don't introduce regressions, we'll have to exclude places where these functions could be used
                // || unary instanceof Eval
                // || unary instanceof Aggregate
                ) {
                    p = unary.child();
                    continue;
                }
            } else if (p instanceof Join lj) {
                p = lj.left();
                // TODO do it also on the right-hand side?
                continue;
            }
            return null;
        }
    }

}
