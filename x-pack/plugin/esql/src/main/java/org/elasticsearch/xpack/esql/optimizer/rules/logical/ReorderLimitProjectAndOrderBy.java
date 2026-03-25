/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

/**
 * This is to fix possible wrong orderings of Limit, Project and Order or LimitBy, Project and OrderBy.
 *
 * We should always end up with Limit on top of OrderBy so we can turn it into TopN
 * We should always end up with LimitBy on top of OrderBy so we can turn it into TopNBy
 */
public final class ReorderLimitProjectAndOrderBy extends OptimizerRules.OptimizerRule<UnaryPlan> {
    public ReorderLimitProjectAndOrderBy() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(UnaryPlan plan) {
        // It is possible that Project is sitting between Limit and OrderBy.
        // Swap Limit and Project so we always have Project -> Limit at the end
        if (plan instanceof Limit limit && plan.child() instanceof Project proj) {
            return proj.replaceChild(limit.replaceChild(proj.child()));
            // For LimitBy, only swap if the groupings don't reference aliases introduced by the Project.
        } else if (plan instanceof LimitBy limitBy
            && plan.child() instanceof Project proj
            && Expressions.references(limitBy.groupings()).subsetOf(proj.child().outputSet())) {
                return proj.replaceChild(limitBy.replaceChild(proj.child()));
                // If swapping Project and Limit / LimitBy was not possible, swap Project and OrderBy if possible
                // Make sure all of the references OrderBy uses are included in the Project, because there could be re-aliasing there
            } else if (plan instanceof Project proj
                && plan.child() instanceof OrderBy orderBy
                && Expressions.references(orderBy.expressions()).subsetOf(proj.child().outputSet())) {
                    return orderBy.replaceChild(proj.replaceChild(orderBy.child()));
                }

        return plan;
    }
}
