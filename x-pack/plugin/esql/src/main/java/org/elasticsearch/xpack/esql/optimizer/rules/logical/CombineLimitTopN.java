/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.listSemanticEquals;

/**
 * Combines a Limit immediately followed by a TopN into a single TopN.
 * This is needed because {@link HoistRemoteEnrichTopN} can create new TopN nodes that are not covered by the previous rules.
 */
public final class CombineLimitTopN extends OptimizerRules.OptimizerRule<UnaryPlan> {

    public CombineLimitTopN() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(UnaryPlan plan) {
        if (plan instanceof Limit == false && plan instanceof LimitBy == false) {
            return plan;
        }
        if (plan instanceof Limit limit && limit.child() instanceof TopN topn) {
            int thisLimitValue = Foldables.limitValue(limit.limit(), limit.sourceText());
            int topNValue = Foldables.limitValue(topn.limit(), topn.sourceText());
            if (topNValue <= thisLimitValue) {
                return topn;
            } else {
                return new TopN(topn.source(), topn.child(), topn.order(), limit.limit(), topn.local());
            }
        }
        if (plan instanceof LimitBy limitBy
            && limitBy.child() instanceof TopNBy topnBy
            && listSemanticEquals(limitBy.groupings(), topnBy.groupings())) {
            int thisLimitValue = Foldables.limitValue(limitBy.limitPerGroup(), limitBy.sourceText());
            int topNValue = Foldables.limitValue(topnBy.limitPerGroup(), topnBy.sourceText());
            if (topNValue <= thisLimitValue) {
                return topnBy;
            } else {
                return new TopNBy(topnBy.source(), topnBy.child(), topnBy.order(), topnBy.limitPerGroup(), topnBy.groupings());
            }
        }

        if (plan.child() instanceof Project proj) {
            // It is possible that Project is sitting on top of TopN. Swap limit and project then.
            // For LimitBy, only swap if the groupings don't reference aliases introduced by the Project.
            if (plan instanceof LimitBy limitBy
                && Expressions.references(limitBy.groupings()).subsetOf(proj.child().outputSet()) == false) {
                return plan;
            }
            return proj.replaceChild(plan.replaceChild(proj.child()));
        }
        return plan;
    }

    /**
     * Combines the limit from a parent Limit/LimitBy with a child TopN/TopNBy.
     * If the child's limit is already smaller or equal, returns the child as-is.
     * Otherwise, creates a new child with the parent's (smaller) limit via the provided factory.
     */
    private static LogicalPlan combineLimits(
        Expression parentLimit,
        LogicalPlan parent,
        Expression childLimit,
        LogicalPlan child,
        Function<Expression, LogicalPlan> withSmallerLimit
    ) {
        int parentValue = Foldables.limitValue(parentLimit, parent.sourceText());
        int childValue = Foldables.limitValue(childLimit, child.sourceText());
        if (childValue <= parentValue) {
            return child;
        }
        return withSmallerLimit.apply(parentLimit);
    }
}
