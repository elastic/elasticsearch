/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

public final class PushDownAndCombineLimits extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {

    public PushDownAndCombineLimits() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (limit.child() instanceof Limit childLimit) {
            var limitSource = limit.limit();
            var parentLimitValue = (int) limitSource.fold(ctx.foldCtx());
            var childLimitValue = (int) childLimit.limit().fold(ctx.foldCtx());
            // We want to preserve the duplicated() value of the smaller limit, so we'll use replaceChild.
            return parentLimitValue < childLimitValue ? limit.replaceChild(childLimit.child()) : childLimit;
        } else if (limit.child() instanceof UnaryPlan unary) {
            if (unary instanceof Eval || unary instanceof Project || unary instanceof RegexExtract || unary instanceof Enrich) {
                return unary.replaceChild(limit.replaceChild(unary.child()));
            } else if (unary instanceof MvExpand mvx && limit.duplicated() == false) {
                // MV_EXPAND can increase the number of rows, so we cannot just push the limit down
                // (we also have to preserve the LIMIT afterwards)
                // To avoid repeating this infinitely, we have to set duplicated = true.
                MvExpand newChild = mvx.replaceChild(limit.replaceChild(mvx.child()));
                return limit.replaceChild(newChild).withDuplicated(true);
            }
            // check if there's a 'visible' descendant limit lower than the current one
            // and if so, align the current limit since it adds no value
            // this applies for cases such as | limit 1 | sort field | limit 10
            else {
                Limit descendantLimit = descendantLimit(unary);
                if (descendantLimit != null) {
                    var l1 = (int) limit.limit().fold(ctx.foldCtx());
                    var l2 = (int) descendantLimit.limit().fold(ctx.foldCtx());
                    if (l2 <= l1) {
                        return limit.withLimit(Literal.of(limit.limit(), l2));
                    }
                }
            }
        } else if (limit.child() instanceof Join join && limit.duplicated() == false) {
            if (join.config().type() == JoinTypes.LEFT) {
                // Left joins increase the number of rows if any join key has multiple matches from the right hand side.
                // Therefore, we cannot simply push down the limit - but we can add another limit before the join.
                // To avoid repeating this infinitely, we have to set duplicated = true.
                Join newChild = join.replaceChildren(limit.replaceChild(join.left()), join.right());
                return limit.replaceChild(newChild).withDuplicated(true);
            }
        }
        return limit;
    }

    /**
     * Checks the existence of another 'visible' Limit, that exists behind an operation that doesn't produce output more data than
     * its input (that is not a relation/source nor aggregation).
     * P.S. Typically an aggregation produces less data than the input.
     */
    private static Limit descendantLimit(UnaryPlan unary) {
        UnaryPlan plan = unary;
        while (plan instanceof Aggregate == false) {
            if (plan instanceof Limit limit) {
                return limit;
            } else if (plan instanceof MvExpand) {
                // the limit that applies to mv_expand shouldn't be changed
                // ie "| limit 1 | mv_expand x | limit 20" where we want that last "limit" to apply on expand results
                return null;
            }
            if (plan.child() instanceof UnaryPlan unaryPlan) {
                plan = unaryPlan;
            } else {
                break;
            }
        }
        return null;
    }
}
