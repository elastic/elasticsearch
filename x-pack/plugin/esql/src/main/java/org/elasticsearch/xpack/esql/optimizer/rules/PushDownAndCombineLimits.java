/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Literal;
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
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

public final class PushDownAndCombineLimits extends OptimizerRules.OptimizerRule<Limit> {

    @Override
    public LogicalPlan rule(Limit limit) {
        if (limit.child() instanceof Limit childLimit) {
            var limitSource = limit.limit();
            var l1 = (int) limitSource.fold();
            var l2 = (int) childLimit.limit().fold();
            return new Limit(limit.source(), Literal.of(limitSource, Math.min(l1, l2)), childLimit.child());
        } else if (limit.child() instanceof UnaryPlan unary) {
            if (unary instanceof Eval || unary instanceof Project || unary instanceof RegexExtract || unary instanceof Enrich) {
                return unary.replaceChild(limit.replaceChild(unary.child()));
            }
            // check if there's a 'visible' descendant limit lower than the current one
            // and if so, align the current limit since it adds no value
            // this applies for cases such as | limit 1 | sort field | limit 10
            else {
                Limit descendantLimit = descendantLimit(unary);
                if (descendantLimit != null) {
                    var l1 = (int) limit.limit().fold();
                    var l2 = (int) descendantLimit.limit().fold();
                    if (l2 <= l1) {
                        return new Limit(limit.source(), Literal.of(limit.limit(), l2), limit.child());
                    }
                }
            }
        } else if (limit.child() instanceof Join join) {
            if (join.config().type() == JoinType.LEFT && join.right() instanceof LocalRelation) {
                // This is a hash join from something like a lookup.
                return join.replaceChildren(limit.replaceChild(join.left()), join.right());
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
