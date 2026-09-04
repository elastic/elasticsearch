/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Prune foldable groupings from {@code LIMIT BY}. A foldable expression evaluates to the same constant for every row,
 * so it has no grouping effect. If all groupings are foldable the {@code LIMIT BY} degenerates to a plain {@code LIMIT}.
 * Groupings arrive from the parser as either raw {@link org.elasticsearch.xpack.esql.core.expression.Attribute}s
 * or {@link Alias} nodes wrapping the expression. {@link Alias#foldable()} is always {@code false},
 * so we unwrap to check the child.
 */
public final class PruneLiteralsInLimitBy extends OptimizerRules.OptimizerRule<LimitBy> {

    @Override
    protected LogicalPlan rule(LimitBy limitBy) {
        List<Expression> newGroupings = new ArrayList<>();
        for (Expression g : limitBy.groupings()) {
            Expression toCheck = g instanceof Alias as ? as.child() : g;
            if (toCheck.foldable() == false) {
                newGroupings.add(g);
            }
        }

        if (newGroupings.size() == limitBy.groupings().size()) {
            return limitBy;
        }
        if (newGroupings.isEmpty()) {
            return new Limit(limitBy.source(), limitBy.limitPerGroup(), limitBy.child());
        }
        return new LimitBy(limitBy.source(), limitBy.limitPerGroup(), limitBy.child(), newGroupings, limitBy.duplicated());
    }
}
