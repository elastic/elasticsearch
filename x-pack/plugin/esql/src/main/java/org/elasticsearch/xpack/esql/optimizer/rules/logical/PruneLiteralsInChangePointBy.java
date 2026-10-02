/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Prune foldable groupings from {@code CHANGE_POINT BY}. A foldable expression evaluates to the same constant for every row,
 * so it has no grouping effect and can be dropped. Unlike {@link PruneLiteralsInLimitBy}, an all-foldable {@code BY} clause
 * simply degenerates to an ungrouped {@code CHANGE_POINT} — there is no simpler plan node to substitute.
 * Groupings arrive from the parser as either raw {@link org.elasticsearch.xpack.esql.core.expression.Attribute}s
 * or {@link Alias} nodes wrapping the expression (introduced by {@link ReplaceChangePointByExpressionWithEval}).
 * {@link Alias#foldable()} is always {@code false}, so we unwrap to check the child.
 */
public final class PruneLiteralsInChangePointBy extends OptimizerRules.OptimizerRule<ChangePoint> {

    @Override
    protected LogicalPlan rule(ChangePoint changePoint) {
        List<Expression> newGroupings = new ArrayList<>();
        for (Expression g : changePoint.groupings()) {
            Expression toCheck = Alias.unwrap(g);
            if (toCheck.foldable() == false) {
                newGroupings.add(g);
            }
        }

        if (newGroupings.size() == changePoint.groupings().size()) {
            return changePoint;
        }
        return new ChangePoint(
            changePoint.source(),
            changePoint.child(),
            changePoint.value(),
            changePoint.key(),
            changePoint.targetType(),
            changePoint.targetPvalue(),
            newGroupings
        );
    }
}
