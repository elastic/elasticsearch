/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Extract non-attribute {@link ChangePoint} grouping expressions into a synthetic {@link Eval}.
 * <p>
 * For example, {@code CHANGE_POINT v ON k BY LENGTH(label)} becomes
 * {@code EVAL $$change_point_by_0 = LENGTH(label) | CHANGE_POINT v ON k BY $$change_point_by_0}.
 * {@link PushDownEval} in the operators batch takes care of pushing the Eval below any
 * {@link org.elasticsearch.xpack.esql.plan.logical.OrderBy} if present.
 * <p>
 * Foldable groupings are pruned separately by {@link PruneLiteralsInChangePointBy} in the operators batch.
 */
public final class ReplaceChangePointByExpressionWithEval extends OptimizerRules.OptimizerRule<ChangePoint> {
    private static int counter = 0;

    @Override
    protected LogicalPlan rule(ChangePoint changePoint) {
        int size = changePoint.groupings().size();
        List<Expression> newGroupings = new ArrayList<>(changePoint.groupings());
        List<Alias> evals = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            Expression g = newGroupings.get(i);
            if (g.foldable()) {
                continue;
            }
            if (g instanceof Attribute == false) {
                var name = rawTemporaryName("change_point_by", String.valueOf(i), String.valueOf(counter++));
                var alias = new Alias(g.source(), name, g, null, true);
                evals.add(alias);
                newGroupings.set(i, alias.toAttribute());
            }
        }

        if (evals.isEmpty()) {
            return changePoint;
        }

        var originalOutput = changePoint.output();
        var evalChild = new Eval(changePoint.source(), changePoint.child(), evals);
        var newChangePoint = new ChangePoint(
            changePoint.source(),
            evalChild,
            changePoint.value(),
            changePoint.key(),
            changePoint.targetType(),
            changePoint.targetPvalue(),
            newGroupings
        );
        // Project restores the original output, hiding the synthetic $$change_point_by_N columns —
        // CHANGE_POINT BY grouping expressions do not produce new output columns.
        return new Project(changePoint.source(), newChangePoint, originalOutput);
    }
}
