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
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Extract non-attribute {@link Limit} grouping expressions into a synthetic {@link Eval}.
 * <p>
 * For example, {@code LIMIT N BY languages * 2} becomes
 * {@code EVAL $$limit_by_0 = languages * 2 | LIMIT N BY $$limit_by_0}.
 * {@link PushDownEval} in the operators batch takes care of pushing the Eval below any
 * {@link org.elasticsearch.xpack.esql.plan.logical.OrderBy} if present.
 * <p>
 * Foldable groupings are pruned separately by {@link PruneLiteralsInLimitBy} in the operators batch.
 */
public final class ReplaceLimitByExpressionWithEval extends OptimizerRules.OptimizerRule<Limit> {

    @Override
    protected LogicalPlan rule(Limit limit) {
        if (limit.groupings().isEmpty()) {
            return limit;
        }

        int counter = 0;
        int size = limit.groupings().size();
        List<Expression> newGroupings = new ArrayList<>(limit.groupings());
        List<Alias> evals = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            Expression g = newGroupings.get(i);
            Expression underlying = g instanceof Alias as ? as.child() : g;
            if (underlying.foldable()) {
                // Foldable expressions are left as-is; PruneLiteralsInLimitBy will prune them in the operators batch.
                continue;
            }
            if (g instanceof Alias as) {
                evals.add(as);
                newGroupings.set(i, as.toAttribute());
            } else if (g instanceof Attribute == false) {
                var name = rawTemporaryName("LIMIT BY", String.valueOf(i), String.valueOf(counter++));
                var alias = new Alias(g.source(), name, g, null, true);
                evals.add(alias);
                newGroupings.set(i, alias.toAttribute());
            }
        }

        if (evals.isEmpty()) {
            return limit;
        }

        var originalOutput = limit.output();
        var evalChild = new Eval(limit.source(), limit.child(), evals);
        var newLimit = new Limit(limit.source(), limit.limit(), evalChild, newGroupings, limit.duplicated(), limit.local());
        return new Project(limit.source(), newLimit, originalOutput);
    }
}
