/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Simplify IsNotNull targets by resolving the underlying expression to its root fields.
 * e.g.
 * (x + 1) / 2 IS NOT NULL --> x IS NOT NULL AND (x+1) / 2 IS NOT NULL
 * SUBSTRING(x, 3) > 4 IS NOT NULL --> x IS NOT NULL AND SUBSTRING(x, 3) > 4 IS NOT NULL
 * When dealing with multiple fields, a conjunction/disjunction based on the predicate:
 * (x + y) / 4 IS NOT NULL --> x IS NOT NULL AND y IS NOT NULL AND (x + y) / 4 IS NOT NULL
 * This handles the case of fields nested inside functions or expressions in order to avoid:
 * - having to evaluate the whole expression
 * - not pushing down the filter due to expression evaluation
 * Only functions with {@link AnyNullIsNull} can propagate IsNotNull.
 * IS NULL cannot be simplified since it leads to a disjunction which prevents the filter to be
 * pushed down:
 * (x + 1) IS NULL --> x IS NULL OR x + 1 IS NULL
 * and x IS NULL cannot be pushed down
 * <br/>
 * Implementation-wise this rule goes bottom-up, keeping an alias up to date to the current plan
 * and then looks for replacing the target.
 */
public class InferIsNotNull extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        // the alias map is shared across the whole plan
        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
        // traverse bottom-up to pick up the aliases as we go
        plan = plan.transformUp(p -> inspectPlan(p, aliasesBuilder));
        return plan;
    }

    private LogicalPlan inspectPlan(LogicalPlan plan, AttributeMap.Builder<Expression> aliasesBuilder) {
        // inspect just this plan properties
        plan.forEachExpression(Alias.class, a -> aliasesBuilder.put(a.toAttribute(), a.child()));
        // now go about finding isNull/isNotNull
        LogicalPlan newPlan = plan.transformExpressionsOnlyUp(
            IsNotNull.class,
            inn -> inferNotNullable(inn, aliasesBuilder.build(), plan.inputSet())
        );
        return newPlan;
    }

    private Expression inferNotNullable(IsNotNull inn, AttributeMap<Expression> aliases, AttributeSet inputSet) {
        Expression result = inn;
        Set<Expression> refs = resolveExpressionAsRootAttributes(inn.field(), aliases, inputSet);
        // no refs found or could not detect - return the original function
        if (refs.size() > 0) {
            // add IsNotNull for the filters along with the initial inn
            var innList = CollectionUtils.combine(refs.stream().map(r -> (Expression) new IsNotNull(inn.source(), r)).toList(), inn);
            result = Predicates.combineAnd(innList);
        }
        return result;
    }

    private Set<Expression> resolveExpressionAsRootAttributes(Expression exp, AttributeMap<Expression> aliases, AttributeSet inputSet) {
        Set<Expression> resolvedExpressions = new LinkedHashSet<>();
        resolve(exp, exp, aliases, inputSet, resolvedExpressions);
        return resolvedExpressions;
    }

    private void resolve(
        Expression exp,
        Expression originalExp,
        AttributeMap<Expression> aliases,
        AttributeSet inputSet,
        Set<Expression> resolvedExpressions
    ) {
        Expression resolved = aliases.resolve(exp, exp);
        if (resolved instanceof Attribute a) {
            if (inputSet.contains(resolved) && resolved != originalExp) {
                resolvedExpressions.add(a);
            }
            return;
        }
        if (resolved instanceof AnyNullIsNull == false) {
            return;
        }

        for (Expression child : resolved.children()) {
            resolve(child, originalExp, aliases, inputSet, resolvedExpressions);
        }
    }
}
