/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Collections.emptySet;

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
        AttributeMap<Expression> aliases = new AttributeMap<>();
        // traverse bottom-up to pick up the aliases as we go
        plan = plan.transformUp(p -> inspectPlan(p, aliases));
        return plan;
    }

    private LogicalPlan inspectPlan(LogicalPlan plan, AttributeMap<Expression> aliases) {
        // inspect just this plan properties
        plan.forEachExpression(Alias.class, a -> aliases.put(a.toAttribute(), a.child()));
        // now go about finding isNull/isNotNull
        LogicalPlan newPlan = plan.transformExpressionsOnlyUp(IsNotNull.class, inn -> inferNotNullable(inn, aliases));
        return newPlan;
    }

    private Expression inferNotNullable(IsNotNull inn, AttributeMap<Expression> aliases) {
        Expression result = inn;
        Set<Expression> refs = resolveExpressionAsRootAttributes(inn.field(), aliases);
        // no refs found or could not detect - return the original function
        if (refs.size() > 0) {
            // add IsNull for the filters along with the initial inn
            var innList = CollectionUtils.combine(refs.stream().map(r -> (Expression) new IsNotNull(inn.source(), r)).toList(), inn);
            result = Predicates.combineAnd(innList);
        }
        return result;
    }

    /**
     * Unroll the expression to its references to get to the root fields
     * that really matter for filtering.
     */
    protected Set<Expression> resolveExpressionAsRootAttributes(Expression exp, AttributeMap<Expression> aliases) {
        Set<Expression> resolvedExpressions = new LinkedHashSet<>();
        boolean changed = doResolve(exp, aliases, resolvedExpressions);
        return changed ? resolvedExpressions : emptySet();
    }

    private boolean doResolve(Expression exp, AttributeMap<Expression> aliases, Set<Expression> resolvedExpressions) {
        boolean changed = false;
        // check if the expression can be skipped
        if (skipExpression(exp)) {
            resolvedExpressions.add(exp);
        } else {
            for (Expression e : exp.references()) {
                Expression resolved = aliases.resolve(e, e);
                // found a root attribute, bail out
                if (resolved instanceof Attribute a && resolved == e) {
                    resolvedExpressions.add(a);
                    // don't mark things as change if the original expression hasn't been broken down
                    changed |= resolved != exp;
                } else {
                    // go further
                    changed |= doResolve(resolved, aliases, resolvedExpressions);
                }
            }
        }
        return changed;
    }

    private static boolean skipExpression(Expression e) {
        // These two functions can have a complex set of expressions as arguments that can mess up the simplification we are trying to add.
        // If there is a "case(f is null, null, ...) is not null" expression,
        // assuming that "case(f is null.....) is not null AND f is not null" (what this rule is doing) is a wrong assumption because
        // the "case" function will want both null "f" and not null "f". Doing it like this contradicts the condition inside case, so we
        // must avoid these cases.
        // We could be smarter and look inside "case" and "coalesce" to see if there is any comparison of fields with "null" but,
        // the complexity is too high to warrant an attempt _now_.
        return e instanceof Coalesce || e instanceof Case;
    }
}
