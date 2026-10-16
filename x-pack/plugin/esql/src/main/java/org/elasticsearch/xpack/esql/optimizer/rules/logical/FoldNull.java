/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import java.util.function.Predicate;

public class FoldNull extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public FoldNull() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public Expression rule(Expression e, LogicalOptimizerContext ctx) {

        // convert an aggregate null filter into a false
        // perform this early to prevent the rule from converting the null filter into nullifying the whole expression
        // P.S. this could be done inside the Aggregate but this place better centralizes the logic
        if (e instanceof AggregateFunction agg) {
            if (Expressions.isGuaranteedNull(agg.filter())) {
                return agg.withFilter(Literal.of(agg.filter(), false));
            }
        }

        if (foldsToNull(e, FoldNull::isNull)) {
            return Literal.of(e, null);
        }
        return e;
    }

    /**
     * Whether {@code e} is guaranteed to evaluate to {@code NULL}, judging null-ness of its direct inputs
     * with {@code isNullChild}.
     * <p>
     * Both callers share the shapes but deliberately not the null check. This rule passes its transitive check,
     * folding anything provably null, so {@code x + (y + NULL)} folds as a whole. The pre-optimizer
     * {@code WarnNullMisuse} only accepts an explicit, user-written {@code NULL} as a direct input, so it warns
     * on the innermost {@code y + NULL} and not on expressions that are null for other reasons.
     */
    public static boolean foldsToNull(Expression e, Predicate<Expression> isNullChild) {
        if (e instanceof In in) {
            return isNullChild.test(in.value());
        }
        return e instanceof Alias == false && e.nullable() == Nullability.TRUE
        // Non-evaluatable functions stay as a STATS grouping (It isn't moved to an early EVAL like other groupings),
        // so folding it to null would currently break the plan, as we don't create an attribute/channel for that null value.
            && e instanceof GroupingFunction.NonEvaluatableGroupingFunction == false
            // We cannot fold aggregate functions until we resolve https://github.com/elastic/elasticsearch/issues/100634.
            // AggregateMapper cannot handle aggregate functions with literal values. Aggregates over null inputs are instead
            // replaced with a literal by ReplaceStatsFilteredOrNullAggWithEval.
            && e instanceof AggregateFunction == false
            && e.children().stream().anyMatch(isNullChild);
    }

    private static boolean isNull(Expression e) {
        return Expressions.isGuaranteedNull(e) || e.nullable() == Nullability.TRUE && e.children().stream().anyMatch(FoldNull::isNull);
    }

}
