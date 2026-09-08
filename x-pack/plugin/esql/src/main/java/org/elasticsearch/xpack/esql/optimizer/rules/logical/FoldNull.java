/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

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

        if (e instanceof In in) {
            if (Expressions.isGuaranteedNull(in.value())) {
                return Literal.of(in, null);
            }
        } else if (canBeReplacedByNull(e) && (nullChildMakesItNull(e) || nullPropagatingWithNullChild(e))) {
            return Literal.of(e, null);
        }
        return e;
    }

    /**
     * Some expressions must survive in the plan even when they always evaluate to null, because
     * later stages depend on their presence rather than on their value.
     */
    private static boolean canBeReplacedByNull(Expression e) {
        return e instanceof Alias == false
            // Non-evaluatable functions stay as a STATS grouping (It isn't moved to an early EVAL like other groupings),
            // so folding it to null would currently break the plan, as we don't create an attribute/channel for that null value.
            && e instanceof GroupingFunction.NonEvaluatableGroupingFunction == false
            // We cannot fold aggregate functions until we resolve https://github.com/elastic/elasticsearch/issues/100634.
            // AggregateMapper cannot handle aggregate functions with literal values.
            && e instanceof AggregateFunction == false;
    }

    private static boolean nullChildMakesItNull(Expression e) {
        return e.nullable() == Nullability.TRUE && e.children().stream().anyMatch(FoldNull::isNull);
    }

    /**
     * {@link AnyNullIsNull} means a null argument forces a null result, so a guaranteed-null child
     * is enough to fold regardless of what the expression reports as its own nullability.
     *
     * <p>This is what {@link #nullChildMakesItNull} misses. COALESCE, CASE and MV_UNION report
     * {@link Nullability#UNKNOWN}, and {@link Expressions#nullable} propagates that upwards, so a
     * parent holding one of them is never seen as nullable. The parent then survives into
     * {@code toEvaluator}, which switches on the argument type, has no NULL branch, and throws.
     */
    private static boolean nullPropagatingWithNullChild(Expression e) {
        return e instanceof AnyNullIsNull && e.children().stream().anyMatch(Expressions::isGuaranteedNull);
    }

    private static boolean isNull(Expression e) {
        return Expressions.isGuaranteedNull(e) || e.nullable() == Nullability.TRUE && e.children().stream().anyMatch(FoldNull::isNull);
    }

}
