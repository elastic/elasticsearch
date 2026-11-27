/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.AnalyzerRule;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AnyParam;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces with {@code true} the conditions that compare with a parameter that stands for "Any" (see {@link AnyParam}), so
 * that a filter that is "Any" matches everything, even the documents where the field is missing.
 * <p>
 * The supported conditions are {@code X == M}, {@code X IN (..., M, ...)}, {@code MV_CONTAINS} and {@code MV_INTERSECTS} with
 * {@code M} as an argument, and {@code M IS NULL}, where {@code M} is the marker and {@code X} any expression. The condition
 * is replaced anywhere an expression can be, except under a {@code NOT}: that would turn "matches everything" into "matches
 * nothing", so the marker stays there, with a reason, and the {@code Verifier} reports it.
 * <p>
 * The condition waits until its other operands are resolved, so that their own errors, like an unknown column, are the ones
 * reported. Then it is type checked as if the marker were {@code null}, which every supported condition accepts in place of a
 * value of any type, except for the values of an {@code IN}, that are also checked with each other: they have to be of
 * compatible types whatever the value is.
 * <p>
 * An {@code aggregate_metric_double} operand of a comparison is checked by its default metric, as the analyzer would compare it.
 * <p>
 * A condition that doesn't type check keeps its marker, as the rules that cast the operands (the implicit casting of the values
 * of an {@code IN}, for example) can still make it valid, and they may need more than a pass of the analyzer. The marker has
 * to be replaced only by what a condition with any value of the parameter would be. {@link #lastResort()} is the rule that runs
 * when the others are done: it keeps the conditions that still don't type check with the {@code null}, so that the error is
 * the one the condition would have for any value of the parameter.
 * <p>
 * Checks that the {@code Verifier} makes on the other operands (an aggregate function in a {@code WHERE}, for example) don't
 * see the condition once it is {@code true}: a query can be valid because a parameter is "Any" and not otherwise.
 */
public class ReplaceAnyParams extends AnalyzerRule<LogicalPlan> {

    static final String UNDER_NOT = "under a NOT or a !=, as it would match nothing instead of everything";

    /** Whether the conditions that don't type check are kept with a {@code null} instead of waiting for the casting rules. */
    private final boolean last;

    public ReplaceAnyParams() {
        this(false);
    }

    private ReplaceAnyParams(boolean last) {
        this.last = last;
    }

    /**
     * The rule for when the analysis of the query is resolved: the conditions that still don't type check are kept with a
     * {@code null} in place of the marker, so that the {@code Verifier} reports the type error.
     */
    public static ReplaceAnyParams lastResort() {
        return new ReplaceAnyParams(true);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return plan.transformExpressionsOnly(Not.class, ReplaceAnyParams::reasonUnderNot)
            .transformExpressionsOnlyUp(Expression.class, this::replace);
    }

    /**
     * Whether the expression is one of the conditions that are replaced when a parameter in it is "Any", whatever is above it
     * and whatever its other operands are.
     */
    public static boolean isSupportedCondition(Expression e) {
        boolean shape = e instanceof Equals
            || e instanceof In
            || e instanceof MvContains
            || e instanceof MvIntersects
            || e instanceof IsNull;
        return shape && e.children().stream().anyMatch(AnyParam.class::isInstance);
    }

    /** Marks the "Any" parameters of the supported conditions under a {@code NOT}, so that they aren't replaced. */
    private static Expression reasonUnderNot(Not not) {
        return not.transformDown(e -> {
            if (isSupportedCondition(e) == false) {
                return e;
            }
            boolean changed = false;
            List<Expression> children = new ArrayList<>(e.children().size());
            for (Expression child : e.children()) {
                if (child instanceof AnyParam any && any.reason() == null) {
                    children.add(any.withReason(UNDER_NOT));
                    changed = true;
                } else {
                    children.add(child);
                }
            }
            return changed ? e.replaceChildren(children) : e;
        });
    }

    private Expression replace(Expression e) {
        if (isSupportedCondition(e) == false) {
            return e;
        }
        List<Expression> children = new ArrayList<>(e.children().size());
        for (Expression child : e.children()) {
            if (child instanceof AnyParam any) {
                if (any.reason() != null) {
                    return e;
                }
                children.add(Literal.NULL);
            } else if (child.resolved()) {
                children.add(e instanceof EsqlBinaryComparison ? withDefaultMetric(child) : child);
            } else {
                return e;
            }
        }
        Expression failing = failingTypeCheck(e, e.replaceChildren(children));
        if (failing == null) {
            return new Literal(e.source(), Boolean.TRUE, DataType.BOOLEAN);
        }
        return last ? failing : e;
    }

    /**
     * The operand of a comparison as the comparison will have it: an {@code aggregate_metric_double} is compared by its default
     * metric, but the analyzer converts it only when both operands of the comparison are resolved, which the marker prevents.
     */
    private static Expression withDefaultMetric(Expression operand) {
        if (operand.dataType() != DataType.AGGREGATE_METRIC_DOUBLE) {
            return operand;
        }
        return FromAggregateMetricDouble.withMetric(operand.source(), operand, AggregateMetricDoubleBlockBuilder.Metric.DEFAULT);
    }

    /**
     * The expression that has the type error the condition would have for any value of the parameter, or {@code null} if it has none.
     *
     * @param withNull the condition with a {@code null} in place of the markers
     */
    private static Expression failingTypeCheck(Expression condition, Expression withNull) {
        if (withNull.typeResolved().unresolved()) {
            return withNull;
        }
        if (condition instanceof In in && in.value() instanceof AnyParam) {
            // A null value is compatible with a list of any types, but a value of any type is not: check the values of the list
            // with a value of the type of the first one, which doesn't ask the first one more than it would have to be.
            List<Expression> typed = ((In) withNull).list().stream().filter(v -> v.dataType() != DataType.NULL).toList();
            if (typed.size() > 1) {
                Expression value = new Literal(in.source(), null, typed.getFirst().dataType());
                In listAmongItself = new In(in.source(), value, typed.subList(1, typed.size()));
                if (listAmongItself.typeResolved().unresolved()) {
                    return listAmongItself;
                }
            }
        }
        return null;
    }
}
