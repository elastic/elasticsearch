/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites {@code MV_COUNT(VALUES(x))} into the approximate, HyperLogLog++ backed
 * {@link CountDistinct} aggregator. Without this rule {@code VALUES(x)} materializes every
 * contributing value per group before {@code MV_COUNT} counts them, which is {@code O(rows)} in
 * memory; {@code COUNT_DISTINCT} keeps a fixed-size sketch per group instead.
 *
 * <p>The rewrite preserves the observable semantics of {@code MV_COUNT(VALUES(x))} except for
 * exactness:
 * <ul>
 *     <li>the result type stays {@code integer} (wrapped in {@link ToInteger});</li>
 *     <li>a group with no non-null values still yields {@code null}: {@code VALUES} returns
 *     {@code null} (never an empty multivalue) for such a group, so {@code MV_COUNT} returns
 *     {@code null}, whereas {@code COUNT_DISTINCT} returns {@code 0}. Mapping {@code 0 -> null} is
 *     the exact inverse since {@code MV_COUNT(VALUES(x))} can never be {@code 0}.</li>
 * </ul>
 * The replacement expression is {@code CASE(COUNT_DISTINCT(x) == 0, null, TO_INTEGER(COUNT_DISTINCT(x)))}.
 * The two {@code COUNT_DISTINCT(x)} references share a canonical form, so the downstream
 * {@link ReplaceAggregateAggExpressionWithEval} extracts a single aggregator plus a follow-up
 * {@code EVAL}.
 *
 * <p>The rule only fires when the {@code VALUES} result is consumed exclusively by {@code MV_COUNT}.
 * If the same {@code VALUES(x)} is also emitted or used elsewhere the materialization is needed
 * regardless, so rewriting would only add the sketch cost without removing the accumulation.
 * It is also restricted to types supported by {@code COUNT_DISTINCT} and to non-windowed aggregates.
 */
public final class ReplaceMvCountOfValuesWithCountDistinct extends OptimizerRules.OptimizerRule<Aggregate> {

    public ReplaceMvCountOfValuesWithCountDistinct() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        Set<Expression> rewritableValues = rewritableValues(aggregate);
        if (rewritableValues.isEmpty()) {
            return aggregate;
        }

        boolean[] changed = { false };
        List<NamedExpression> newAggregates = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression agg : aggregate.aggregates()) {
            NamedExpression rewritten = (NamedExpression) agg.transformDown(MvCount.class, mvCount -> {
                if (mvCount.field() instanceof Values values && rewritableValues.contains(values.canonical())) {
                    changed[0] = true;
                    return toCountDistinct(values);
                }
                return mvCount;
            });
            newAggregates.add(rewritten);
        }

        return changed[0] ? aggregate.with(aggregate.child(), aggregate.groupings(), newAggregates) : aggregate;
    }

    /**
     * Returns the canonical forms of the {@link Values} aggregates that are safe to rewrite: those
     * supported by {@link CountDistinct}, non-windowed, and consumed exclusively as the direct
     * argument of an {@link MvCount}.
     */
    private static Set<Expression> rewritableValues(Aggregate aggregate) {
        Map<Expression, Integer> totalUses = new HashMap<>();
        Map<Expression, Integer> mvCountUses = new HashMap<>();
        for (NamedExpression agg : aggregate.aggregates()) {
            agg.forEachDown(Values.class, values -> totalUses.merge(values.canonical(), 1, Integer::sum));
            agg.forEachDown(MvCount.class, mvCount -> {
                if (mvCount.field() instanceof Values values) {
                    mvCountUses.merge(values.canonical(), 1, Integer::sum);
                }
            });
        }

        Set<Expression> rewritable = new HashSet<>();
        for (Map.Entry<Expression, Integer> entry : totalUses.entrySet()) {
            Values values = (Values) entry.getKey();
            // every use is wrapped directly by MV_COUNT
            if (entry.getValue().equals(mvCountUses.get(entry.getKey()))
                && values.hasWindow() == false
                && CountDistinct.isSupportedType(values.field().dataType())) {
                rewritable.add(entry.getKey());
            }
        }
        return rewritable;
    }

    private static Expression toCountDistinct(Values values) {
        Source source = values.source();
        // precision left null so COUNT_DISTINCT uses its default threshold
        CountDistinct countDistinct = new CountDistinct(source, values.field(), values.filter(), values.window(), null);
        Equals isEmpty = new Equals(source, countDistinct, new Literal(source, 0L, DataType.LONG));
        Literal nullInteger = new Literal(source, null, DataType.INTEGER);
        ToInteger count = new ToInteger(source, countDistinct);
        return new Case(source, isEmpty, List.of(nullInteger, count));
    }
}
