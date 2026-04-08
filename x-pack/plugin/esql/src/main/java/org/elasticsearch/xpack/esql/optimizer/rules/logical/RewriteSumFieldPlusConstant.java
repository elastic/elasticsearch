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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSingleValueOrNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Rewrites two or more {@code SUM(field ± c)} expressions over the same field into
 * {@code SUM(SINGLE_VALUE_OR_NULL(field)) ± c * COUNT(SINGLE_VALUE_OR_NULL(field))},
 * decomposing the per-row arithmetic into separate aggregations on the raw field:
 * <pre>
 *     STATS s1 = SUM(x + 1), s2 = SUM(x - 2) BY g
 *     →
 *     STATS _sv_sum = SUM(SINGLE_VALUE_OR_NULL(x)), _sv_count = COUNT(SINGLE_VALUE_OR_NULL(x)) BY g
 *     | EVAL s1 = _sv_sum + 1 * _sv_count, s2 = _sv_sum - 2 * _sv_count
 *     | PROJECT s1, s2, g
 * </pre>
 *
 * <p>Supported {@code SUM(field ± c)} and {@code SUM(c ± field)}, where exactly one operand
 * is foldable (the constant) and the other is not (the field).</p>
 *
 * <p>This rule must run before {@link ReplaceAggregateNestedExpressionWithEval}, which would
 * extract {@code field ± c} into a pre-agg EVAL, hiding the pattern from this rule.</p>
 */
public class RewriteSumFieldPlusConstant extends OptimizerRules.ParameterizedOptimizerRule<Aggregate, LogicalOptimizerContext> {

    public RewriteSumFieldPlusConstant() {
        super(OptimizerRules.TransformDirection.UP);
    }

    private record SvPair(Attribute sum, Attribute count) {}

    /**
     * A matched {@code SUM(field ± c)} expression.
     */
    private record Match(Alias alias, Expression dataExpr, Expression constant, Sum sum, boolean isSubtraction, boolean fieldIsLeft) {
        Key key() {
            return new Key(dataExpr.canonical(), sum.summationMode().canonical());
        }

        /**
         * Can share SUM/COUNT if expressions are over the same field and the sum uses the same summation mode.
         */
        private record Key(Expression field, Expression summationMode) {}
    }

    /**
     * Returns a {@link Match} if {@code agg} is an unfiltered {@code SUM(field ± c)} where
     * exactly one operand is foldable (the constant) and the other is not (the field),
     * or {@code null} if it does not match.
     */
    private static Match tryMatch(NamedExpression agg) {
        if (!(agg instanceof Alias alias) || !(alias.child() instanceof Sum s) || s.hasFilter()) {
            return null;
        }
        if (s.field() instanceof Add add) {
            if (add.right().foldable() && add.left().foldable() == false) {
                return new Match(alias, add.left(), add.right(), s, false, true);
            } else if (add.left().foldable() && add.right().foldable() == false) {
                return new Match(alias, add.right(), add.left(), s, false, false);
            }
        } else if (s.field() instanceof Sub sub) {
            if (sub.right().foldable() && sub.left().foldable() == false) {
                return new Match(alias, sub.left(), sub.right(), s, true, true);
            } else if (sub.left().foldable() && sub.right().foldable() == false) {
                return new Match(alias, sub.right(), sub.left(), s, true, false);
            }
        }
        return null;
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate, LogicalOptimizerContext context) {
        var source = aggregate.source();

        // Pass 1: count how many SUM(field ± c) or SUM(c ± field) expressions share the same (field, summationMode).
        // Filtered aggregates are excluded because they cannot share the same base aggregation.
        Map<Match.Key, Long> fieldMatchCount = aggregate.aggregates().stream()
                .map(RewriteSumFieldPlusConstant::tryMatch)
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(Match::key, Collectors.counting()));

        // Only rewrite fields that appear in 2 or more SUM expressions
        if (fieldMatchCount.values().stream().noneMatch(c -> c >= 2)) {
            return aggregate;
        }

        // Pass 2: rewrite eligible expressions, sharing a single SUM(sv)/COUNT(sv) pair per field.
        Map<Match.Key, SvPair> fieldToSvPair = new HashMap<>();
        List<NamedExpression> newAggs = new ArrayList<>();
        List<Alias> newEvals = new ArrayList<>();
        int[] counter = { 0 };

        for (NamedExpression agg : aggregate.aggregates()) {
            Match m = tryMatch(agg);
            if (m != null && fieldMatchCount.getOrDefault(m.key(), 0L) >= 2) {
                final Expression de = m.dataExpr();
                final Sum fs = m.sum();
                SvPair pair = fieldToSvPair.computeIfAbsent(m.key(), k -> {
                    var sv = new MvSingleValueOrNull(source, de);
                    var svSumName = TemporaryNameGenerator.temporaryName(sv, fs, counter[0]++);
                    var svSumExpr = new Sum(
                        source,
                        sv,
                        Literal.TRUE,
                        AggregateFunction.NO_WINDOW,
                        fs.summationMode(),
                        fs.longOverflowMode()
                    );
                    var svSumAlias = new Alias(source, svSumName, svSumExpr, null, true);
                    newAggs.add(svSumAlias);
                    var svCountName = TemporaryNameGenerator.temporaryName(sv, fs, counter[0]++);
                    var svCountExpr = new Count(source, sv);
                    var svCountAlias = new Alias(source, svCountName, svCountExpr, null, true);
                    newAggs.add(svCountAlias);
                    return new SvPair(svSumAlias.toAttribute(), svCountAlias.toAttribute());
                });

                var countMulConst = new Mul(source, m.constant(), pair.count());
                Expression evalExpr;
                if (m.isSubtraction()) {
                    var sub = new Sub(source, pair.sum(), countMulConst, context.configuration());
                    evalExpr = m.fieldIsLeft() ? sub : sub.swapLeftAndRight();
                } else {
                    evalExpr = new Add(source, pair.sum(), countMulConst, context.configuration());
                    // original field order doesn't matter since addition commutative
                }
                newEvals.add(m.alias().replaceChild(evalExpr));
            } else {
                newAggs.add(agg);
            }
        }

        assert newEvals.isEmpty() == false : "expected at least one rewrite since a field with count >= 2";

        LogicalPlan plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
        plan = new Eval(source, plan, newEvals);
        plan = new Project(source, plan, Expressions.asAttributes(aggregate.aggregates()));
        return plan;
    }
}
