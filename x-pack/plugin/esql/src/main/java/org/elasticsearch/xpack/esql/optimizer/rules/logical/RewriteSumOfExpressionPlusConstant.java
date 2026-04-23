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
import org.elasticsearch.xpack.esql.core.tree.Source;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rewrites two or more {@code SUM(x ± c)} expressions over the same non-foldable expression
 * {@code x} into {@code SUM(SINGLE_VALUE_OR_NULL(x)) ± c * COUNT(SINGLE_VALUE_OR_NULL(x))},
 * decomposing the per-row arithmetic into separate aggregations on the shared expression:
 * <pre>
 *     STATS s1 = SUM(x + 1), s2 = SUM(x - 2) BY g
 *     →
 *     STATS _sv_sum = SUM(SINGLE_VALUE_OR_NULL(x)), _sv_count = COUNT(SINGLE_VALUE_OR_NULL(x)) BY g
 *     | EVAL s1 = _sv_sum + 1 * _sv_count, s2 = _sv_sum - 2 * _sv_count
 *     | PROJECT s1, s2, g
 * </pre>
 *
 * <p>{@code x} can be any non-foldable expression (a field reference, a function call, etc.).
 * Two SUM expressions share a {@code SUM(sv)/COUNT(sv)} pair when their {@code x} operands are
 * canonically equal. Supported forms: {@code SUM(x ± c)} and {@code SUM(c ± x)}, where exactly
 * one operand is foldable (the constant {@code c}) and the other is not.</p>
 *
 * <p>This rule must run before {@link ReplaceAggregateNestedExpressionWithEval}, which would
 * extract {@code x ± c} into a pre-agg EVAL, hiding the pattern from this rule.</p>
 */
public class RewriteSumOfExpressionPlusConstant extends OptimizerRules.ParameterizedOptimizerRule<Aggregate, LogicalOptimizerContext> {

    public RewriteSumOfExpressionPlusConstant() {
        super(OptimizerRules.TransformDirection.UP);
    }

    private record SvPair(Attribute sum, Attribute count) {}

    private record Match(Alias alias, Expression dataExpr, Expression constant, Sum sum, boolean isSubtraction, boolean constantIsRight) {
        Key key() {
            return new Key(dataExpr.canonical(), sum.summationMode().canonical());
        }

        private record Key(Expression expr, Expression summationMode) {}
    }

    /**
     * Returns a {@link Match} if {@code agg} is {@code SUM(x ± c)} or {@code SUM(c ± x)},
     * where exactly one operand is foldable. Returns {@code null} otherwise.
     */
    private static Match tryMatch(NamedExpression agg) {
        // Every aggregate output is wrapped in an Alias. Filtered aggregates are excluded because
        // they operate on a subset of rows and cannot share a base SUM(sv)/COUNT(sv) with others.
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
        if (context.minimumVersion().supports(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION) == false) {
            return aggregate;
        }
        var source = aggregate.source();

        // Pass 1: count matches and collect field sources per (expression, summationMode) key.
        Map<Match.Key, Long> exprMatchCount = new HashMap<>();
        Map<Match.Key, List<Source>> exprFieldSources = new HashMap<>();
        for (NamedExpression agg : aggregate.aggregates()) {
            Match m = tryMatch(agg);
            if (m != null) {
                exprMatchCount.merge(m.key(), 1L, Long::sum);
                exprFieldSources.computeIfAbsent(m.key(), k -> new ArrayList<>()).add(m.sum().field().source());
            }
        }

        // Only rewrite expressions that appear in 2 or more SUM expressions
        if (exprMatchCount.values().stream().noneMatch(c -> c >= 2)) {
            return aggregate;
        }

        // Pass 2: rewrite eligible expressions, sharing a single SUM(sv)/COUNT(sv) pair per expression.
        Map<Match.Key, SvPair> exprToSvPair = new HashMap<>();
        List<NamedExpression> newAggs = new ArrayList<>();
        List<Alias> newEvals = new ArrayList<>();
        int[] counter = { 0 };

        for (NamedExpression agg : aggregate.aggregates()) {
            Match m = tryMatch(agg);
            if (m != null && exprMatchCount.getOrDefault(m.key(), 0L) >= 2) {
                final Expression de = m.dataExpr();
                final Sum fs = m.sum();
                SvPair pair = exprToSvPair.computeIfAbsent(m.key(), k -> {
                    List<Source> warningSources = exprFieldSources.get(k);
                    var sv = new MvSingleValueOrNull(warningSources.get(0), de, warningSources);
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
                if (m.isSubtraction() && m.constantIsRight()) {
                    // SUM(field - c) → SUM(sv) - c * COUNT(sv)
                    evalExpr = new Sub(source, pair.sum(), countMulConst, context.configuration());
                } else if (m.isSubtraction()) {
                    // SUM(c - field) → c * COUNT(sv) - SUM(sv)
                    evalExpr = new Sub(source, countMulConst, pair.sum(), context.configuration());
                } else {
                    // SUM(field + c) or SUM(c + field) → SUM(sv) + c * COUNT(sv)
                    // We don't need to worry about order since addition is commutative
                    evalExpr = new Add(source, pair.sum(), countMulConst, context.configuration());
                }
                newEvals.add(m.alias().replaceChild(evalExpr));
            } else {
                // agg is not the right form to optimize, so keep the old version
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
