/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSingleValueOrNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rewrites multiple {@code SUM(field + c_i)} expressions over the same field into a shared
 * {@code SUM(SINGLE_VALUE_OR_NULL(field))} and {@code COUNT(SINGLE_VALUE_OR_NULL(field))},
 * deriving each original sum via a post-aggregation eval:
 * <pre>
 *     STATS s1 = SUM(x + 1), s2 = SUM(x + 2) BY g
 *     →
 *     STATS _sv_sum = SUM(SINGLE_VALUE_OR_NULL(x)), _sv_count = COUNT(SINGLE_VALUE_OR_NULL(x)) BY g
 *     | EVAL s1 = _sv_sum + 1 * _sv_count, s2 = _sv_sum + 2 * _sv_count
 *     | PROJECT s1, s2, g
 * </pre>
 *
 * <p>The rewrite is only applied when two or more {@code SUM(field + c)} expressions share
 * the same base field expression, since a single such SUM would not benefit (it would produce
 * more aggregates, not fewer).</p>
 *
 * <p>Filtered aggregates ({@code SUM(x + c) WHERE filter}) are excluded because per-agg
 * filters differ and cannot be shared.</p>
 *
 * <p>This rule must run before {@link ReplaceAggregateNestedExpressionWithEval}, which would
 * extract {@code field + c} into a pre-agg EVAL, hiding the pattern.</p>
 */
public class RewriteSumFieldPlusConstant extends OptimizerRules.OptimizerRule<Aggregate> {

    public RewriteSumFieldPlusConstant() {
        super(OptimizerRules.TransformDirection.UP);
    }

    private record Match(Alias alias, Expression dataExpr, Expression constant, Sum sum) {}

    private record SvPair(Attribute sum, Attribute count) {}

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        var source = aggregate.source();

        // Pass 1: collect all SUM(field + const) patterns, grouped by canonical(field).
        // Skip filtered aggregates — they can't share a common SUM(sv_field).
        Map<Expression, List<Match>> groups = new LinkedHashMap<>();
        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof Sum sum
                && sum.filter() == null
                && sum.field() instanceof Add add) {

                Expression dataExpr = null, constant = null;
                if (add.right().foldable() && add.left().foldable() == false) {
                    dataExpr = add.left();
                    constant = add.right();
                } else if (add.left().foldable() && add.right().foldable() == false) {
                    dataExpr = add.right();
                    constant = add.left();
                }
                if (dataExpr != null) {
                    groups.computeIfAbsent(dataExpr.canonical(), k -> new ArrayList<>())
                        .add(new Match(alias, dataExpr, constant, sum));
                }
            }
        }

        // Only rewrite groups with 2+ members. A single SUM(X+c) would produce more
        // aggregates after rewrite, not fewer, so it's not beneficial.
        groups.values().removeIf(list -> list.size() < 2);
        if (groups.isEmpty()) {
            return aggregate;
        }

        // Pass 2: build the replacement plan.
        // Index qualifying matches by alias id for fast lookup.
        Map<Object, Match> aliasIdToMatch = new HashMap<>();
        for (List<Match> matches : groups.values()) {
            for (Match m : matches) {
                aliasIdToMatch.put(m.alias().id(), m);
            }
        }

        Map<Expression, SvPair> fieldToSvPair = new LinkedHashMap<>();
        List<NamedExpression> newAggs = new ArrayList<>();
        List<Alias> newEvals = new ArrayList<>();
        int[] counter = { 0 };

        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias && aliasIdToMatch.containsKey(alias.id())) {
                Match m = aliasIdToMatch.get(alias.id());

                SvPair pair = fieldToSvPair.computeIfAbsent(m.dataExpr().canonical(), k -> {
                    var sv = new MvSingleValueOrNull(source, m.dataExpr());
                    var svSumAlias = new Alias(
                        source,
                        TemporaryNameGenerator.temporaryName(sv, m.sum(), counter[0]++),
                        new Sum(source, sv, null, null, m.sum().summationMode(), m.sum().longOverflowMode()),
                        null,
                        true
                    );
                    var svCountAlias = new Alias(
                        source,
                        TemporaryNameGenerator.temporaryName(sv, m.sum(), counter[0]++),
                        new Count(source, sv, null, null),
                        null,
                        true
                    );
                    newAggs.add(svSumAlias);
                    newAggs.add(svCountAlias);
                    return new SvPair(svSumAlias.toAttribute(), svCountAlias.toAttribute());
                });

                // sv_sum + constant * sv_count — preserve original alias nameId so
                // downstream references to the original output column remain valid.
                var evalExpr = new Add(
                    source,
                    pair.sum(),
                    new Mul(source, m.constant(), pair.count()),
                    ConfigurationAware.CONFIGURATION_MARKER
                );
                newEvals.add(alias.replaceChild(evalExpr));
            } else {
                newAggs.add(agg);
            }
        }

        LogicalPlan plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
        plan = new Eval(source, plan, newEvals);
        // Restore the original output schema (names and order) via a projection.
        plan = new Project(source, plan, Expressions.asAttributes(aggregate.aggregates()));
        return plan;
    }
}
