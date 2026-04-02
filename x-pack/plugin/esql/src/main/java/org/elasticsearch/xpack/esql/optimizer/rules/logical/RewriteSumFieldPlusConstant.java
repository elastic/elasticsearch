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
import java.util.List;
import java.util.Map;

/**
 * Rewrites {@code SUM(field + c)} expressions into a shared
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
 * <p>Multiple {@code SUM(field + c_i)} over the same field share a single pair of aggregates.
 * Filtered aggregates ({@code SUM(x + c) WHERE filter}) are skipped.</p>
 *
 * <p>This rule must run before {@link ReplaceAggregateNestedExpressionWithEval}, which would
 * extract {@code field + c} into a pre-agg EVAL, hiding the pattern.</p>
 */
public class RewriteSumFieldPlusConstant extends OptimizerRules.OptimizerRule<Aggregate> {

    public RewriteSumFieldPlusConstant() {
        super(OptimizerRules.TransformDirection.UP);
    }

    private record SvPair(Attribute sum, Attribute count) {}

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        var source = aggregate.source();

        // Maps canonical(field) → shared SUM(sv_field) / COUNT(sv_field) attributes.
        Map<Expression, SvPair> fieldToSvPair = new HashMap<>();
        List<NamedExpression> newAggs = new ArrayList<>();
        List<Alias> newEvals = new ArrayList<>();
        int[] counter = { 0 };

        for (NamedExpression agg : aggregate.aggregates()) {
            Expression dataExpr = null, constant = null;
            Sum sum = null;
            if (agg instanceof Alias alias && alias.child() instanceof Sum s
                && s.filter() == null
                && s.field() instanceof Add add) {
                if (add.right().foldable() && add.left().foldable() == false) {
                    dataExpr = add.left();
                    constant = add.right();
                    sum = s;
                } else if (add.left().foldable() && add.right().foldable() == false) {
                    dataExpr = add.right();
                    constant = add.left();
                    sum = s;
                }
            }

            if (dataExpr != null) {
                final Expression de = dataExpr;
                final Sum fs = sum;
                SvPair pair = fieldToSvPair.computeIfAbsent(de.canonical(), k -> {
                    var sv = new MvSingleValueOrNull(source, de);
                    var svSumAlias = new Alias(
                        source,
                        TemporaryNameGenerator.temporaryName(sv, fs, counter[0]++),
                        new Sum(source, sv, null, null, fs.summationMode(), fs.longOverflowMode()),
                        null,
                        true
                    );
                    var svCountAlias = new Alias(
                        source,
                        TemporaryNameGenerator.temporaryName(sv, fs, counter[0]++),
                        new Count(source, sv, null, null),
                        null,
                        true
                    );
                    newAggs.add(svSumAlias);
                    newAggs.add(svCountAlias);
                    return new SvPair(svSumAlias.toAttribute(), svCountAlias.toAttribute());
                });

                var evalExpr = new Add(source, pair.sum(), new Mul(source, constant, pair.count()), ConfigurationAware.CONFIGURATION_MARKER);
                newEvals.add(((Alias) agg).replaceChild(evalExpr));
            } else {
                newAggs.add(agg);
            }
        }

        if (newEvals.isEmpty()) {
            return aggregate;
        }

        LogicalPlan plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
        plan = new Eval(source, plan, newEvals);
        plan = new Project(source, plan, Expressions.asAttributes(aggregate.aggregates()));
        return plan;
    }
}
