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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rewrites {@code SUM(field ± c)} into {@code SUM(SINGLE_VALUE_OR_NULL(field)) ± c * COUNT(SINGLE_VALUE_OR_NULL(field))},
 * decomposing the per-row arithmetic into separate aggregations on the raw field:
 * <pre>
 *     STATS s1 = SUM(x + 1), s2 = SUM(x - 2) BY g
 *     →
 *     STATS _sv_sum = SUM(SINGLE_VALUE_OR_NULL(x)), _sv_count = COUNT(SINGLE_VALUE_OR_NULL(x)) BY g
 *     | EVAL s1 = _sv_sum + 1 * _sv_count, s2 = _sv_sum - 2 * _sv_count
 *     | PROJECT s1, s2, g
 * </pre>
 *
 * <p>This decomposition has two benefits: multiple {@code SUM(field ± c_i)} over the same field
 * share a single pair of aggregates, and the SUM/COUNT now operate directly on the field rather
 * than a computed expression, enabling downstream pushdowns (e.g. COUNT via {@code Weight.count()},
 * or future SUM pushdowns using doc-value metadata).</p>
 *
 * <p>Filtered aggregates ({@code SUM(x + c) WHERE filter}) are skipped since sums with different
 * filters cannot share the same base aggregation.</p>
 *
 * <p>This rule must run before {@link ReplaceAggregateNestedExpressionWithEval}, which would
 * extract {@code field ± c} into a pre-agg EVAL, hiding the pattern.</p>
 */
public class RewriteSumFieldPlusConstant extends OptimizerRules.ParameterizedOptimizerRule<Aggregate, LogicalOptimizerContext> {

    public RewriteSumFieldPlusConstant() {
        super(OptimizerRules.TransformDirection.UP);
    }

    private record SvPair(Attribute sum, Attribute count) {}

    @Override
    protected LogicalPlan rule(Aggregate aggregate, LogicalOptimizerContext context) {
        var source = aggregate.source();

        Map<Expression, SvPair> fieldToSvPair = new HashMap<>();
        List<NamedExpression> newAggs = new ArrayList<>();
        List<Alias> newEvals = new ArrayList<>();
        int[] counter = { 0 };

        for (NamedExpression agg : aggregate.aggregates()) {
            Expression dataExpr = null, constant = null;
            Sum sum = null;
            boolean isSubtraction = false;

            if (agg instanceof Alias alias && alias.child() instanceof Sum s && s.hasFilter() == false) {
                if (s.field() instanceof Add add) {
                    if (add.right().foldable() && add.left().foldable() == false) {
                        dataExpr = add.left();
                        constant = add.right();
                        sum = s;
                    } else if (add.left().foldable() && add.right().foldable() == false) {
                        dataExpr = add.right();
                        constant = add.left();
                        sum = s;
                    }
                } else if (s.field() instanceof Sub sub && sub.right().foldable() && sub.left().foldable() == false) {
                    dataExpr = sub.left();
                    constant = sub.right();
                    sum = s;
                    isSubtraction = true;
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
                        new Sum(source, sv, Literal.TRUE, AggregateFunction.NO_WINDOW, fs.summationMode(), fs.longOverflowMode()),
                        null,
                        true
                    );
                    var svCountAlias = new Alias(
                        source,
                        TemporaryNameGenerator.temporaryName(sv, fs, counter[0]++),
                        new Count(source, sv),
                        null,
                        true
                    );
                    newAggs.add(svSumAlias);
                    newAggs.add(svCountAlias);
                    return new SvPair(svSumAlias.toAttribute(), svCountAlias.toAttribute());
                });

                var mulExpr = new Mul(source, constant, pair.count());
                Expression evalExpr;
                if (isSubtraction) {
                    evalExpr = new Sub(source, pair.sum(), mulExpr, context.configuration());
                } else {
                    evalExpr = new Add(source, pair.sum(), mulExpr, context.configuration());
                }
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
