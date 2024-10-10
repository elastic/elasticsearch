/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Replace nested expressions over aggregates with synthetic eval post the aggregation
 * stats a = sum(a) + min(b) by x
 * becomes
 * stats a1 = sum(a), a2 = min(b) by x | eval a = a1 + a2 | keep a, x
 * The rule also considers expressions applied over groups:
 * {@code STATS a = x + 1 BY x} becomes {@code STATS BY x | EVAL a = x + 1 | KEEP a, x}
 * And to combine the two:
 * stats a = x + count(*) by x
 * becomes
 * stats a1 = count(*) by x | eval a = x + a1 | keep a1, x
 * Since the logic is very similar, this rule also handles duplicate aggregate functions to avoid duplicate compute
 * stats a = min(x), b = min(x), c = count(*), d = count() by g
 * becomes
 * stats a = min(x), c = count(*) by g | eval b = a, d = c | keep a, b, c, d, g
 */
public final class ReplaceStatsAggExpressionWithEval extends OptimizerRules.OptimizerRule<Aggregate> {
    public ReplaceStatsAggExpressionWithEval() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        // build alias map
        AttributeMap<Expression> aliases = new AttributeMap<>();
        aggregate.forEachExpressionUp(Alias.class, a -> aliases.put(a.toAttribute(), a.child()));

        // break down each aggregate into AggregateFunction and/or grouping key
        // preserve the projection at the end
        List<? extends NamedExpression> aggs = aggregate.aggregates();

        // root/naked aggs
        Map<AggregateFunction, Alias> rootAggs = Maps.newLinkedHashMapWithExpectedSize(aggs.size());
        // evals (original expression relying on multiple aggs)
        List<Alias> newEvals = new ArrayList<>();
        List<NamedExpression> newProjections = new ArrayList<>();
        // track the aggregate aggs (including grouping which is not an AggregateFunction)
        List<NamedExpression> newAggs = new ArrayList<>();

        Holder<Boolean> changed = new Holder<>(false);
        int[] counter = new int[] { 0 };

        for (NamedExpression agg : aggs) {
            if (agg instanceof Alias as) {
                // use intermediate variable to mark child as final for lambda use
                Expression child = as.child();

                // common case - handle duplicates
                if (child instanceof AggregateFunction af) {
                    // canonical representation, with resolved aliases
                    AggregateFunction canonical = (AggregateFunction) af.canonical().transformUp(e -> aliases.resolve(e, e));

                    Alias found = rootAggs.get(canonical);
                    // aggregate is new
                    if (found == null) {
                        rootAggs.put(canonical, as);
                        newAggs.add(as);
                        newProjections.add(as.toAttribute());
                    }
                    // agg already exists - preserve the current alias but point it to the existing agg
                    // thus don't add it to the list of aggs as we don't want duplicated compute
                    else {
                        changed.set(true);
                        newProjections.add(as.replaceChild(found.toAttribute()));
                    }
                }
                // nested expression over aggregate function or groups
                // replace them with reference and move the expression into a follow-up eval
                else {
                    changed.set(true);
                    Expression aggExpression = child.transformUp(AggregateFunction.class, af -> {
                        AggregateFunction canonical = (AggregateFunction) af.canonical();
                        Alias alias = rootAggs.get(canonical);
                        if (alias == null) {
                            // create synthetic alias ove the found agg function
                            alias = new Alias(af.source(), syntheticName(canonical, child, counter[0]++), canonical, null, true);
                            // and remember it to remove duplicates
                            rootAggs.put(canonical, alias);
                            // add it to the list of aggregates and continue
                            newAggs.add(alias);
                        }
                        // (even when found) return a reference to it
                        return alias.toAttribute();
                    });

                    Alias alias = as.replaceChild(aggExpression);
                    newEvals.add(alias);
                    newProjections.add(alias.toAttribute());
                }
            }
            // not an alias (e.g. grouping field)
            else {
                newAggs.add(agg);
                newProjections.add(agg.toAttribute());
            }
        }

        LogicalPlan plan = aggregate;
        if (changed.get()) {
            Source source = aggregate.source();
            plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
            if (newEvals.size() > 0) {
                plan = new Eval(source, plan, newEvals);
            }
            // preserve initial projection
            plan = new Project(source, plan, newProjections);
        }

        return plan;
    }

    static String syntheticName(Expression expression, Expression af, int counter) {
        return TemporaryNameUtils.temporaryName(expression, af, counter);
    }
}
