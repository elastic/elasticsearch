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
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This rule handles duplicate aggregate functions to avoid duplicate compute
 * stats a = min(x), b = min(x), c = count(*), d = count() by g
 * becomes
 * stats a = min(x), c = count(*) by g | eval b = a, d = c | keep a, b, c, d, g
 */
public final class ReplaceDuplicatedAggs extends OptimizerRules.OptimizerRule<Aggregate> implements OptimizerRules.CoordinatorOnly {
    public ReplaceDuplicatedAggs() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        // an alias map for evaluatable grouping functions
        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
        aggregate.forEachExpressionUp(Alias.class, a -> {
            if (a.child() instanceof GroupingFunction.NonEvaluatableGroupingFunction == false) {
                aliasesBuilder.put(a.toAttribute(), a.child());
            }
        });
        var aliases = aliasesBuilder.build();

        // break down each aggregate into AggregateFunction and/or grouping key
        // preserve the projection at the end
        List<? extends NamedExpression> aggs = aggregate.aggregates();

        // root/naked aggs
        Map<AggregateFunction, Alias> rootAggs = Maps.newLinkedHashMapWithExpectedSize(aggs.size());
        List<NamedExpression> newProjections = new ArrayList<>();
        // track the aggregate aggs (including grouping which is not an AggregateFunction)
        List<NamedExpression> newAggs = new ArrayList<>();

        Holder<Boolean> changed = new Holder<>(false);

        for (NamedExpression agg : aggs) {
            if (agg instanceof Alias as) {
                // use intermediate variable to mark child as final for lambda use
                Expression child = as.child();

                // common case - handle duplicates
                if (child instanceof AggregateFunction af) {
                    // canonical representation, with resolved aliases
                    AggregateFunction canonical = (AggregateFunction) af.transformUp(e -> aliases.resolve(e, e));

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
            // preserve initial projection
            plan = new Project(source, plan, newProjections);
        }

        return plan;
    }
}
