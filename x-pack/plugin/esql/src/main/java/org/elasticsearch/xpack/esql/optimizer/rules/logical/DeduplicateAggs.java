/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;

import java.util.List;
import java.util.Map;

/**
 * This rule handles duplicate aggregate functions to avoid duplicate compute
 * stats a = min(x), b = min(x), c = count(*), d = count() by g
 * becomes
 * stats a = min(x), c = count(*) by g | eval b = a, d = c | keep a, b, c, d, g
 */
public final class DeduplicateAggs extends AbstractAggregateDeduplicator implements OptimizerRules.CoordinatorOnly {
    @Override
    protected AttributeMap<Expression> buildAliases(
        Aggregate aggregate,
        Map<GroupingFunction.NonEvaluatableGroupingFunction, Attribute> nonEvalGroupingAttributes
    ) {
        AttributeMap.Builder<Expression> builder = AttributeMap.builder();
        aggregate.forEachExpressionUp(Alias.class, a -> {
            if (a.child() instanceof GroupingFunction.NonEvaluatableGroupingFunction == false) {
                builder.put(a.toAttribute(), a.child());
            }
        });
        return builder.build();
    }

    @Override
    protected void processAlias(
        Alias as,
        AttributeMap<Expression> aliases,
        Map<AggregateFunction, Alias> rootAggs,
        List<NamedExpression> newAggs,
        List<NamedExpression> newProjections,
        List<Alias> newEvals,
        Holder<Boolean> changed,
        int[] counter,
        Map<GroupingFunction.NonEvaluatableGroupingFunction, Attribute> nonEvalGroupingAttributes
    ) {
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
}
