/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

/**
 * Removes {@link Aggregate} overrides in grouping, aggregates and across them inside.
 * The overrides appear when the same alias is used multiple times in aggregations
 * and/or groupings:
 * {@code STATS x = COUNT(*), x = MIN(a) BY x = b + 1, x = c + 10}
 * becomes
 * {@code STATS BY x = c + 10}
 * and
 * {@code INLINESTATS x = COUNT(*), x = MIN(a) BY x = b + 1, x = c + 10}
 * becomes
 * {@code INLINESTATS BY x = c + 10}
 * This is "last one wins", with groups having priority over aggregates.
 * Separately, it replaces expressions used as group keys inside the aggregates with references:
 * {@code STATS max(a + b + 1) BY a + b}
 * becomes
 * {@code STATS max($x + 1) BY $x = a + b}
 */
public final class RemoveStatsOverride extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        return aggregate.with(removeDuplicateNames(aggregate.groupings()), removeDuplicateNames(aggregate.aggregates()));
    }

    private static <T extends Expression> List<T> removeDuplicateNames(List<T> list) {
        var newList = new ArrayList<>(list);
        var expressionsByName = Maps.<String, T>newMapWithExpectedSize(list.size());

        // remove duplicates
        for (int i = list.size() - 1; i >= 0; i--) {
            var element = list.get(i);
            var name = Expressions.name(element);
            var previousExpression = expressionsByName.putIfAbsent(name, element);
            if (previousExpression != null) {
                var source = element.source().source();
                var previousSource = previousExpression.source().source();
                addWarning(
                    "Line {}:{}: Field '{}' shadowed by field at line {}:{}",
                    source.getLineNumber(),
                    source.getColumnNumber(),
                    name,
                    previousSource.getLineNumber(),
                    previousSource.getColumnNumber()
                );
                newList.remove(i);
            }
        }
        return newList.size() == list.size() ? list : newList;
    }
}
