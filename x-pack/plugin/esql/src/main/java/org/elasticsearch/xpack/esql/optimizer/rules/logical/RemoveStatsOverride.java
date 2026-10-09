/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

/**
 * Removes {@link Aggregate} overrides in grouping, aggregates and across them inside.
 * The overrides appear when the same alias is used multiple times in aggregations
 * and/or groupings:
 * {@code STATS x = COUNT(*), x = MIN(a) BY x = b + 1, x = c + 10}
 * becomes
 * {@code STATS BY x = c + 10}
 * and
 * {@code INLINE STATS x = COUNT(*), x = MIN(a) BY x = b + 1, x = c + 10}
 * becomes
 * {@code INLINE STATS BY x = c + 10}
 * This is "last one wins", with groups having priority over aggregates.
 * <p>
 * {@link TranslateTimeSeriesAggregate} reuses {@link #keepLastNamedExpression} on the PackDims
 * path so a {@code TS} aggregate alias that collides with a grouping key is dropped before the
 * rewrite emits {@code Project[[alias, grouping]]}.
 */
public final class RemoveStatsOverride extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        return aggregate.with(keepLastNamedExpression(aggregate.groupings()), keepLastNamedExpression(aggregate.aggregates()));
    }

    /**
     * Drops earlier expressions that share a name with a later one (last wins) and emits a shadow
     * {@link org.elasticsearch.common.logging.HeaderWarning}.
     */
    static <T extends Expression> List<T> keepLastNamedExpression(List<T> list) {
        return keepLastNamedExpression(list, warning -> addWarning("{}", warning));
    }

    /**
     * Same as {@link #keepLastNamedExpression(List)} but reports shadow messages via {@code warn}
     * (e.g. deferred analysis warnings in {@link TranslateTimeSeriesAggregate}).
     */
    static <T extends Expression> List<T> keepLastNamedExpression(List<T> list, Consumer<String> warn) {
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
                warn.accept(
                    Strings.format(
                        "Line %s:%s: Field '%s' shadowed by field at line %s:%s",
                        source.getLineNumber(),
                        source.getColumnNumber(),
                        name,
                        previousSource.getLineNumber(),
                        previousSource.getColumnNumber()
                    )
                );
                newList.remove(i);
            }
        }
        return newList.size() == list.size() ? list : newList;
    }
}
