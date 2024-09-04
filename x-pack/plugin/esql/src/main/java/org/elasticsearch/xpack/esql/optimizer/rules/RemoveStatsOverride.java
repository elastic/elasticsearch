/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Stats;

import java.util.ArrayList;
import java.util.List;

/**
 * Removes {@link Stats} overrides in grouping, aggregates and across them inside.
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
public final class RemoveStatsOverride extends AnalyzerRules.AnalyzerRule<LogicalPlan> {

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(LogicalPlan p) {
        if (p.resolved() == false) {
            return p;
        }
        if (p instanceof Stats stats) {
            return (LogicalPlan) stats.with(
                stats.child(),
                removeDuplicateNames(stats.groupings()),
                removeDuplicateNames(stats.aggregates())
            );
        }
        return p;
    }

    private static <T extends Expression> List<T> removeDuplicateNames(List<T> list) {
        var newList = new ArrayList<>(list);
        var nameSet = Sets.newHashSetWithExpectedSize(list.size());

        // remove duplicates
        for (int i = list.size() - 1; i >= 0; i--) {
            var element = list.get(i);
            var name = Expressions.name(element);
            if (nameSet.add(name) == false) {
                newList.remove(i);
            }
        }
        return newList.size() == list.size() ? list : newList;
    }
}
