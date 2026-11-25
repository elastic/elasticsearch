/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Adds to a {@link UnionAll} the columns its branches gain after resolution.
 * <p>
 * The analyzer determines a union's columns while resolving it. A time-series aggregation over a bare time-series function, such as
 * {@code TS k8s | STATS rate(x) BY tbucket}, only gets its implicit {@code _timeseries} column later, from
 * {@link ResolveImplicitTimeSeriesIdentityGrouping} and {@link TranslateTimeSeriesAggregate}. Without this rule a union over such a
 * branch drops that column, although the same query run on its own returns it. That union can be a subquery next to an index, or a
 * view kept as its own branch, as happens when the caller sets {@code unmapped_fields}.
 * <p>
 * Columns the union already has keep their ids and their order. Each new column follows the column that precedes it in the
 * branches, so a union with one branch returns that branch's columns in the order the query without the union would.
 */
public class AddLateUnionAllColumns extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(UnionAll.class, AddLateUnionAllColumns::addLateColumns);
    }

    private static LogicalPlan addLateColumns(UnionAll union) {
        List<Attribute> previous = union.output();
        if (previous.isEmpty() || union.childrenResolved() == false) {
            return union;
        }
        List<Attribute> refreshed = union.refreshOutput().output();
        Set<String> previousNames = new HashSet<>(previous.size());
        for (Attribute attr : previous) {
            previousNames.add(attr.name());
        }
        Map<String, Attribute> added = new LinkedHashMap<>();
        for (Attribute attr : refreshed) {
            if (previousNames.contains(attr.name()) == false) {
                added.put(attr.name(), attr);
            }
        }
        if (added.isEmpty()) {
            return union;
        }

        // Attach each new column to the column that precedes it in the branches; new columns that lead the branches go first.
        List<Attribute> leading = new ArrayList<>();
        Map<String, List<Attribute>> following = new HashMap<>();
        String preceding = null;
        for (Attribute attr : MergePlan.outputUnion(union.children())) {
            String name = attr.name();
            Attribute newColumn = added.get(name);
            if (newColumn != null) {
                (preceding == null ? leading : following.computeIfAbsent(preceding, k -> new ArrayList<>())).add(newColumn);
                preceding = name;
            } else if (previousNames.contains(name)) {
                preceding = name;
            }
        }

        List<Attribute> output = new ArrayList<>(refreshed.size());
        for (Attribute attr : leading) {
            addWithFollowing(attr, following, output);
        }
        for (Attribute attr : refreshed) {
            if (previousNames.contains(attr.name())) {
                addWithFollowing(attr, following, output);
            }
        }
        return union.replaceSubPlansAndOutput(union.children(), output);
    }

    private static void addWithFollowing(Attribute attr, Map<String, List<Attribute>> following, List<Attribute> output) {
        output.add(attr);
        for (Attribute next : following.getOrDefault(attr.name(), List.of())) {
            addWithFollowing(next, following, output);
        }
    }
}
