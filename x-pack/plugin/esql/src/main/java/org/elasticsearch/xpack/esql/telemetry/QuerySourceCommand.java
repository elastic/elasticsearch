/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedEqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;

import java.util.EnumSet;
import java.util.Set;

/**
 * What a query reads, as a small closed set of values for metric attributes: the kind of every data source of the query, or
 * {@link #MIXED} if it reads sources of different kinds. Unlike the command counters, this is about the data sources of the whole
 * query, wherever they are in it: in a subquery, in the body of a view, or behind a dataset.
 */
public enum QuerySourceCommand {
    FROM,
    TS,
    PROMQL,
    /** External sources, read with {@code EXTERNAL} or with {@code FROM} on a dataset. */
    EXTERNAL,
    /** Sources of more than one kind, such as a {@code TS} subquery in a {@code FROM}. */
    MIXED,
    /** No data source, such as {@code ROW} or {@code SHOW}, or one of no other kind, such as {@code EQL}. */
    OTHER,
    /** The query failed before its sources were known, for instance because it did not parse. */
    UNKNOWN;

    /**
     * Classifies a plan whose views and datasets are resolved but which is not analyzed yet. Analysis translates {@code PROMQL} into
     * the same plan as {@code TS}, while before dataset resolution a dataset still looks like an index read by {@code FROM}.
     */
    public static QuerySourceCommand of(LogicalPlan plan) {
        Set<QuerySourceCommand> sources = EnumSet.noneOf(QuerySourceCommand.class);
        collect(plan, sources);
        if (sources.isEmpty()) {
            return OTHER;
        }
        return sources.size() == 1 ? sources.iterator().next() : MIXED;
    }

    private static void collect(LogicalPlan plan, Set<QuerySourceCommand> sources) {
        switch (plan) {
            case UnresolvedRelation relation -> {
                // A relation without a source command, such as a LOOKUP JOIN index, enriches the query rather than being read by it.
                if (relation.sourceCommand() != null) {
                    sources.add(of(relation.sourceCommand()));
                }
            }
            case UnresolvedExternalRelation relation -> sources.add(EXTERNAL);
            case UnresolvedEqlRelation relation -> sources.add(OTHER);
            // The subquery of `x IN (subquery)` filters the query, like a lookup, so only the side it filters is a source.
            case AbstractSubqueryJoin join -> collect(join.left(), sources);
            default -> plan.children().forEach(child -> collect(child, sources));
        }
    }

    private static QuerySourceCommand of(SourceCommand command) {
        return switch (command) {
            case FROM -> FROM;
            case TS -> TS;
            case PROMQL -> PROMQL;
        };
    }
}
