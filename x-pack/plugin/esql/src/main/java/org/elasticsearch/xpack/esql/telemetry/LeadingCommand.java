/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowInfo;

import java.util.Locale;

/**
 * The first source command of a query, the one the user typed first: the answer to "is this a PromQL or an ES|QL query".
 * <p>
 * It is read from the parsed plan, before {@code LET} bindings, views and {@code EXPLAIN} are resolved, so it is the command in
 * the query text: {@code FROM my_view} is {@link #FROM} whatever the view's body starts with, a {@code SET} or {@code LET} prefix
 * does not count, {@code FORK} and {@code LOOKUP JOIN} count under the command that starts the main query, and a {@code FROM}
 * with subqueries is {@link #FROM}. One exception: the parser drops a {@code FROM} whose only source is a single subquery, so
 * {@code FROM (ROW a = 1)} is {@link #ROW}. A query that fails before it is parsed is {@link #UNKNOWN}.
 * <p>
 * It is counted when the query finishes, because it is only known once the query is parsed, while the client and entry point
 * counters count a query when it starts. So the leading-command totals trail {@code queries._all.total} by the queries still
 * running.
 */
public enum LeadingCommand {
    FROM,
    TS,
    PROMQL,
    ROW,
    SHOW,
    EXPLAIN,
    EXTERNAL,
    UNKNOWN;

    /**
     * The leading command of {@code parsed}, found by following the first child down to the source command.
     */
    public static LeadingCommand of(@Nullable LogicalPlan parsed) {
        LogicalPlan plan = parsed;
        while (plan != null) {
            if (plan instanceof Explain) {
                return EXPLAIN;
            }
            if (plan instanceof PromqlCommand) {
                return PROMQL;
            }
            if (plan instanceof Row) {
                return ROW;
            }
            if (plan instanceof ShowInfo) {
                return SHOW;
            }
            if (plan instanceof UnresolvedExternalRelation) {
                return EXTERNAL;
            }
            if (plan instanceof UnresolvedRelation relation) {
                return ofSourceCommand(relation.telemetryLabel());
            }
            // in a parsed plan, these only come from a FROM with subqueries
            if (plan instanceof UnionAll || plan instanceof Subquery) {
                return FROM;
            }
            plan = plan.children().isEmpty() ? null : plan.children().getFirst();
        }
        return UNKNOWN;
    }

    private static LeadingCommand ofSourceCommand(@Nullable String commandName) {
        if (commandName == null || SourceCommand.isSourceCommand(commandName) == false) {
            return UNKNOWN;
        }
        return switch (SourceCommand.valueOf(commandName.toUpperCase(Locale.ROOT))) {
            case FROM -> FROM;
            case TS -> TS;
            case PROMQL -> PROMQL;
        };
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
