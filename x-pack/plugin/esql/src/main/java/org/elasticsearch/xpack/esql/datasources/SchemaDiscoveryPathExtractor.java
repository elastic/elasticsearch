/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.HashSet;
import java.util.Set;

/**
 * Finds the external relations a query asks for a schema and no rows, the {@code FROM ds | LIMIT 0} shape.
 * <p>{@code SkipQueryOnLimitZero} already empties such a plan, but it is a logical-optimizer rule and external
 * resolution runs earlier, in {@code EsqlSession#preAnalyzeExternalSources}, so the listing is paid before the
 * plan is emptied. The parsed plan is available there, which is what makes the shape knowable in time.
 * <p>This says only that no rows will be read; how much of the glob a schema needs is the dataset's business,
 * and the resolver decides it.
 * <p>Unlike {@link ExternalStatsRequirementExtractor}, which unions, this subtracts: one resolution feeds every
 * branch, so a path read for its rows in <em>any</em> branch must resolve as a reading query.
 * <p>{@code LIMIT 0 BY k} is a {@code LimitBy} and an unfolded zero is not a {@link Literal}; both are missed,
 * and both only leave work switched on.
 */
public final class SchemaDiscoveryPathExtractor {

    private SchemaDiscoveryPathExtractor() {}

    /**
     * Returns the literal {@code tablePath} of every {@link UnresolvedExternalRelation} whose occurrences all
     * sit below a {@link Limit} of literal zero. Path keys are derived as {@code PreAnalyzer} and
     * {@code EsqlSession#extractExternalConfigs} derive them, so they line up with the resolver's paths.
     *
     * @param unresolvedPlan the root of the unresolved logical plan
     * @return the set of literal path strings whose resolution needs no rows read
     */
    public static Set<String> pathsReadingNoRows(LogicalPlan unresolvedPlan) {
        Set<String> underZeroLimit = new HashSet<>();
        Set<String> read = new HashSet<>();
        collect(unresolvedPlan, false, underZeroLimit, read);
        underZeroLimit.removeAll(read);
        return underZeroLimit;
    }

    private static void collect(Node<?> node, boolean zeroLimitAbove, Set<String> underZeroLimit, Set<String> read) {
        if (node instanceof Limit limit && isLiteralZero(limit.limit())) {
            zeroLimitAbove = true;
        }

        // An aggregate below the zero limit still consumes every row to produce the one the limit
        // discards, so the relation under it is read. `STATS COUNT(*) | LIMIT 0` is the shape:
        // the limit throws away the count, not the scan. INLINESTATS wraps its aggregate as a
        // child, so it consumes rows the same way.
        if (node instanceof Aggregate || node instanceof InlineStats) {
            zeroLimitAbove = false;
        }

        if (node instanceof UnresolvedExternalRelation relation) {
            String path = extractPath(relation);
            if (path != null) {
                (zeroLimitAbove ? underZeroLimit : read).add(path);
            }
            return; // leaf: no children below a relation
        }

        for (Node<?> child : node.children()) {
            collect(child, zeroLimitAbove, underZeroLimit, read);
        }
    }

    /**
     * Tested exactly as {@code SkipQueryOnLimitZero} tests it: that rule compares against
     * {@code Integer.valueOf(0)} and folds the expression, this cannot fold during pre-analysis. Both
     * differences must only turn paths away — a path marked schema discovery whose plan then survives to execution
     * would read its rows from a bounded listing.
     */
    private static boolean isLiteralZero(Expression limit) {
        return limit instanceof Literal literal && Integer.valueOf(0).equals(literal.value());
    }

    /**
     * Path-key derivation kept in lockstep with {@code PreAnalyzer#icebergPaths} and
     * {@code EsqlSession#extractExternalConfigs}. Returns {@code null} for a non-literal
     * {@code tablePath} so detection never throws; that path is then absent from the set and
     * resolves as a reading query.
     */
    private static String extractPath(UnresolvedExternalRelation relation) {
        Expression tablePath = relation.tablePath();
        if (tablePath instanceof Literal literal && literal.value() != null) {
            return BytesRefs.toString(literal.value());
        }
        return null;
    }
}
