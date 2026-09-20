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
 * Detects which external relations a query asks for a schema and no rows — the shape a UI issues
 * against a dataset before anything else, {@code FROM ds | LIMIT 0}.
 * <p>Such a query reads nothing: {@code SkipQueryOnLimitZero} replaces the plan with an empty
 * relation and split discovery never runs. But that rule is a <em>logical optimizer</em> rule, and
 * external resolution happens earlier, in {@code EsqlSession#preAnalyzeExternalSources} during
 * pre-analysis, so by the time the plan is emptied the listing has already been paid in full. The
 * parsed plan is available at that point, which is what makes the shape knowable early enough to
 * act on.
 * <p>The result of {@link #pathsReadingNoRows(LogicalPlan)} lets resolution list only what the
 * schema requires rather than what a scan would require. What each mode requires differs — a
 * declared mapping needs no file, {@code first_file_wins} needs one, {@code union_by_name} and
 * {@code strict} need every file by contract — so this set says only that no rows will be read,
 * and the resolver decides what follows from that.
 * <h2>Conservatism runs the opposite way to the stats extractor</h2>
 * {@link ExternalStatsRequirementExtractor} unions: a path under an ungrouped aggregate in any
 * branch must be resolved eagerly, because one resolution feeds every branch. Here the safe
 * direction is the reverse. One resolution feeds every branch, so a path read for its rows in
 * <em>any</em> branch — a {@code FORK} whose other arm has a real limit, a relation named twice in
 * one query — must be resolved as a reading query. A path therefore qualifies only when every one
 * of its occurrences sits under a zero limit, which is why this subtracts rather than unions.
 * <p>Both known gaps are conservative, and both leave work switched on that could have been
 * skipped: {@code LIMIT 0 BY k} is a {@code LimitBy}, not a {@code Limit}, and a limit that folds
 * to zero without being a {@link Literal} is not recognised. Neither can produce a wrong answer.
 */
public final class SchemaOnlyPathExtractor {

    private SchemaOnlyPathExtractor() {}

    /**
     * Returns the literal {@code tablePath} of every {@link UnresolvedExternalRelation} whose rows
     * are all discarded — every occurrence sits below a {@link Limit} of literal zero. The path-key
     * derivation matches {@code PreAnalyzer} and {@code EsqlSession#extractExternalConfigs}
     * ({@code BytesRefs.toString(literal.value())}), so the keys line up with the resolver's paths
     * by construction.
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
     * Whether a limit is the literal zero. Deliberately narrower than {@code SkipQueryOnLimitZero},
     * which folds the expression: an unfoldable-at-parse-time zero is missed here and the query
     * resolves as a reading one, which is slower and not wrong. Folding during pre-analysis would
     * mean evaluating expressions before the plan is analysed.
     */
    private static boolean isLiteralZero(Expression limit) {
        return limit instanceof Literal literal && literal.value() instanceof Number number && number.longValue() == 0L;
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
