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
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.HashSet;
import java.util.Set;

/**
 * Detects which external relations are read by a query shape that needs <em>eager</em> global
 * statistics aggregated across <em>all</em> files of a multi-file glob during planning.
 *
 * <p>Eager global stats exist for exactly one optimization: the metadata-only fast path for an
 * <b>ungrouped</b> aggregate ({@code COUNT}/{@code MIN}/{@code MAX} with no {@code BY}) directly over
 * an external relation (see {@code ComputeService#canSkipSplitDiscovery}). Every other consumer
 * (filter/aggregate pushdown, split-filter classification) runs after split discovery and prefers
 * per-split stats over the global {@code sourceMetadata}. Therefore grouped {@code STATS ... BY} and
 * {@code INLINESTATS} never benefit from the eager global aggregation and must not pay its
 * per-file footer-read cost.
 *
 * <p>The result of {@link #pathsRequiringEagerStats(LogicalPlan)} is used by
 * {@code ExternalSourceResolver} to gate the FIRST_FILE_WINS eager all-file stats aggregation: a
 * path absent from the set defers the N footer reads (keeping {@code STATS_FILE_COUNT}, marking
 * stats partial), so wide globs no longer block planning on thousands of object-store GETs for
 * queries that do not consume the global stats (e.g. {@code LIMIT}, {@code SELECT *}, grouped
 * {@code STATS ... BY}, {@code INLINESTATS}).
 *
 * <h2>Deliberate safety bias</h2>
 * An ungrouped aggregate is matched <em>anywhere above</em> the relation, not only as its direct
 * parent. A false <em>negative</em> would turn a metadata-only {@code COUNT(*)} over a huge glob
 * into a full N-file scan (a severe regression); a false <em>positive</em> (e.g.
 * {@code ... | WHERE x > 5 | STATS COUNT(*)}, which usually will not actually skip split discovery)
 * only costs the over-read we already tolerate today. We bias toward eager for ungrouped aggregates.
 *
 * <h2>Per-path conservatism for mixed branches</h2>
 * The result is a {@link Set}: if the same path appears under an ungrouped aggregate in one branch
 * (e.g. a {@code FORK} arm) and under {@code LIMIT} in another, the union marks it as requiring eager
 * stats. The single resolution of that path stays eager, which is correct because one resolution
 * feeds both branches.
 */
public final class ExternalStatsRequirementExtractor {

    private ExternalStatsRequirementExtractor() {}

    /**
     * Returns the literal {@code tablePath} of every {@link UnresolvedExternalRelation} that has an
     * <b>ungrouped</b> {@link Aggregate} ancestor. The path-key derivation is identical to
     * {@code PreAnalyzer} and {@code EsqlSession#extractExternalConfigs}
     * ({@code BytesRefs.toString(literal.value())}), so the keys match the resolver's
     * {@code icebergPaths} by construction.
     *
     * @param unresolvedPlan the root of the unresolved logical plan
     * @return the set of literal path strings whose resolution must eagerly aggregate global stats
     */
    public static Set<String> pathsRequiringEagerStats(LogicalPlan unresolvedPlan) {
        Set<String> result = new HashSet<>();
        collect(unresolvedPlan, false, result);
        return result;
    }

    private static void collect(Node<?> node, boolean ungroupedAggAbove, Set<String> result) {
        // INLINESTATS wraps its embedded Aggregate as its child (UnaryPlan child). That embedded
        // aggregate produces an InlineJoin, not the ungrouped-aggregate metadata fast path, so it
        // must not flip the flag — skip the Aggregate node and walk below it with the flag unchanged.
        if (node instanceof InlineStats inlineStats) {
            collect(inlineStats.aggregate().child(), ungroupedAggAbove, result);
            return;
        }

        if (node instanceof Aggregate aggregate && aggregate.groupings().isEmpty()) {
            // Covers Aggregate and its subclass TimeSeriesAggregate; a grouped/TS aggregate has
            // non-empty groupings, so it does not set the flag.
            ungroupedAggAbove = true;
        }

        if (node instanceof UnresolvedExternalRelation relation) {
            if (ungroupedAggAbove) {
                String path = extractPath(relation);
                if (path != null) {
                    result.add(path);
                }
            }
            return; // leaf: no children below a relation
        }

        for (Node<?> child : node.children()) {
            collect(child, ungroupedAggAbove, result);
        }
    }

    /**
     * Path-key derivation kept in lockstep with {@code PreAnalyzer#icebergPaths} and
     * {@code EsqlSession#extractExternalConfigs}: a non-null {@link Literal} {@code tablePath}
     * rendered via {@code BytesRefs.toString}. Returns {@code null} for a non-literal {@code tablePath}
     * so detection never throws; that path is then absent from the set and, since the resolver
     * receives a non-null set, would <em>defer</em> rather than aggregate eagerly. In practice this
     * branch is unreachable: {@code EsqlSession#extractExternalConfigs} runs first on the same plan
     * and throws on a non-literal {@code tablePath}, so resolution never reaches a path this method
     * could fail to key.
     */
    private static String extractPath(UnresolvedExternalRelation relation) {
        Expression tablePath = relation.tablePath();
        if (tablePath instanceof Literal literal && literal.value() != null) {
            return BytesRefs.toString(literal.value());
        }
        return null;
    }
}
