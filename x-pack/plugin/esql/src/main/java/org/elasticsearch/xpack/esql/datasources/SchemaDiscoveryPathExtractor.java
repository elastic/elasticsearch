/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Describe;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.HashSet;
import java.util.Set;

/**
 * Finds the external relations a query asks for a schema and no rows, the {@code FROM ds | LIMIT 0} and
 * {@code FROM ds | DESCRIBE} shapes.
 * <p>{@code SkipQueryOnLimitZero} already empties such a plan, but it is a logical-optimizer rule and external
 * resolution runs earlier, in {@code EsqlSession#preAnalyzeExternalSources}, so the listing is paid before the
 * plan is emptied. The parsed plan is available there, which is what makes the shape knowable in time.
 * <p>This says only that no rows will be read; how much of the glob a schema needs is the dataset's business,
 * and the resolver decides it.
 * <p>Unlike {@link ExternalStatsRequirementExtractor}, which unions, this subtracts: a spec read for its rows in
 * <em>any</em> branch must resolve as a reading query. Keyed by {@link ExternalSourceSpec}, as resolution is, so
 * a resource read two ways is two entries and one of them reading rows does not bind the other.
 * <p>{@code LIMIT 0 BY k} is a {@code LimitBy} and an unfolded zero is not a {@link Literal}; both are missed,
 * and both only leave work switched on.
 */
public final class SchemaDiscoveryPathExtractor {

    private SchemaDiscoveryPathExtractor() {}

    /**
     * Every {@link UnresolvedExternalRelation} whose occurrences all sit below a {@link Limit} of literal zero,
     * as the spec it denotes. Keyed by spec rather than by resource: two datasets over one resource are two reads,
     * and one of them reading no rows says nothing about the other.
     *
     * @param unresolvedPlan the root of the unresolved logical plan
     * @return the specs whose resolution needs no rows read
     */
    public static Set<ExternalSourceSpec> specsReadingNoRows(LogicalPlan unresolvedPlan) {
        Set<ExternalSourceSpec> underZeroLimit = new HashSet<>();
        Set<ExternalSourceSpec> read = new HashSet<>();
        collect(unresolvedPlan, false, false, underZeroLimit, read);
        underZeroLimit.removeAll(read);
        return underZeroLimit;
    }

    private static void collect(
        Node<?> node,
        boolean zeroLimitAbove,
        boolean describeAbove,
        Set<ExternalSourceSpec> underZeroLimit,
        Set<ExternalSourceSpec> read
    ) {
        if (node instanceof Limit limit && isLiteralZero(limit.limit())) {
            zeroLimitAbove = true;
        }
        // DESCRIBE replaces everything below it with a description of its columns during analysis, so nothing
        // under it runs - not even an aggregate, unlike LIMIT 0.
        if (node instanceof Describe) {
            describeAbove = true;
        }

        // An aggregate below the zero limit still consumes every row to produce the one the limit
        // discards, so the relation under it is read. `STATS COUNT(*) | LIMIT 0` is the shape:
        // the limit throws away the count, not the scan. INLINESTATS wraps its aggregate as a
        // child, so it consumes rows the same way.
        if (node instanceof Aggregate || node instanceof InlineStats) {
            zeroLimitAbove = false;
        }

        if (node instanceof UnresolvedExternalRelation relation) {
            ExternalSourceSpec spec = ExternalSourceSpec.ofOrNull(relation);
            if (spec != null) {
                (zeroLimitAbove || describeAbove ? underZeroLimit : read).add(spec);
            }
            return; // leaf: no children below a relation
        }

        for (Node<?> child : node.children()) {
            collect(child, zeroLimitAbove, describeAbove, underZeroLimit, read);
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

}
