/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Applies the out-of-band request {@code filter} to external-source (dataset) leaves of an analyzed plan.
 *
 * <p>This is the request-filter <em>policy</em> over the source-agnostic {@link FilterRewriter} mechanism: it targets
 * {@link ExternalRelation} leaves, version-gates the rewrite (below), and turns each clause the translator could not
 * apply into a response-header warning. {@code FilterRewriter} does the actual work — translating the DSL against each
 * targeted node's schema and wrapping it as an ordinary {@code Filter}, so from there the existing optimizer pushes it
 * down and the engine evaluates it, indistinguishable from a user-written {@code WHERE}. Extending the request filter
 * to other source boundaries (a view, say) is a change of the target predicate here, not of the mechanism. Index leaves
 * keep their existing (pre-analysis) request-filter path and are not touched.
 *
 * <p>A construct outside the supported subset is dropped per-clause with a superset substitution (partial application):
 * every supported clause of the filter still applies, and the unsupported clause is skipped in the direction that
 * <em>widens</em> the result — a relaxed restriction returns more rows, a relaxed exclusion returns previously-excluded
 * rows — so the source over-matches rather than silently dropping a row it should have returned. A response-header
 * warning names the source, the construct, and that direction. A filter with no supported clause folds to a no-op and
 * the relation is read unfiltered.
 *
 * <p>The rewrite is version-gated. The translated predicate can contain {@code mv_in_range}, which older nodes do not
 * have; the inserted {@code Filter} rides inside the fragment distributed to data nodes, so on a mixed-version cluster
 * an older node would fail to deserialize it. Below {@link #ESQL_REQUEST_FILTER_ON_DATASET} the rewrite is skipped
 * entirely — datasets are read unfiltered (the pre-feature behavior) with a warning — rather than shipping a plan a
 * peer cannot read. This mirrors how the analyzer and verifier gate version-sensitive rewrites on
 * {@code context.minimumVersion()}.
 */
public final class RequestFilterRewriter {

    static final TransportVersion ESQL_REQUEST_FILTER_ON_DATASET = TransportVersion.fromName("esql_request_filter_on_dataset");

    private RequestFilterRewriter() {}

    /**
     * @param nowInMillis    the query's start time, epoch millis — anchors {@code now} date math so a request filter
     *                       over an external source resolves {@code "now-15m"} to the same instant the index path would.
     * @param minimumVersion the minimum transport version across the nodes this plan targets; below
     *                       {@link #ESQL_REQUEST_FILTER_ON_DATASET} the rewrite is skipped (see the class javadoc).
     */
    public static LogicalPlan rewrite(LogicalPlan analyzed, QueryBuilder requestFilter, long nowInMillis, TransportVersion minimumVersion) {
        if (requestFilter == null) {
            return analyzed;
        }
        if (minimumVersion.supports(ESQL_REQUEST_FILTER_ON_DATASET) == false) {
            warnNotApplied(analyzed, "the cluster contains a node too old to evaluate the translated filter");
            return analyzed;
        }
        // Target the dataset source relations; index leaves keep their existing (pre-analysis) request-filter path.
        return FilterRewriter.rewrite(
            analyzed,
            ExternalRelation.class::isInstance,
            requestFilter,
            nowInMillis,
            (node, clause) -> warnDropped((ExternalRelation) node, clause)
        );
    }

    /**
     * Warns that one clause of the request filter could not be applied to a dataset and was skipped, naming the
     * construct and the direction of the resulting over-match: a relaxed restriction returns more rows, a relaxed
     * exclusion returns previously-excluded rows. The supported clauses of the same filter still applied.
     */
    private static void warnDropped(ExternalRelation relation, QueryDslTranslator.DroppedClause clause) {
        HeaderWarning.addWarning(
            "The request filter on external dataset [{}] could not apply [{}]; it was skipped, so {} may be returned",
            name(relation),
            clause.construct(),
            clause.positive() ? "more rows" : "previously-excluded rows"
        );
    }

    /** Warns that the filter was not applied to the plan's dataset leaves, naming them, when there are any. */
    private static void warnNotApplied(LogicalPlan plan, String reason) {
        List<String> datasets = plan.collect(ExternalRelation.class::isInstance)
            .stream()
            .map(ExternalRelation.class::cast)
            .map(RequestFilterRewriter::name)
            .distinct()
            .toList();
        if (datasets.isEmpty() == false) {
            HeaderWarning.addWarning(
                "The request filter was not applied to external dataset(s) [{}] because {}; they were read unfiltered",
                String.join(", ", datasets),
                reason
            );
        }
    }

    private static String name(ExternalRelation relation) {
        return relation.datasetName() != null ? relation.datasetName() : relation.sourcePath();
    }
}
