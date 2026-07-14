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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Grafts the out-of-band request {@code filter} onto external-source (dataset) leaves of an analyzed plan.
 *
 * <p>For each {@link ExternalRelation}, the filter is translated against that relation's exposed schema (a present
 * field binds to its attribute, a missing field to {@link Literal#NULL}) and wrapped as an ordinary {@link Filter}
 * above the relation. From there the existing optimizer pushes it down and the engine evaluates it — the grafted
 * filter is indistinguishable from a user-written {@code WHERE}. Index leaves keep their existing path and are not
 * touched here.
 *
 * <p>A construct outside the supported subset is dropped per-clause with a superset substitution (partial application):
 * every supported clause of the filter still applies, and the unsupported clause is skipped in the direction that
 * <em>widens</em> the result — a relaxed restriction returns more rows, a relaxed exclusion returns previously-excluded
 * rows — so the source over-matches rather than silently dropping a row it should have returned. A response-header
 * warning names the source, the construct, and that direction. A filter with no supported clause folds to a no-op and
 * the relation is read unfiltered.
 *
 * <p>The graft is version-gated. The translated predicate can contain {@code mv_in_range}, which older nodes do not
 * have; the grafted {@link Filter} rides inside the fragment distributed to data nodes, so on a mixed-version cluster
 * an older node would fail to deserialize it. Below {@link #ESQL_REQUEST_FILTER_ON_DATASET} the rewrite is skipped
 * entirely — datasets are read unfiltered (the pre-feature behavior) with a warning — rather than shipping a plan a
 * peer cannot read. This mirrors how the analyzer and verifier gate version-sensitive rewrites on
 * {@code context.minimumVersion()}.
 */
public final class RequestFilterGraft {

    /**
     * The dataset-filter feature gate. The graft here and the {@code KQL()}-on-dataset analyzer rewrite both emit the
     * same mv-function vocabulary, so both version-gate on this one transport version — below it, neither applies.
     */
    public static final TransportVersion ESQL_REQUEST_FILTER_ON_DATASET = TransportVersion.fromName("esql_request_filter_on_dataset");

    private RequestFilterGraft() {}

    /**
     * @param nowInMillis    the query's start time, epoch millis — anchors {@code now} date math so a request filter
     *                       over an external source resolves {@code "now-15m"} to the same instant the index path would.
     * @param minimumVersion the minimum transport version across the nodes this plan targets; below
     *                       {@link #ESQL_REQUEST_FILTER_ON_DATASET} the graft is skipped (see the class javadoc).
     */
    public static LogicalPlan graft(LogicalPlan analyzed, QueryBuilder requestFilter, long nowInMillis, TransportVersion minimumVersion) {
        if (requestFilter == null) {
            return analyzed;
        }
        if (minimumVersion.supports(ESQL_REQUEST_FILTER_ON_DATASET) == false) {
            warnNotApplied(analyzed, "the cluster contains a node too old to evaluate the translated filter");
            return analyzed;
        }
        LogicalPlan grafted = analyzed.transformUp(ExternalRelation.class, relation -> {
            Map<String, Attribute> byName = new HashMap<>();
            for (Attribute a : relation.output()) {
                byName.put(a.name(), a);
            }
            QueryDslTranslator translator = new QueryDslTranslator(name -> {
                Attribute a = byName.get(name);
                return a != null ? a : Literal.NULL;
            }, byName.keySet(), nowInMillis, true);
            // Partial application: an unsupported clause is not fatal — the translator drops it with a superset
            // substitution (so the result over-matches, never silently dropping rows) and records it, while every
            // supported clause still applies. Each dropped clause is surfaced as a header warning naming its effect.
            Expression condition = translator.translate(requestFilter);
            for (QueryDslTranslator.DroppedClause clause : translator.droppedClauses()) {
                warnDropped(relation, clause);
            }
            // A wholly-unsupported filter folds to TRUE (a no-op); leave the source unfiltered rather than graft it.
            return condition == Literal.TRUE ? relation : new Filter(relation.source(), relation, condition);
        });
        // The grafted Filter and the rebuilt spine above it are fresh nodes at stage NEW; the plan was already
        // analyzed, so mark the (idempotent for unchanged nodes) tree analyzed to satisfy the pre-optimizer.
        grafted.forEachDown(LogicalPlan.class, LogicalPlan::setAnalyzed);
        return grafted;
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
            .map(RequestFilterGraft::name)
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
