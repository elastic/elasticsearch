/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

/**
 * Applies the out-of-band request {@code filter} to external-source (dataset) leaves of an analyzed plan.
 *
 * <p>This is the request-filter <em>policy</em> over the source-agnostic {@link FilterRewriter} mechanism: it targets
 * {@link ExternalRelation} leaves and version-gates the rewrite (below). {@code FilterRewriter} does the actual work —
 * translating the DSL against each targeted node's schema and wrapping it as an ordinary {@code Filter}, so from there
 * the existing optimizer pushes it down and the engine evaluates it, indistinguishable from a user-written {@code WHERE}.
 * Extending the request filter to other source boundaries (a view, say) is a change of the target predicate here, not of
 * the mechanism. Index leaves keep their existing (pre-analysis) request-filter path and are not touched.
 *
 * <p>The translation is <em>fail-closed</em>: a construct outside the supported subset fails the whole query with a 400
 * ({@link IllegalArgumentException}) naming the construct, rather than silently applying a widened superset. A filter
 * that translates to a supported no-op ({@code match_all}) leaves the relation read unfiltered.
 *
 * <p>The rewrite is <em>feature-flagged</em>. Applying the filter to datasets changes what an existing dataset query
 * returns — a filter that used to be dropped now selects rows, and DSL outside the supported subset now fails the
 * query — so it is gated on {@link #REQUEST_FILTER_ON_DATASET_FEATURE_FLAG}: on by default in snapshot builds (so
 * development, CI and tests exercise it) and excluded from release builds until we choose to ship it. While it is off
 * the relation is read unfiltered <em>with a warning</em>, which is the behavior datasets had before this feature
 * existed — never a silent drop.
 *
 * <p>The rewrite is also version-gated. The translated predicate can contain {@code mv_in_range}, which older nodes do
 * not have; the inserted {@code Filter} rides inside the fragment distributed to data nodes, so on a mixed-version
 * cluster an older node would fail to deserialize it. Below {@link #ESQL_REQUEST_FILTER_ON_DATASET} the rewrite is
 * skipped entirely — datasets are read unfiltered (the pre-feature behavior) with a warning — rather than shipping a
 * plan a peer cannot read. This mirrors how the analyzer and verifier gate version-sensitive rewrites on
 * {@code context.minimumVersion()}.
 */
public final class RequestFilterRewriter {

    /**
     * Gates applying the request filter to datasets: on by default in snapshot builds, excluded from release builds
     * unless {@code -Des.esql_request_filter_on_dataset_feature_flag_enabled=true}. Shipping this code therefore cannot
     * change what an existing dataset query returns until we decide to turn it on.
     */
    public static final FeatureFlag REQUEST_FILTER_ON_DATASET_FEATURE_FLAG = new FeatureFlag("esql_request_filter_on_dataset");

    static final TransportVersion ESQL_REQUEST_FILTER_ON_DATASET = TransportVersion.fromName("esql_request_filter_on_dataset");

    private RequestFilterRewriter() {}

    /**
     * @param enabled        whether the feature is on (production passes {@link #REQUEST_FILTER_ON_DATASET_FEATURE_FLAG});
     *                       when {@code false} the relation is read unfiltered with a warning. A parameter rather than a
     *                       direct flag read so the disabled path is unit-testable.
     * @param configuration  the query configuration — anchors {@code now} date math so a request filter over an
     *                       external source resolves {@code "now-15m"} to the same instant the index path would, and
     *                       supplies the locale for case-folding.
     * @param minimumVersion the minimum transport version across the nodes this plan targets; below
     *                       {@link #ESQL_REQUEST_FILTER_ON_DATASET} the rewrite is skipped (see the class javadoc).
     */
    public static LogicalPlan rewrite(
        LogicalPlan analyzed,
        QueryBuilder requestFilter,
        boolean enabled,
        Configuration configuration,
        TransportVersion minimumVersion
    ) {
        if (requestFilter == null) {
            return analyzed;
        }
        if (enabled == false) {
            warnNotApplied(analyzed, "applying the request filter to datasets is not enabled in this build");
            return analyzed;
        }
        if (minimumVersion.supports(ESQL_REQUEST_FILTER_ON_DATASET) == false) {
            warnNotApplied(analyzed, "the cluster contains a node too old to evaluate the translated filter");
            return analyzed;
        }
        // Target the dataset source relations; index leaves keep their existing (pre-analysis) request-filter path.
        // Translation is fail-closed: an unsupported construct throws out of FilterRewriter and becomes a 400.
        try {
            return FilterRewriter.rewrite(analyzed, ExternalRelation.class::isInstance, requestFilter, configuration);
        } catch (TranslationUnsupportedException e) {
            throw new IllegalArgumentException(
                "The request filter uses a Query DSL construct not supported on external datasets: [" + e.construct() + "]",
                e
            );
        }
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
                "The request filter was not applied to external dataset(s) [{}] because {}; they were read unfiltered. "
                    + "Use a WHERE clause to filter rows from external datasets instead",
                String.join(", ", datasets),
                reason
            );
        }
    }

    private static String name(ExternalRelation relation) {
        return relation.datasetName() != null ? relation.datasetName() : relation.sourcePath();
    }
}
