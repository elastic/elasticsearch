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
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
 * <p>Translation is <em>fail-closed by default</em>: a construct outside the supported subset fails the whole query
 * with a 400 ({@link VerificationException}) listing every offending clause, rather than silently applying a widened
 * superset. With {@code allow_partial_dsl_filter=true} the translatable AND-conjuncts are applied and the rest are
 * dropped with a {@link HeaderWarning}. A filter that translates to a supported no-op ({@code match_all}) leaves the
 * relation read unfiltered.
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
     * @param enabled               whether the feature is on (production passes
     *                              {@link #REQUEST_FILTER_ON_DATASET_FEATURE_FLAG}); when {@code false} the relation
     *                              is read unfiltered with a warning. A parameter rather than a direct flag read so
     *                              the disabled path is unit-testable.
     * @param configuration         the query configuration — anchors {@code now} date math so a request filter over
     *                              an external source resolves {@code "now-15m"} to the same instant the index path
     *                              would, and supplies the locale for case-folding.
     * @param minimumVersion        the minimum transport version across the nodes this plan targets; below
     *                              {@link #ESQL_REQUEST_FILTER_ON_DATASET} the rewrite is skipped (see the class
     *                              javadoc).
     * @param allowPartialDslFilter when {@code true}, unsupported DSL clauses are dropped with a warning rather than
     *                              failing the query.
     */
    public static LogicalPlan rewrite(
        LogicalPlan analyzed,
        QueryBuilder requestFilter,
        boolean enabled,
        Configuration configuration,
        TransportVersion minimumVersion,
        boolean allowPartialDslFilter
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
        FilterRewriter.RewriteResult result = FilterRewriter.rewrite(
            analyzed,
            ExternalRelation.class::isInstance,
            requestFilter,
            configuration
        );
        if (result.isComplete() == false) {
            if (allowPartialDslFilter) {
                warnUnsupportedClauses(result.failures());
            } else {
                List<String> messages = new ArrayList<>(result.failures().size());
                for (FilterRewriter.NodeFailure nf : result.failures()) {
                    messages.add(
                        "request filter clause uses [" + nf.clause().construct() + "], unsupported on dataset [" + name(nf.node()) + "]"
                    );
                }
                throw new VerificationException(String.join("\n", messages));
            }
        }
        return result.plan();
    }

    /** Warns about unsupported clauses dropped in partial mode, naming each construct and its dataset. */
    private static void warnUnsupportedClauses(List<FilterRewriter.NodeFailure> failures) {
        // Deduplicate: the same construct can fail several times on the same dataset (e.g. two wildcard clauses),
        // and repeating the pair only inflates the header. LinkedHashSet keeps the first-seen order.
        Set<String> skipped = new LinkedHashSet<>();
        for (FilterRewriter.NodeFailure nf : failures) {
            skipped.add("[" + nf.clause().construct() + "] on dataset [" + name(nf.node()) + "]");
        }
        // "could not be fully applied" is accurate whether some conjuncts were installed or none were.
        HeaderWarning.addWarning(
            "The request filter could not be fully applied to external dataset(s); the following Query DSL constructs"
                + " are not supported and were skipped: "
                + String.join("; ", skipped)
                + ". Use a WHERE clause to filter rows from external datasets instead."
        );
    }

    /** Warns that the filter was not applied to the plan's dataset leaves, naming them, when there are any. */
    private static void warnNotApplied(LogicalPlan plan, String reason) {
        List<String> datasets = plan.collect(ExternalRelation.class::isInstance)
            .stream()
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

    /**
     * The display name of a failure's target node. Today the target predicate only selects {@link ExternalRelation}
     * leaves; the fallback keeps this safe if the predicate is ever broadened to other source boundaries (see the
     * class javadoc) rather than turning into a production {@code ClassCastException}.
     */
    private static String name(LogicalPlan node) {
        if (node instanceof ExternalRelation relation) {
            return relation.datasetName() != null ? relation.datasetName() : relation.sourcePath();
        }
        return node.nodeName();
    }
}
