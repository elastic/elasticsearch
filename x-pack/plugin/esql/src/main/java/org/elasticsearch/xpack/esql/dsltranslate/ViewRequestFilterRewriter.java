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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.dsltranslate.RequestFilterRewriter.ESQL_REQUEST_FILTER_ON_DATASET;

/**
 * Applies the out-of-band request {@code filter} to view subplan boundaries of an analyzed plan.
 *
 * <p>This is the request-filter <em>policy</em> over the source-agnostic {@link FilterRewriter} mechanism, analogous to
 * {@link RequestFilterRewriter} for external datasets: it targets the subplan outputs of logical views — specifically,
 * every non-index child of a {@link ViewUnionAll} — and version-gates the rewrite. {@code FilterRewriter} does the
 * actual translation work (DSL → ES|QL expression, bound against the child's output schema), and the inserted
 * {@link Filter} enters the ordinary optimizer pipeline.
 *
 * <p>A logical view ({@code FROM viewName}) is expanded by {@link org.elasticsearch.xpack.esql.view.ViewResolver} into
 * its subquery plan, stored as a named child of a {@link ViewUnionAll}. That subplan may compute values (via
 * {@code STATS}, {@code EVAL}, {@code RENAME}, …) that do not correspond to any raw field in the view's source index.
 * Applying the request filter via the Lucene scan path (the index path, used for plain index relations) would therefore
 * push the filter into the source index <em>before</em> those computations, producing wrong results for computed or
 * renamed fields and, in the worst case (a field computed by {@code EVAL} that does not exist in the index),
 * silently returning zero rows. This rewriter avoids that by inserting the filter <em>above</em> the view's subplan —
 * i.e., against the view's <em>output</em> schema — so it applies after the view's own processing.
 *
 * <p>Bare-index children of a {@link ViewUnionAll} (stored under the {@code null} key in the named-subqueries map) are
 * left untouched: those are plain Elasticsearch index relations whose request filter is handled by the existing
 * pre-analysis Lucene-scan path.
 *
 * <p>The translation is <em>fail-closed</em>: a construct outside the supported subset fails the whole query with a 400
 * ({@link IllegalArgumentException}) naming the construct, rather than silently applying a widened superset. A filter
 * that translates to a supported no-op ({@code match_all}) leaves the relation read unfiltered.
 *
 * <p>The rewrite is <em>feature-flagged</em>. Applying the filter to view outputs changes what an existing view query
 * returns — a filter that used to be dropped now selects rows, and DSL outside the supported subset now fails the
 * query — so it is gated on {@link #REQUEST_FILTER_ON_VIEW_FEATURE_FLAG}: on by default in snapshot builds (so
 * development, CI and tests exercise it) and excluded from release builds until we choose to ship it.
 *
 * <p>The rewrite is also <em>version-gated</em>. Both this rewriter and {@link RequestFilterRewriter} (for datasets) use
 * the same {@link QueryDslTranslator}, which can emit {@code mv_in_range} nodes for range queries; older nodes do not
 * know that function and would fail to deserialize a plan containing it. The gate is therefore the same version:
 * {@link RequestFilterRewriter#ESQL_REQUEST_FILTER_ON_DATASET}. Any cluster new enough to apply the dataset rewrite is
 * already new enough to apply the view rewrite — introducing a separate transport version would add no protection and
 * would fragment the version history unnecessarily. Below that version the rewrite is skipped entirely — views are read
 * unfiltered (the pre-feature behavior) with a warning — rather than shipping a plan a peer cannot read.
 *
 * <p><b>Known limitation</b>: The existing Lucene-filter integration path ({@code PlannerUtils.integrateEsFilterIntoFragment})
 * applies the raw DSL request filter to <em>all</em> {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}
 * nodes in the physical plan, including those that originate from source indices inside view subplans. For fields that
 * exist in the view's source index (e.g., GROUP BY keys on raw fields) this is semantically redundant but harmless. For
 * fields computed inside the view (e.g., via {@code EVAL}) that do not exist as mapped fields in the source index, the
 * Lucene query returns nothing, overriding the correct result that this rewriter produces. Eliminating this Lucene-path
 * application for view-internal relations is tracked as a follow-up improvement.
 */
public final class ViewRequestFilterRewriter {

    /**
     * Gates applying the request filter to view outputs: on by default in snapshot builds, excluded from release builds
     * unless {@code -Des.esql_request_filter_on_view_feature_flag_enabled=true}. Shipping this code therefore cannot
     * change what an existing view query returns until we decide to turn it on.
     */
    public static final FeatureFlag REQUEST_FILTER_ON_VIEW_FEATURE_FLAG = new FeatureFlag("esql_request_filter_on_view");

    private ViewRequestFilterRewriter() {}

    /**
     * Rewrites {@code analyzed} so that {@code requestFilter} is applied as an ordinary {@link Filter} above each view
     * subplan boundary in every {@link ViewUnionAll} node of the plan, bound against that boundary's output schema.
     *
     * @param analyzed       the fully analyzed logical plan — must have passed through
     *                       {@link org.elasticsearch.xpack.esql.view.ViewCompaction#postIndexResolution} so that
     *                       {@link ViewUnionAll} nodes carry their resolved subplans.
     * @param requestFilter  the Query DSL from the request; {@code null} means no filter and the plan is returned
     *                       unchanged.
     * @param enabled        whether the feature is on (production passes {@link #REQUEST_FILTER_ON_VIEW_FEATURE_FLAG});
     *                       when {@code false} the view is read unfiltered with a warning.
     * @param configuration  the query configuration — anchors {@code now} date math so a request filter over a view
     *                       resolves {@code "now-15m"} to the same instant the index path would, and supplies the
     *                       locale for case-folding.
     * @param minimumVersion the minimum transport version across the nodes this plan targets; below
     *                       {@link RequestFilterRewriter#ESQL_REQUEST_FILTER_ON_DATASET} the rewrite is skipped (see the
     *                       class javadoc).
     * @throws IllegalArgumentException if {@code requestFilter} contains a construct outside the supported subset —
     *                       the translation is fail-closed.
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
            warnNotApplied(analyzed, "applying the request filter to views is not enabled in this build");
            return analyzed;
        }
        if (minimumVersion.supports(ESQL_REQUEST_FILTER_ON_DATASET) == false) {
            warnNotApplied(analyzed, "the cluster contains a node too old to evaluate the translated filter");
            return analyzed;
        }
        // Target only the actual view branches of each ViewUnionAll in the plan. Bare-index and literal
        // subquery branches are NOT view branches and are handled by the existing Lucene-scan request-filter
        // path (or pass through unchanged). Use vua.isViewBranch(key) to distinguish: key != null is NOT
        // sufficient because bare-index branches carry "main" and literal subqueries carry "unnamed_view_<hash>".
        // Translation is fail-closed: an unsupported construct throws out of the transformUp and becomes a 400.
        try {
            LogicalPlan rewritten = analyzed.transformUp(ViewUnionAll.class, vua -> {
                LinkedHashMap<String, LogicalPlan> newSubqueries = new LinkedHashMap<>();
                boolean changed = false;
                for (Map.Entry<String, LogicalPlan> entry : vua.namedSubqueries().entrySet()) {
                    String key = entry.getKey();
                    LogicalPlan child = entry.getValue();
                    if (vua.isViewBranch(key) == false) {
                        // Bare-index or literal-subquery branch: the existing Lucene-scan filter path handles it.
                        newSubqueries.put(key, child);
                    } else {
                        // View subplan: apply the filter against the view's output schema.
                        Expression condition = translateFilter(child.output(), requestFilter, configuration);
                        if (condition == Literal.TRUE) {
                            // match_all → no-op; leave this view unfiltered.
                            newSubqueries.put(key, child);
                        } else {
                            newSubqueries.put(key, new Filter(child.source(), child, condition));
                            changed = true;
                        }
                    }
                }
                if (changed == false) {
                    return vua;
                }
                // Output columns are unchanged: a Filter never adds columns. Preserve viewBranchKeys.
                return new ViewUnionAll(vua.source(), newSubqueries, vua.viewBranchKeys(), vua.output());
            });
            // The inserted Filter nodes and the spine rebuilt above them are at stage NEW; the plan was already
            // analyzed, so mark the whole tree analyzed to satisfy the pre-optimizer.
            rewritten.forEachDown(LogicalPlan.class, LogicalPlan::setAnalyzed);
            return rewritten;
        } catch (TranslationUnsupportedException e) {
            throw new IllegalArgumentException(
                "The request filter uses a Query DSL construct not supported on views: [" + e.construct() + "]",
                e
            );
        }
    }

    /**
     * Translates {@code filter} into an ES|QL {@link Expression} bound against the given output schema. A field present
     * in {@code output} binds to its {@link Attribute}; a field absent from {@code output} binds to
     * {@link Literal#NULL} so that the DSL's missing-field leniency is reproduced automatically.
     */
    private static Expression translateFilter(List<Attribute> output, QueryBuilder filter, Configuration configuration) {
        Map<String, Attribute> byName = new HashMap<>();
        for (Attribute a : output) {
            byName.put(a.name(), a);
        }
        QueryDslTranslator translator = new QueryDslTranslator(name -> {
            Attribute a = byName.get(name);
            return a != null ? a : Literal.NULL;
        }, byName.keySet(), configuration);
        return translator.translate(filter);
    }

    /**
     * Warns, via a response header, that the request filter was not applied to the view subplans in {@code plan},
     * naming those views, when there are any.
     */
    private static void warnNotApplied(LogicalPlan plan, String reason) {
        List<String> viewNames = new ArrayList<>();
        plan.forEachDown(ViewUnionAll.class, vua -> {
            for (String key : vua.viewBranchKeys()) {
                viewNames.add(key);
            }
        });
        List<String> distinct = viewNames.stream().distinct().sorted().toList();
        if (distinct.isEmpty() == false) {
            HeaderWarning.addWarning(
                "The request filter was not applied to view(s) [{}] because {}; they were read unfiltered. "
                    + "Use a WHERE clause to filter rows from views instead",
                String.join(", ", distinct),
                reason
            );
        }
    }
}
