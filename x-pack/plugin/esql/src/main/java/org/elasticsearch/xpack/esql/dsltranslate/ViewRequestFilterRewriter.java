/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

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
 * <p>A construct outside the supported subset never fails the query, matching {@link RequestFilterRewriter}: the
 * translatable conjuncts are applied and the rest are dropped with a {@link HeaderWarning} naming each construct and the
 * view it was meant for. Dropping only ever widens what matches, never narrows it. The request filter is not part of the
 * query text, so a construct it cannot honor is not a query error; and failing would turn a working Kibana panel into an
 * error the moment a view starts matching its pattern. A filter that translates to a supported no-op ({@code match_all})
 * leaves the view unfiltered.
 *
 * <p>The rewrite is <em>version-gated</em>. Both this rewriter and {@link RequestFilterRewriter} (for datasets) use
 * the same {@link QueryDslTranslator}, which can emit {@code mv_in_range} nodes for range queries; older nodes do not
 * know that function and would fail to deserialize a plan containing it. The gate is therefore the same version:
 * {@link RequestFilterRewriter#ESQL_REQUEST_FILTER_ON_DATASET}. Any cluster new enough to apply the dataset rewrite is
 * already new enough to apply the view rewrite — introducing a separate transport version would add no protection and
 * would fragment the version history unnecessarily. Below that version the rewrite is skipped entirely rather than shipping a
 * plan a peer cannot read, and the query falls back to the pre-feature behavior: the raw DSL is pushed into the view's source
 * scan (see below), with a warning, because that filters computed fields against their raw indexed values. Whether such
 * mixed clusters should instead fail the query outright is an open decision; {@link #supportsRewrite} is the single place
 * both halves consult, so changing it changes them together.
 *
 * <p>The raw DSL filter must <em>not</em> also reach the source scan inside a view subplan, or a filter on a field the view
 * computes (via {@code EVAL}/{@code STATS}) would match no documents in the source index and override the correct result
 * produced here. {@code Mapper#mapMergePlan} therefore marks every
 * {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec} under a view branch (see
 * {@code FragmentExec#isFromViewBranch()}) and {@code PlannerUtils.integrateEsFilterIntoFragment} skips those fragments —
 * but only while {@link #supportsRewrite} holds, so the Lucene path takes over exactly when this rewrite stands down. The
 * mark itself is structural and always set; which path a marked fragment takes is decided where the filter is integrated.
 * The marker is coordinator-only state on a plan node, so it has to survive generic tree rebuilds: it is part of
 * {@code FragmentExec}'s {@code NodeInfo} and its {@code equals}/{@code hashCode} for exactly that reason.
 *
 * <p>Because the filter is bound against the view's output schema, the fields it references must have been loaded from
 * field-caps — and field-name pruning is computed from the query, which need not mention them (Kibana routinely filters on
 * columns it discovered via {@code FROM my_view | LIMIT 0}, not on ones the query selects). Pre-analysis therefore adds the
 * filter's own field references to the pruned set, via {@link QueryDslFieldNameExtractor} from
 * {@code EsqlSession#resolveFieldNames}. Where those references cannot be enumerated — notably a {@code multi_match},
 * whose fields are resolved against the source's complete field list — it falls back to requesting every field.
 */
public final class ViewRequestFilterRewriter {

    private ViewRequestFilterRewriter() {}

    /**
     * Whether every node the plan targets can deserialize the functions the translated filter may contain. This is the
     * version gate described in the class javadoc; {@link #rewrite} and {@code PlannerUtils.integrateEsFilterIntoFragment}
     * both consult it so the logical filter and the Lucene fallback can never both apply, or both be absent.
     */
    public static boolean supportsRewrite(TransportVersion minimumVersion) {
        return minimumVersion.supports(ESQL_REQUEST_FILTER_ON_DATASET);
    }

    /**
     * Whether {@code requestFilter} could actually put a {@link Filter} on a view's output — i.e. whether anything downstream needs
     * view boundaries preserved. Callers use this to decide {@code preserveViewBoundaries}; keeping the decision here means the
     * planner and this rewriter cannot disagree about it.
     *
     * <p>Returns {@code false} for a filter that matches every document. Kibana sends an empty filter rather than omitting the field
     * when no filtering is wanted, and {@link #applyRequestFilterToViewBranches} would translate such a filter to {@link Literal#TRUE}
     * and drop it as a no-op — so without this check those requests would suppress view compaction in order to install nothing.
     *
     * <p>The test is syntactic, so it needs neither a {@link Configuration} nor an output schema and can run before either exists.
     * It is deliberately <em>narrower</em> than the translator's notion of a no-op: a {@code should} group or a {@code must_not} counts
     * as filtering even in the shapes where the translator would discard it. Erring that way costs an unnecessary wrapper; erring the
     * other way would push the filter into the view's source scan.
     */
    public static boolean appliesToViewOutputs(@Nullable QueryBuilder requestFilter) {
        return requestFilter != null && matchesEveryDocument(requestFilter) == false;
    }

    /**
     * Structurally true when {@code filter} cannot exclude any document: {@code match_all}, or a {@code bool} with no {@code should}
     * and no {@code must_not} whose {@code must}/{@code filter} clauses (if any) all match every document too — which covers both an
     * empty {@code bool} and one nesting only empty bools.
     */
    private static boolean matchesEveryDocument(QueryBuilder filter) {
        if (filter instanceof MatchAllQueryBuilder) {
            return true;
        }
        if (filter instanceof BoolQueryBuilder bool) {
            if (bool.should().isEmpty() == false || bool.mustNot().isEmpty() == false) {
                return false;
            }
            return Stream.concat(bool.must().stream(), bool.filter().stream()).allMatch(ViewRequestFilterRewriter::matchesEveryDocument);
        }
        return false;
    }

    /**
     * Rewrites {@code analyzed} so that {@code requestFilter} is applied as an ordinary {@link Filter} above each view
     * subplan boundary in every {@link ViewUnionAll} node of the plan, bound against that boundary's output schema.
     *
     * @param analyzed       the fully analyzed logical plan — must have passed through
     *                       {@link org.elasticsearch.xpack.esql.view.ViewCompaction#postIndexResolution} so that
     *                       {@link ViewUnionAll} nodes carry their resolved subplans.
     * @param requestFilter  the Query DSL from the request; {@code null} means no filter and the plan is returned
     *                       unchanged.
     * @param configuration  the query configuration — anchors {@code now} date math so a request filter over a view
     *                       resolves {@code "now-15m"} to the same instant the index path would, and supplies the
     *                       locale for case-folding.
     * @param minimumVersion the minimum transport version across the nodes this plan targets; when
     *                       {@link #supportsRewrite} is false the rewrite is skipped and the filter falls back to the
     *                       view's source scan (see the class javadoc).
     */
    public static LogicalPlan rewrite(
        LogicalPlan analyzed,
        QueryBuilder requestFilter,
        Configuration configuration,
        TransportVersion minimumVersion
    ) {
        if (requestFilter == null) {
            return analyzed;
        }
        if (supportsRewrite(minimumVersion) == false) {
            warnPushedIntoSources(analyzed);
            return analyzed;
        }
        // Walk down and stop at the first ViewUnionAll on each path: the filter belongs on the output of the views the
        // *query* names, and a view nested inside another view's definition is an implementation detail of the outer
        // view, not a boundary the request filter addresses. Filtering an inner boundary too would apply the predicate
        // before the outer view's own processing — the very mistake the Lucene push-in path makes — and for a field the
        // outer view computes it would bind to NULL there and silently drop every row.
        Set<String> skipped = new LinkedHashSet<>();
        LogicalPlan rewritten = analyzed.transformDownSkipBranch((plan, skipBranch) -> {
            if (plan instanceof ViewUnionAll vua) {
                skipBranch.set(true);
                return applyRequestFilterToViewBranches(vua, requestFilter, configuration, skipped);
            }
            return plan;
        });
        if (skipped.isEmpty() == false) {
            warnUnsupportedClauses(skipped);
        }
        // The inserted Filter nodes and the spine rebuilt above them are at stage NEW; the plan was already
        // analyzed, so mark the whole tree analyzed to satisfy the pre-optimizer.
        rewritten.forEachDown(LogicalPlan.class, LogicalPlan::setAnalyzed);
        return rewritten;
    }

    /**
     * Installs the filter above every actual view branch of {@code vua}, bound against that branch's output schema.
     *
     * <p>Bare-index and literal-subquery branches are left alone — they are not view branches and the existing
     * Lucene-scan path handles them. {@link ViewUnionAll#isViewBranch(String)} is the test to use: {@code key != null}
     * is not sufficient, because bare-index branches carry {@code "main"} and literal subqueries carry
     * {@code "unnamed_view_<hash>"}. Unsupported constructs are recorded in {@code skipped} as {@code [construct] on view [name]}
     * and the translatable remainder is installed; the caller warns once for the whole plan.
     */
    private static LogicalPlan applyRequestFilterToViewBranches(
        ViewUnionAll vua,
        QueryBuilder requestFilter,
        Configuration configuration,
        Set<String> skipped
    ) {
        LinkedHashMap<String, LogicalPlan> newSubqueries = new LinkedHashMap<>();
        boolean changed = false;
        for (Map.Entry<String, LogicalPlan> entry : vua.namedSubqueries().entrySet()) {
            String key = entry.getKey();
            LogicalPlan child = entry.getValue();
            if (vua.isViewBranch(key) == false) {
                newSubqueries.put(key, child);
            } else {
                QueryDslTranslator.TranslationResult result = translateFilter(child.output(), requestFilter, configuration);
                // The same construct can fail more than once on one view (two wildcard clauses, say); the set keeps the header short.
                for (QueryDslTranslator.UnsupportedClause unsupported : result.unsupported()) {
                    skipped.add("[" + unsupported.construct() + "] on view [" + key + "]");
                }
                Expression condition = result.applied();
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
    }

    /**
     * Translates {@code filter} into a {@link QueryDslTranslator.TranslationResult} bound against the given output
     * schema. A field present in {@code output} binds to its {@link Attribute}; a field absent from {@code output}
     * binds to {@link Literal#NULL} so that the DSL's missing-field leniency is reproduced automatically.
     */
    private static QueryDslTranslator.TranslationResult translateFilter(
        List<Attribute> output,
        QueryBuilder filter,
        Configuration configuration
    ) {
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

    /** Warns, via a response header, which constructs were dropped from the filter and on which views. */
    private static void warnUnsupportedClauses(Set<String> skipped) {
        HeaderWarning.addWarning(
            "The request filter could not be fully applied to view(s); the following Query DSL constructs are not supported and were "
                + "skipped: "
                + String.join("; ", skipped)
                + ". Use a WHERE clause to filter rows from views instead"
        );
    }

    /**
     * Warns, via a response header, that the request filter was pushed into the source indices of the views in {@code plan}
     * rather than applied to their output, naming those views, when there are any.
     */
    private static void warnPushedIntoSources(LogicalPlan plan) {
        TreeSet<String> viewNames = new TreeSet<>();
        plan.forEachDown(ViewUnionAll.class, vua -> {
            for (String key : vua.viewBranchKeys()) {
                viewNames.add(key);
            }
        });
        if (viewNames.isEmpty() == false) {
            HeaderWarning.addWarning(
                "The request filter was applied to the source indices of view(s) [{}] rather than to their output because the "
                    + "cluster contains a node too old to evaluate the translated filter; a filter on a field a view computes "
                    + "or renames may therefore be wrong. Use a WHERE clause to filter rows from views instead",
                String.join(", ", viewNames)
            );
        }
    }
}
