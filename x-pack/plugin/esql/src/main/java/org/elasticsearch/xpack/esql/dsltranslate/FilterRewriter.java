/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Rewrites a (logical plan, Query DSL {@code filter}) pair into an equivalent logical plan, by installing the filter as
 * an ordinary {@link Filter}.
 *
 * <p>The mechanism is source-agnostic: it installs the filter above <em>every node the {@code target} predicate
 * selects</em> — a dataset relation, a view boundary, or any point in the tree — and translates the DSL against
 * <em>that node's own output schema</em> (a present field binds to its attribute, a missing field to {@link Literal#NULL}).
 * Binding is therefore relative to the insertion point, so the installed filter behaves exactly like a user-written
 * {@code WHERE} placed there. Callers decide <em>where</em> (the predicate); this class decides nothing about datasets,
 * indices, or views.
 *
 * <p>It is meant to run on the analyzed plan <em>before</em> optimization, so the inserted {@link Filter} enters the
 * ordinary optimizer pipeline — the existing filter-pushdown rules push it toward the source and prune with it wherever
 * the source and the translated predicate allow, indistinguishable from a hand-written filter.
 *
 * <p>Translation is a collecting walk: every unsupported leaf is recorded in {@link RewriteResult#failures()} rather
 * than thrown. The applied expression in the rewritten plan is the translatable subset; the caller decides what to do
 * with any failures. A filter that translates to a supported no-op ({@code match_all}) leaves the node unwrapped.
 */
public final class FilterRewriter {

    /**
     * A (node, clause) pair produced when a target node's filter could not be fully translated. {@link #node()} is
     * the plan node the filter was being applied to; {@link #clause()} is the specific offending DSL leaf.
     */
    public record NodeFailure(LogicalPlan node, QueryDslTranslator.UnsupportedClause clause) {}

    /**
     * The result of a {@link #rewrite} call: the rewritten plan (with the translatable filter subset installed
     * above each target node) and the list of translation failures ({@link #failures()}). A fully-translated
     * filter has an empty {@link #failures()} list.
     */
    public record RewriteResult(LogicalPlan plan, List<NodeFailure> failures) {
        public boolean isComplete() {
            return failures.isEmpty();
        }
    }

    private FilterRewriter() {}

    /**
     * Installs {@code filter} as a {@link Filter} above every node of {@code plan} that {@code target} accepts,
     * binding it against that node's output schema. Returns a {@link RewriteResult} holding the rewritten plan and
     * any translation failures. The applied expression in the plan is always the fully-translatable subset of the
     * filter (the AND of top-level conjuncts that translated without errors); the caller decides what to do with
     * any failures in {@link RewriteResult#failures()}.
     *
     * @param target        selects the nodes to install the filter above. Prefer the lowest matching nodes:
     *                      {@code transformUp} rebuilds a parent whose child changed, so a predicate matching both a
     *                      node and its ancestor may not see the rebuilt ancestor.
     * @param filter        the Query DSL to translate; must not be null.
     * @param configuration the query configuration, carrying the {@code now} anchor for date math and the locale
     *                      for case-folding.
     */
    public static RewriteResult rewrite(
        LogicalPlan plan,
        Predicate<? super LogicalPlan> target,
        QueryBuilder filter,
        Configuration configuration
    ) {
        Objects.requireNonNull(filter, "filter must not be null");
        List<NodeFailure> allFailures = new ArrayList<>();
        LogicalPlan rewritten = plan.transformUp(LogicalPlan.class, node -> {
            if (target.test(node) == false) {
                return node;
            }
            Map<String, Attribute> byName = new HashMap<>();
            for (Attribute a : node.output()) {
                byName.put(a.name(), a);
            }
            QueryDslTranslator translator = new QueryDslTranslator(name -> {
                Attribute a = byName.get(name);
                return a != null ? a : Literal.NULL;
            }, byName.keySet(), configuration);
            QueryDslTranslator.TranslationResult result = translator.translate(filter);
            for (QueryDslTranslator.UnsupportedClause u : result.unsupported()) {
                allFailures.add(new NodeFailure(node, u));
            }
            Expression condition = result.applied();
            // A filter that translates to a supported no-op (match_all -> TRUE) leaves the node unwrapped.
            return condition == Literal.TRUE ? node : new Filter(node.source(), node, condition);
        });
        // The inserted Filter and the rebuilt spine above it are fresh nodes at stage NEW; the plan was already
        // analyzed, so mark the (idempotent for unchanged nodes) tree analyzed to satisfy the pre-optimizer.
        rewritten.forEachDown(LogicalPlan.class, LogicalPlan::setAnalyzed);
        return new RewriteResult(rewritten, allFailures);
    }
}
