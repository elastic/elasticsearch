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

import java.util.HashMap;
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
 * <p>Translation is fail-closed: a construct outside the supported subset raises {@link TranslationUnsupportedException},
 * which propagates out of {@code rewrite} for the caller to turn into an error. A filter that translates to a supported
 * no-op ({@code match_all}) leaves the node unwrapped.
 */
public final class FilterRewriter {

    private FilterRewriter() {}

    /**
     * Installs {@code filter} as a {@link Filter} above every node of {@code plan} that {@code target} accepts, binding
     * it against that node's output schema. Returns the rewritten plan, marked analyzed so the pre-optimizer accepts the
     * fresh {@link Filter} nodes and the spine rebuilt above them.
     *
     * @param target      selects the nodes to install the filter above (e.g. {@code node -> node instanceof ExternalRelation} for a
     *                    source boundary). Prefer the lowest matching nodes: {@code transformUp} rebuilds a parent whose
     *                    child changed, so a predicate matching both a node and its ancestor may not see the rebuilt
     *                    ancestor.
     * @param filter        the Query DSL to translate; must not be null (callers gate an absent filter themselves).
     * @param configuration the query configuration, carrying the {@code now} anchor for date math and the locale for
     *                      case-folding in the translated filter.
     * @throws TranslationUnsupportedException if {@code filter} contains a construct outside the supported subset — the
     *                    translation is fail-closed, so this propagates for the caller to turn into an error.
     */
    public static LogicalPlan rewrite(
        LogicalPlan plan,
        Predicate<? super LogicalPlan> target,
        QueryBuilder filter,
        Configuration configuration
    ) {
        Objects.requireNonNull(filter, "filter must not be null");
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
            // Fail-closed: an unsupported construct throws TranslationUnsupportedException out of transformUp.
            Expression condition = translator.translate(filter);
            // A filter that translates to a supported no-op (match_all -> TRUE) leaves the node unwrapped.
            return condition == Literal.TRUE ? node : new Filter(node.source(), node, condition);
        });
        // The inserted Filter and the rebuilt spine above it are fresh nodes at stage NEW; the plan was already
        // analyzed, so mark the (idempotent for unchanged nodes) tree analyzed to satisfy the pre-optimizer.
        rewritten.forEachDown(LogicalPlan.class, LogicalPlan::setAnalyzed);
        return rewritten;
    }
}
