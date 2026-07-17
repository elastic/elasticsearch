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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Generic mechanism that rewrites a logical plan to apply a Query DSL {@code filter} as an ordinary {@link Filter}.
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
 * <p>Partial application: a construct outside the supported subset is dropped per-clause with a superset substitution
 * (so the result over-matches rather than silently dropping a row it should have returned) and reported to
 * {@code onDropped}, while every supported clause still applies. A filter with no supported clause folds to a no-op and
 * the node is left unwrapped.
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
     * @param filter      the Query DSL to translate; must not be null (callers gate an absent filter themselves).
     * @param nowInMillis the query's start time, epoch millis, anchoring {@code now} date math in the translated filter.
     * @param onDropped   invoked once per clause the translator could not apply at a given target node, so the caller can
     *                    surface it (e.g. a response-header warning); the node is the one the clause was dropped for.
     */
    public static LogicalPlan rewrite(
        LogicalPlan plan,
        Predicate<? super LogicalPlan> target,
        QueryBuilder filter,
        long nowInMillis,
        BiConsumer<LogicalPlan, QueryDslTranslator.DroppedClause> onDropped
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
            }, byName.keySet(), nowInMillis, true);
            // Partial application: an unsupported clause is not fatal — the translator drops it with a superset
            // substitution (so the result over-matches, never silently dropping rows) and records it, while every
            // supported clause still applies. Each dropped clause is handed to the caller to surface.
            Expression condition = translator.translate(filter);
            for (QueryDslTranslator.DroppedClause clause : translator.droppedClauses()) {
                onDropped.accept(node, clause);
            }
            // A wholly-unsupported filter folds to TRUE (a no-op); leave the node unwrapped rather than wrap it.
            return condition == Literal.TRUE ? node : new Filter(node.source(), node, condition);
        });
        // The inserted Filter and the rebuilt spine above it are fresh nodes at stage NEW; the plan was already
        // analyzed, so mark the (idempotent for unchanged nodes) tree analyzed to satisfy the pre-optimizer.
        rewritten.forEachDown(LogicalPlan.class, LogicalPlan::setAnalyzed);
        return rewritten;
    }
}
