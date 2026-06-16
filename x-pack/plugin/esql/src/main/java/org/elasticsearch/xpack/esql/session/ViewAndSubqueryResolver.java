/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.function.BiFunction;

/**
 * Resolves views and {@code InSubquery} expressions in a single pass.
 * <p>
 * {@code ViewResolver#replaceViews} now handles {@code InSubquery} expressions inline as it traverses the plan: whenever it
 * encounters a {@link org.elasticsearch.xpack.esql.plan.logical.Filter} containing an {@code InSubquery}, it rewrites it to a
 * {@link org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin}/{@link org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin}/
 * {@link org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin} and immediately recurses to resolve any view references in the
 * newly created subquery plans. This means a single {@code replaceViews} call fully expands the plan — no fixed-point loop is needed.
 * <p>
 * Whether any {@code InSubquery} was rewritten — directly in the query <em>or</em> inside a view definition — is reported back on
 * {@link ViewResolver.ViewResolutionResult#hasInSubquery()} so callers (e.g. {@code EsqlSession}) can drive {@code IN_SUBQUERY}
 * telemetry. The pre-resolution plan cannot be used for this because the IN subqueries hidden inside view bodies are not yet visible.
 *
 * TODO: {@code ViewResolver} and {@code ViewAndSubqueryResolver} need refactor. Keep he core of view resolution in {@code ViewResolver},
 *  and have {@code ViewAndSubqueryResolver} drives the plan tree traversal, it calls {@code ViewResolver} and {@code InSubqueryResolver}
 *  to do the view and IN subquery resolution respectively.
 */
public final class ViewAndSubqueryResolver {

    private final ViewResolver viewResolver;

    public ViewAndSubqueryResolver(ViewResolver viewResolver) {
        this.viewResolver = viewResolver;
    }

    /**
     * Resolves views and IN subqueries in the plan. The resulting {@link ViewResolver.ViewResolutionResult} reports whether any IN
     * subquery was rewritten during resolution.
     */
    public void resolve(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> viewParser,
        ActionListener<ViewResolver.ViewResolutionResult> listener
    ) {
        viewResolver.replaceViews(plan, projectRouting, viewParser, listener.delegateFailureAndWrap((l, viewResult) -> {
            // Validate: no InSubquery expressions should survive view+subquery resolution.
            InSubqueryResolver.verify(viewResult.plan());
            l.onResponse(viewResult);
        }));
    }
}
