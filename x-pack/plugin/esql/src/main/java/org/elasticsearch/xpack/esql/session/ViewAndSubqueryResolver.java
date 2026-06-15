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
import java.util.function.Consumer;

/**
 * Resolves views and {@code InSubquery} expressions in a single pass.
 * <p>
 * {@link ViewResolver#replaceViews} now handles {@code InSubquery} expressions inline as it traverses the plan: whenever it
 * encounters a {@link org.elasticsearch.xpack.esql.plan.logical.Filter} containing an {@code InSubquery}, it rewrites it to a
 * {@link org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin}/{@link org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin}/
 * {@link org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin} and immediately recurses to resolve any view references in the
 * newly created subquery plans. This means a single {@code replaceViews} call fully expands the plan — no fixed-point loop is needed.
 * <p>
 * The {@code viewResolvedListener} consumer is invoked with the <em>original</em> (pre-resolution) plan so callers can inspect
 * {@code InSubquery} expressions that appear directly in the query before they are rewritten. Callers wanting to detect
 * {@code InSubquery} usage should check the original plan via
 * {@link InSubqueryResolver#hasInSubqueryInFilter(LogicalPlan)}.
 */
public final class ViewAndSubqueryResolver {

    private final ViewResolver viewResolver;

    public ViewAndSubqueryResolver(ViewResolver viewResolver) {
        this.viewResolver = viewResolver;
    }

    /**
     * Resolves views and IN subqueries in the plan.
     *
     * @param viewResolvedListener invoked with the original (pre-resolution) plan so callers can check for {@code InSubquery} expressions
     *                             before they are rewritten.
     */
    public void resolve(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> viewParser,
        Consumer<LogicalPlan> viewResolvedListener,
        ActionListener<ViewResolver.ViewResolutionResult> listener
    ) {
        // Surface the original plan before resolution so callers can detect InSubquery usage.
        viewResolvedListener.accept(plan);

        viewResolver.replaceViews(plan, projectRouting, viewParser, listener.delegateFailureAndWrap((l, viewResult) -> {
            // Validate: no InSubquery expressions should survive view+subquery resolution.
            InSubqueryResolver.verify(viewResult.plan());
            l.onResponse(viewResult);
        }));
    }
}
