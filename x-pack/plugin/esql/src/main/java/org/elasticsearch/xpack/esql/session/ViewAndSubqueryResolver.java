/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Iteratively resolves views and {@code InSubquery} expressions until a fixed point is reached.
 * <p>
 * Views and IN subqueries form a mutually recursive expansion problem: a view definition may contain an IN subquery, and an IN subquery
 * may reference a view. Each iteration:
 * <ol>
 *   <li>{@link ViewResolver} expands view references, which may introduce new {@code InSubquery} expressions</li>
 *   <li>{@link InSubqueryResolver} converts {@code InSubquery} to {@code SemiJoin}/{@code AntiJoin}/{@code LeftSemiJoin}, which may
 *   expose new view references (previously hidden inside {@code InSubquery} expression trees)</li>
 * </ol>
 * The loop terminates when neither resolver produces changes.
 */
public final class ViewAndSubqueryResolver {

    private static final Logger log = LogManager.getLogger(ViewAndSubqueryResolver.class);

    // Maximum iteration allowed for the view / subquery resolution loop before failing.
    public static final Setting<Integer> MAX_VIEW_SUBQUERY_RESOLUTION_ITERATIONS_SETTING = Setting.intSetting(
        "esql.planning.max_view_subquery_resolution_iterations",
        10,
        1,
        100,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    private final ViewResolver viewResolver;
    private volatile int maxViewSubqueryResolutionIterations;

    public ViewAndSubqueryResolver(ClusterService clusterService, ViewResolver viewResolver) {
        this.viewResolver = viewResolver;
        clusterService.getClusterSettings()
            .initializeAndWatch(MAX_VIEW_SUBQUERY_RESOLUTION_ITERATIONS_SETTING, v -> this.maxViewSubqueryResolutionIterations = v);
    }

    /**
     * Resolves views and IN subqueries.
     *
     * @param viewResolvedListener invoked with the plan produced by every {@link ViewResolver} pass, before that iteration's
     * {@code InSubquery} expressions are rewritten away. Each pass exposes the {@code InSubquery} expressions visible at that point —
     * including those revealed only after a view referenced from inside another IN subquery is expanded in a later iteration — so callers
     * can inspect the full set across iterations. Callers are responsible for de-duplicating any per-query telemetry they derive from it,
     * since the same originating expression can be surfaced on more than one pass.
     */
    public void resolve(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> viewParser,
        Consumer<LogicalPlan> viewResolvedListener,
        ActionListener<ViewResolver.ViewResolutionResult> listener
    ) {
        resolve(plan, projectRouting, viewParser, viewResolvedListener, new HashMap<>(), 0, listener);
    }

    private void resolve(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> viewParser,
        Consumer<LogicalPlan> viewResolvedListener,
        Map<String, String> accumulatedViewQueries,
        int iteration,
        ActionListener<ViewResolver.ViewResolutionResult> listener
    ) {
        if (iteration > maxViewSubqueryResolutionIterations) {
            listener.onFailure(
                new VerificationException(
                    "Too many view/subquery resolution iterations: "
                        + iteration
                        + " (exceeds "
                        + MAX_VIEW_SUBQUERY_RESOLUTION_ITERATIONS_SETTING.getKey()
                        + "="
                        + maxViewSubqueryResolutionIterations
                        + ")"
                )
            );
            return;
        }

        // Step 1: Resolve views
        viewResolver.replaceViews(plan, projectRouting, viewParser, listener.delegateFailureAndWrap((l, viewResult) -> {
            accumulatedViewQueries.putAll(viewResult.viewQueries());
            LogicalPlan afterViews = viewResult.plan();
            if (log.isDebugEnabled()) {
                log.debug("ViewAndSubqueryResolver: logical plan after ViewResolver, iteration {}:\n{}", iteration, afterViews);
            }

            // Surface this iteration's view-resolved plan before the InSubqueryResolver below rewrites its InSubquery expressions away.
            // Reporting every pass (not just the first) lets callers also see IN subqueries that only become visible once a view
            // referenced from inside another IN subquery is expanded in a later iteration.
            viewResolvedListener.accept(afterViews);

            // Step 2: Resolve InSubquery expressions with validation.
            // Throws VerificationException immediately if InSubquery is used in an unsupported position
            // (e.g. EVAL, SORT), so we fail fast without continuing to iterate.
            LogicalPlan afterInSubquery = InSubqueryResolver.resolve(afterViews);
            if (log.isDebugEnabled()) {
                log.debug("ViewAndSubqueryResolver: logical plan after InSubqueryResolver, iteration {}:\n{}", iteration, afterInSubquery);
            }

            boolean inSubqueryResolved = afterInSubquery != afterViews;

            // Step 3: Check if another round is needed
            // If InSubquery was resolved (plan changed), new plan nodes may contain view references → need ViewResolver again.
            if (inSubqueryResolved) {
                resolve(afterInSubquery, projectRouting, viewParser, viewResolvedListener, accumulatedViewQueries, iteration + 1, l);
            } else {
                l.onResponse(new ViewResolver.ViewResolutionResult(afterInSubquery, accumulatedViewQueries));
            }
        }));
    }
}
