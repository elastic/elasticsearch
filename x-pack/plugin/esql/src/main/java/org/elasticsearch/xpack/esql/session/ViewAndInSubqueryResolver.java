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
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Iteratively resolves views and {@code IN} /subquery expressions until a fixed point is reached.
 * <p>
 * Views and {@code IN} subqueries form a mutually recursive expansion problem: a view definition may
 * contain an {@code IN} subquery, and an {@code IN} subquery may reference a view. Each iteration:
 * <ol>
 *   <li>{@link ViewResolver} expands view references, which may introduce new {@code InSubquery} expressions</li>
 *   <li>{@link InSubqueryResolver} converts {@code InSubquery} to {@code SemiJoin}/{@code AntiJoin}, which may expose
 *       new view references (previously hidden inside {@code InSubquery} expression trees)</li>
 * </ol>
 * The loop terminates when neither resolver produces changes, then {@link InSubqueryResolver} validates that no
 * unresolved {@code InSubquery} expressions remain.
 */
public final class ViewAndInSubqueryResolver {

    /**
     * Maximum zero-based iteration index allowed for the view / {@code IN} subquery resolution loop
     * before failing. The loop may run with {@code iteration ==} this value; the next pass fails.
     * <p>
     * This setting is registered as OperatorDynamic so it is not exposed to end users yet.
     * To fully expose it later:
     * <ol>
     *   <li>Change OperatorDynamic to Dynamic (makes it user-settable on self-managed)</li>
     *   <li>Add ServerlessPublic (makes it visible to non-operator users on Serverless)</li>
     * </ol>
     */
    public static final Setting<Integer> MAX_VIEW_IN_SUBQUERY_RESOLUTION_ITERATIONS_SETTING = Setting.intSetting(
        "esql.planning.max_view_in_subquery_resolution_iterations",
        10,
        0,
        100,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    private final ViewResolver viewResolver;
    private volatile int maxViewInSubqueryResolutionIterations;

    public ViewAndInSubqueryResolver(ViewResolver viewResolver, ClusterService clusterService) {
        this.viewResolver = viewResolver;
        clusterService.getClusterSettings()
            .initializeAndWatch(MAX_VIEW_IN_SUBQUERY_RESOLUTION_ITERATIONS_SETTING, v -> this.maxViewInSubqueryResolutionIterations = v);
    }

    /**
     * Resolves views and {@code IN} subqueries to a fixed point. View query text is accumulated for
     * {@link Configuration} on the returned {@link ViewResolver.ViewResolutionResult}.
     */
    public void resolve(
        LogicalPlan plan,
        BiFunction<String, String, LogicalPlan> viewParser,
        ActionListener<ViewResolver.ViewResolutionResult> listener
    ) {
        resolve(plan, viewParser, new HashMap<>(), 0, listener);
    }

    private void resolve(
        LogicalPlan plan,
        BiFunction<String, String, LogicalPlan> viewParser,
        Map<String, String> accumulatedViewQueries,
        int iteration,
        ActionListener<ViewResolver.ViewResolutionResult> listener
    ) {
        if (iteration > maxViewInSubqueryResolutionIterations) {
            listener.onFailure(
                new VerificationException(
                    "Too many view/IN subquery resolution iterations: "
                        + iteration
                        + " (exceeds "
                        + MAX_VIEW_IN_SUBQUERY_RESOLUTION_ITERATIONS_SETTING.getKey()
                        + "="
                        + maxViewInSubqueryResolutionIterations
                        + ")"
                )
            );
            return;
        }

        // Step 1: Resolve views
        viewResolver.replaceViews(plan, viewParser, listener.delegateFailureAndWrap((l, viewResult) -> {
            boolean viewsExpanded = viewResult.viewQueries().isEmpty() == false;
            accumulatedViewQueries.putAll(viewResult.viewQueries());
            LogicalPlan afterViews = viewResult.plan();

            // Step 2: Resolve InSubquery expressions with validation.
            // Throws VerificationException immediately if InSubquery is used in an unsupported position
            // (e.g. EVAL, SORT), so we fail fast without continuing to iterate.
            LogicalPlan afterInSubquery = InSubqueryResolver.resolve(afterViews);
            boolean inSubqueryResolved = afterInSubquery != afterViews;

            // Step 3: Check if another round is needed
            // - If InSubquery was resolved (plan changed), new plan nodes may contain view references → need ViewResolver
            // - If views were expanded, InSubqueryResolver may have resolved new InSubquery from view definitions
            if (inSubqueryResolved || viewsExpanded) {
                resolve(afterInSubquery, viewParser, accumulatedViewQueries, iteration + 1, l);
            } else {
                l.onResponse(new ViewResolver.ViewResolutionResult(afterInSubquery, accumulatedViewQueries));
            }
        }));
    }
}
