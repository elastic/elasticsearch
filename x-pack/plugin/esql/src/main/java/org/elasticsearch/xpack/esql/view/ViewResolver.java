/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

/**
 * Resolves view references in a logical plan by expanding each view into the plan parsed from its definition. As part of the same
 * traversal it also rewrites {@code InSubquery} expressions (in {@link Filter} conditions) into {@code SemiJoin}/{@code AntiJoin}/
 * {@code MarkJoin} nodes, so a single pass fully expands the plan — including views referenced from inside IN subqueries and IN
 * subqueries nested in view bodies.
 * <p>
 * Resolution (see {@link #replaceViews}) is a depth-first, top-down (pre-order) traversal of the plan tree. During traversal it
 * intercepts specific node types:
 * <ul>
 *   <li>{@link UnresolvedRelation}: Resolves views and replaces them with their query plans, then recursively processes those
 *       plans</li>
 *   <li>{@link Fork}: Recursively processes each child branch</li>
 *   <li>{@code UnionAll}: Skipped (assumes rewriting is already complete)</li>
 *   <li>{@link AbstractSubqueryJoin}: Recursively processes the left and right sides</li>
 *   <li>{@link Filter}: Calls {@link InSubqueryResolver} to expand any {@code InSubquery} into a {@code SemiJoin}/{@code AntiJoin}/
 *       {@code MarkJoin}, then recurses into the newly created subquery plans to resolve view references nested there</li>
 *   <li>{@link ViewUnionAll}: Skipped (already the result of view resolution)</li>
 * </ul>
 * <p>
 * View resolution may introduce new nodes that need further processing, so explicit recursive calls are made on newly resolved view
 * plans. The traversal tracks circular references and enforces the maximum view depth ({@link #MAX_VIEW_DEPTH_SETTING}).
 * <p>
 * TODO: {@code ViewResolver} needs rename or refactor, as it does two tasks - view resolution and IN subquery resolution. Keep the core
 *  of view resolution in {@code ViewResolver}, and have a {@code ViewAndSubqueryResolver} drive the plan tree traversal, call
 *  {@code ViewResolver} and {@code InSubqueryResolver} to do the view and IN subquery resolution respectively.
 */
public class ViewResolver {

    protected Logger log = LogManager.getLogger(getClass());
    private final Executor executor;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private volatile int maxViewDepth;
    private final Client client;
    // This setting is registered as OperatorDynamic so it is not exposed to end users yet.
    // To fully expose it later:
    // 1. Change OperatorDynamic to Dynamic (makes it user-settable on self-managed)
    // 2. Add ServerlessPublic (makes it visible to non-operator users on Serverless)
    public static final Setting<Integer> MAX_VIEW_DEPTH_SETTING = Setting.intSetting(
        "esql.views.max_view_depth",
        10,
        0,
        100,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    /**
     * Public constructor for NOOP instance (in release mode, when component is not registered, but TransportEsqlQueryAction still needs it)
     */
    public ViewResolver() {
        this.executor = null;
        this.clusterService = null;
        this.projectResolver = null;
        this.crossProjectModeDecider = CrossProjectModeDecider.NOOP;
        this.maxViewDepth = 0;
        this.client = null;
    }

    public ViewResolver(
        ThreadPool threadPool,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Client client,
        CrossProjectModeDecider crossProjectModeDecider
    ) {
        this.executor = threadPool != null ? threadPool.executor(ThreadPool.Names.SEARCH) : null;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.crossProjectModeDecider = crossProjectModeDecider;
        this.client = client;
        clusterService.getClusterSettings().initializeAndWatch(MAX_VIEW_DEPTH_SETTING, v -> this.maxViewDepth = v);
    }

    ViewMetadata getMetadata() {
        return projectResolver.getProjectMetadata(clusterService.state()).custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
    }

    // TODO: Remove this function entirely if we no longer need to do micro-benchmarks on views enabled/disabled
    protected boolean viewsFeatureEnabled() {
        return true;
    }

    /**
     * Result of view resolution containing the rewritten plan, the view queries, and whether any {@code InSubquery} expression was
     * rewritten into a {@code SemiJoin}/{@code AntiJoin}/{@code MarkJoin} during resolution.
     * <p>
     * {@code hasInSubquery} drives the {@code IN_SUBQUERY} telemetry counter (see {@code EsqlSession#gatherInSubqueryMetrics}).
     */
    public record ViewResolutionResult(LogicalPlan plan, Map<String, String> viewQueries, boolean hasInSubquery) {}

    /**
     * Entry point for view + IN subquery resolution; see the {@link ViewResolver} class documentation for the traversal model and the
     * node types it intercepts. Produces a {@link ViewResolutionResult} containing the rewritten (uncompacted) plan, the map of
     * resolved view names to their queries, and — via {@link ViewResolutionResult#hasInSubquery()} — whether any IN subquery was
     * rewritten during resolution.
     *
     * @param plan the logical plan to process
     * @param parser function to parse view query strings into logical plans
     * @param listener callback that receives the {@link ViewResolutionResult}
     */
    public void replaceViews(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        ActionListener<ViewResolutionResult> listener
    ) {
        Map<String, String> viewQueries = new HashMap<>();
        // Set when the traversal below rewrites an InSubquery (in the query or in a view body) into a Semi/Anti/MarkJoin; reported in
        // the result so the session can count IN_SUBQUERY telemetry even for IN subqueries that only exist inside a view definition.
        // A Holder (not a plain boolean) because it is written from inside the async resolution callbacks; cross-thread visibility is
        // provided by the ActionListener plumbing, the same way the viewQueries map threaded through these callbacks is.
        Holder<Boolean> hasInSubquery = new Holder<>(false);
        boolean noViews = viewsFeatureEnabled() == false || getMetadata().views().isEmpty();
        if (noViews && InSubqueryResolver.hasInSubqueryInFilter(plan) == false) {
            listener.onResponse(new ViewResolutionResult(plan, viewQueries, false));
            return;
        }
        // Note: this returns the uncompacted nested plan. Compaction (UnionAll/ViewUnionAll
        // rewriting, sibling UnresolvedRelation merging, NamedSubquery unwrapping) now lives in
        // {@link org.elasticsearch.xpack.esql.view.ViewCompaction} and is applied by EsqlSession
        // between view resolution and pre-analysis, so PreAnalyzer extracts the same index
        // patterns that the analyzer's ResolveTable will later look up. Keeping the resolver's
        // output uncompacted is the foundation for the CPS lenient-field-caps work in
        // esql-planning #543, #472.
        replaceViews(
            plan,
            projectRouting,
            parser,
            new LinkedHashSet<>(),
            viewQueries,
            hasInSubquery,
            0,
            listener.delegateFailureAndWrap(
                (l, rewritten) -> l.onResponse(new ViewResolutionResult(rewritten, viewQueries, hasInSubquery.get()))
            )
        );
    }

    private void replaceViews(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        LinkedHashSet<String> seenViews,
        Map<String, String> viewQueries,
        Holder<Boolean> hasInSubquery,
        int depth,
        ActionListener<LogicalPlan> listener
    ) {
        LinkedHashSet<String> seenInner = new LinkedHashSet<>(seenViews);
        // Tracks wildcard patterns already resolved within this transformDown traversal to prevent duplicate processing
        HashSet<String> seenWildcards = new HashSet<>();
        // Tracks plans already resolved by view handlers (Fork, UnresolvedRelation) to prevent double-processing.
        // Without this, transformDown recurses into the children of resolved plans, causing wildcards
        // in view subqueries to be re-resolved against sibling view names, producing false circular
        // reference errors and deeply nested duplicate resolution.
        Set<LogicalPlan> resolvedPlans = Collections.newSetFromMap(new IdentityHashMap<>());

        plan.transformDown((p, planListener) -> {
            if (resolvedPlans.contains(p)) {
                // This plan was already resolved by a handler — skip it to prevent double-processing.
                planListener.onResponse(p);
                return;
            }
            switch (p) {
                case ViewUnionAll viewUnion ->
                    // ViewUnionAll is the result of view resolution, so we skip it.
                    // Plain UnionAll (from user-written subqueries) matches the Fork case below
                    // and its children are recursed into with proper seen-set scoping.
                    planListener.onResponse(viewUnion);
                case Fork fork -> replaceViewsFork(
                    fork,
                    projectRouting,
                    parser,
                    seenInner,
                    viewQueries,
                    hasInSubquery,
                    depth,
                    planListener.delegateFailureAndWrap((l, result) -> {
                        plan.forEachDown(resolvedPlans::add);
                        result.forEachDown(resolvedPlans::add);
                        l.onResponse(result);
                    })
                );
                case Filter filter -> {
                    LogicalPlan resolved = InSubqueryResolver.resolveInSubqueryInFilter(filter);
                    if (resolved == filter) {
                        // No InSubquery in this filter — let transformDown process its children normally.
                        planListener.onResponse(filter);
                    } else {
                        // InSubquery rewritten to SemiJoin/AntiJoin/MarkJoin — record it for telemetry, then resolve any view
                        // references introduced in the subquery plans.
                        hasInSubquery.set(true);
                        replaceViews(
                            resolved,
                            projectRouting,
                            parser,
                            seenInner,
                            viewQueries,
                            hasInSubquery,
                            depth,
                            planListener.delegateFailureAndWrap((l, result) -> {
                                result.forEachDown(resolvedPlans::add);
                                l.onResponse(result);
                            })
                        );
                    }
                }
                case AbstractSubqueryJoin subqueryJoin -> replaceViewsSubqueryJoin(
                    subqueryJoin,
                    projectRouting,
                    parser,
                    seenInner,
                    viewQueries,
                    hasInSubquery,
                    depth,
                    planListener.delegateFailureAndWrap((l, result) -> {
                        result.forEachDown(resolvedPlans::add);
                        l.onResponse(result);
                    })
                );
                case UnresolvedRelation ur -> replaceViewsUnresolvedRelation(
                    ur,
                    projectRouting,
                    parser,
                    seenInner,
                    seenWildcards,
                    viewQueries,
                    hasInSubquery,
                    depth,
                    planListener.delegateFailureAndWrap((l, result) -> {
                        plan.forEachDown(resolvedPlans::add);
                        // Also mark the resolved result subtree so transformDown does not
                        // re-process view-body nodes the UnresolvedRelation was replaced with.
                        result.forEachDown(resolvedPlans::add);
                        l.onResponse(result);
                    })
                );
                default -> planListener.onResponse(p);
            }
        }, listener);
    }

    private void replaceViewsFork(
        Fork fork,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        LinkedHashSet<String> seenViews,
        Map<String, String> viewQueries,
        Holder<Boolean> hasInSubquery,
        int depth,
        ActionListener<LogicalPlan> listener
    ) {
        var currentSubplans = fork.children();
        SubscribableListener<List<LogicalPlan>> chain = SubscribableListener.newForked(l -> l.onResponse(null));
        for (int i = 0; i < currentSubplans.size(); i++) {
            var index = i;
            var subplan = currentSubplans.get(i);
            chain = chain.andThen(
                (l, updatedSubplans) -> replaceViews(
                    subplan,
                    projectRouting,
                    parser,
                    seenViews,
                    viewQueries,
                    hasInSubquery,
                    depth + 1,
                    l.delegateFailureAndWrap((subListener, newPlan) -> {
                        if (newPlan instanceof Subquery sq && sq.child() instanceof NamedSubquery named) {
                            newPlan = named;
                        }
                        if (newPlan.equals(subplan) == false) {
                            var updatedSubplansInner = updatedSubplans;
                            if (updatedSubplansInner == null) {
                                updatedSubplansInner = new ArrayList<>(currentSubplans);
                            }
                            updatedSubplansInner.set(index, newPlan);
                            subListener.onResponse(updatedSubplansInner);
                        } else {
                            subListener.onResponse(updatedSubplans);
                        }
                    })
                )
            );
        }
        chain.andThenApply(updatedSubplans -> {
            if (updatedSubplans != null) {
                return fork.replaceSubPlans(updatedSubplans);
            }
            return (LogicalPlan) fork;
        }).addListener(listener);
    }

    private void replaceViewsSubqueryJoin(
        AbstractSubqueryJoin subqueryJoin,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        LinkedHashSet<String> seenViews,
        Map<String, String> viewQueries,
        Holder<Boolean> hasInSubquery,
        int depth,
        ActionListener<LogicalPlan> listener
    ) {
        LogicalPlan origLeft = subqueryJoin.left();
        LogicalPlan origRight = subqueryJoin.right();
        SubscribableListener<LogicalPlan> leftChain = SubscribableListener.newForked(
            l -> replaceViews(
                origLeft,
                projectRouting,
                parser,
                seenViews,
                viewQueries,
                hasInSubquery,
                depth + 1,
                l.delegateFailureAndWrap((sl, newLeft) -> {
                    if (newLeft instanceof Subquery sq && sq.child() instanceof NamedSubquery named) {
                        newLeft = named;
                    }
                    sl.onResponse(newLeft);
                })
            )
        );
        leftChain.<LogicalPlan>andThen(
            (l, newLeft) -> replaceViews(
                origRight,
                projectRouting,
                parser,
                seenViews,
                viewQueries,
                hasInSubquery,
                depth + 1,
                l.delegateFailureAndWrap((sl, newRight) -> {
                    if (newRight instanceof Subquery sq && sq.child() instanceof NamedSubquery named) {
                        newRight = named;
                    }
                    if (newLeft.equals(origLeft) == false || newRight.equals(origRight) == false) {
                        sl.onResponse(subqueryJoin.replaceChildren(newLeft, newRight));
                    } else {
                        sl.onResponse(subqueryJoin);
                    }
                })
            )
        ).addListener(listener);
    }

    private void replaceViewsUnresolvedRelation(
        UnresolvedRelation unresolvedRelation,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        LinkedHashSet<String> seenViews,
        HashSet<String> seenWildcards,
        Map<String, String> viewQueries,
        Holder<Boolean> hasInSubquery,
        int depth,
        ActionListener<LogicalPlan> listener
    ) {
        // Avoid re-resolving wildcards preserved for non-view matches in subsequent transformDown visits.
        var patterns = Arrays.stream(unresolvedRelation.indexPattern().indexPattern().split(","))
            .filter(pattern -> Regex.isSimpleMatchPattern(pattern) == false || seenWildcards.contains(pattern) == false)
            .toArray(String[]::new);
        if (patterns.length == 0) {
            // All patterns are wildcards already resolved in this scope. Returning without a
            // request is a no-op AND avoids the security layer's empty-indices → "_all"
            // normalization, which would otherwise re-expand to the full cluster lookup and
            // leak "_all" as a literal pattern into downstream merge/concat code.
            listener.onResponse(unresolvedRelation);
            return;
        }
        for (String pattern : patterns) {
            if (Regex.isSimpleMatchPattern(pattern)) {
                seenWildcards.add(pattern);
            }
        }

        // ViewShadowRelation siblings are only emitted in CPS mode — they exist solely to drive a
        // per-level lenient field-caps lookup against linked projects (esql-planning #543). In
        // non-CPS mode the shadow has no consumer, so we skip the bookkeeping entirely; the rest of
        // the resolver behaves as if shadows are simply not part of the tree.
        boolean cpsEnabled = crossProjectModeDecider.crossProjectEnabled();
        String[] urPatterns = unresolvedRelation.indexPattern().indexPattern().split(",");

        var req = new EsqlResolveViewAction.Request(REST_MASTER_TIMEOUT_DEFAULT, cpsEnabled);
        req.setProjectRouting(projectRouting);
        req.indices(patterns);

        doEsqlResolveViewsRequest(req, listener.delegateFailureAndWrap((l1, response) -> {
            if (response.views().length == 0) {
                listener.onResponse(
                    crossProjectModeDecider.crossProjectEnabled()
                        ? unresolvedRelation
                        : stripValidConcreteViewExclusions(unresolvedRelation, patterns)
                );
                return;
            }

            final HashMap<String, ViewPlan> resolvedViews = new HashMap<>();
            final HashMap<String, ViewShadowRelation> viewShadows = new HashMap<>();
            final LinkedHashSet<String> ancestorViews = new LinkedHashSet<>(seenViews);
            SubscribableListener<Void> chain = SubscribableListener.newForked(l2 -> l2.onResponse(null));
            for (var view : response.views()) {
                chain = chain.andThen(l2 -> {
                    // Make sure we don't block sibling branches from containing the same views
                    LinkedHashSet<String> branchSeenViews = new LinkedHashSet<>(ancestorViews);
                    validateViewReferenceAndMarkSeen(view.name(), branchSeenViews);
                    if (cpsEnabled) {
                        // find pattern referencing current view
                        var patternPosition = findMatchingPattern(view.name(), urPatterns, response);
                        assert patternPosition >= 0 : "Pattern must be found";
                        // cluster alias : index pattern
                        var clusterAndPattern = RemoteClusterAware.splitIndexName(urPatterns[patternPosition]);
                        var isConcreteExpression = clusterAndPattern.indexExpression().contains("*") == false;
                        if (isConcreteExpression) {
                            var isFlat = clusterAndPattern.clusterAlias() == null;
                            var isRequiredOnEveryProject = clusterAndPattern.clusterAlias() != null
                                && clusterAndPattern.clusterAlias().contains("*");
                            if (isFlat) {
                                var pattern = new ArrayList<String>();
                                pattern.add(view.name());
                                pattern.addAll(collectExclusionsAfterPosition(patternPosition, urPatterns));
                                viewShadows.putIfAbsent(
                                    view.name(),
                                    new ViewShadowRelation(
                                        unresolvedRelation.source(),
                                        view.name(),
                                        LinkedIndexPattern.Kind.OPTIONAL,
                                        String.join(",", pattern)
                                    )
                                );
                            } else if (isRequiredOnEveryProject) {
                                var pattern = new ArrayList<String>();
                                pattern.add(urPatterns[patternPosition]);
                                pattern.add("-_origin:*");
                                pattern.addAll(collectExclusionsAfterPosition(patternPosition, urPatterns));
                                viewShadows.putIfAbsent(
                                    view.name(),
                                    new ViewShadowRelation(
                                        unresolvedRelation.source(),
                                        view.name(),
                                        LinkedIndexPattern.Kind.REQUIRED,
                                        String.join(",", pattern)
                                    )
                                );
                            }
                        }
                    }
                    replaceViews(
                        resolve(view, parser, viewQueries),
                        projectRouting,
                        parser,
                        branchSeenViews,
                        viewQueries,
                        hasInSubquery,
                        depth + 1,
                        l2.delegateFailureAndWrap((l3, fullyResolved) -> {
                            ViewPlan viewPlan = new ViewPlan(view.name(), fullyResolved);
                            resolvedViews.put(view.name(), viewPlan);
                            l3.onResponse(null);
                        })
                    );
                });
            }
            chain.andThenApply(ignored -> {
                List<ViewPlan> subqueries = buildOrderedSubqueries(unresolvedRelation, response, resolvedViews, patterns);
                if (cpsEnabled) {
                    // Append the per-resolved-view ViewShadowRelations as additional siblings at
                    // this same level. They live under suffixed names so they don't collide with
                    // the strict ViewPlan keys when the LinkedHashMap is built downstream.
                    for (var view : response.views()) {
                        ViewShadowRelation shadow = viewShadows.get(view.name());
                        if (shadow != null) {
                            subqueries.add(new ViewPlan(view.name() + "#shadow", shadow));
                        }
                    }
                }
                if (subqueries.size() == 1) {
                    return subqueries.getFirst().plan();
                }
                return buildPlanFromBranches(unresolvedRelation, subqueries, depth);
            }).addListener(listener);
        }));
    }

    /**
     * Finds a position of the pattern that resolved to the given view.
     */
    private static int findMatchingPattern(String viewName, String[] patterns, EsqlResolveViewAction.Response response) {
        for (int p = 0; p < patterns.length; p++) {
            String pattern = patterns[p];
            for (var expression : response.getResolvedIndexExpressions().expressions()) {
                // find resolved expression for the current pattern that resolves to the given view name
                if (Objects.equals(pattern, expression.original()) && expression.localExpressions().indices().contains(viewName)) {
                    return p;
                }
            }
        }
        return -1;
    }

    private static List<String> collectExclusionsAfterPosition(int position, String[] patterns) {
        var exclusions = new ArrayList<String>();
        for (int p = position + 1; p < patterns.length; p++) {
            String pattern = patterns[p];
            if (patternIsExclusion(pattern)) {
                exclusions.add(pattern);
            }
        }
        return exclusions;
    }

    /**
     * Builds an ordered list of subqueries by iterating the resolved index expressions in order.
     * Each expression is classified as either a view (added as a ViewPlan) or a concrete index
     * (accumulated into a single UnresolvedRelation).
     */
    private List<ViewPlan> buildOrderedSubqueries(
        UnresolvedRelation unresolvedRelation,
        EsqlResolveViewAction.Response response,
        HashMap<String, ViewPlan> resolvedViews,
        String[] originalPatterns
    ) {
        List<ViewPlan> result = new ArrayList<>();
        HashSet<String> addedViews = new HashSet<>();
        // Positive patterns that must remain in the unresolved UnresolvedRelation because they contribute non-view
        // resources or were not visible / unauthorized. We collect them here during the first pass
        // but emit them into unresolvedPatterns in original-query order during the second pass,
        // so exclusion patterns stay at their original positions — reordering them changes the
        // semantics of {@link IndexAbstractionResolver}, which applies exclusions against state
        // accumulated so far.
        HashSet<String> patternsNeedingUnresolved = new HashSet<>();
        int unresolvedInsertPos = -1;

        for (var expr : response.getResolvedIndexExpressions().expressions()) {
            var localResult = expr.localExpressions().localIndexResolutionResult();

            // Unauthorized or not visible resources pass through to field caps
            if (localResult == ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                || localResult == ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED) {
                if (unresolvedInsertPos < 0) {
                    unresolvedInsertPos = result.size();
                }
                patternsNeedingUnresolved.add(expr.original());
                continue;
            }

            // Classify each resolved index as view or non-view
            boolean hasNonView = false;
            List<ViewPlan> exprViews = new ArrayList<>();
            for (String index : expr.localExpressions().indices()) {
                ViewPlan vp = resolvedViews.get(index);
                if (vp != null) {
                    if (addedViews.add(index)) {
                        exprViews.add(vp);
                    }
                } else {
                    hasNonView = true;
                }
            }

            // Add view plans first, then record unresolved position so that resolved view
            // indexes precede the original wildcard pattern in the final merged result.
            // Sort so simple UnresolvedRelation plans come before complex plans — this ensures deterministic
            // ordering regardless of HashSet iteration order in the expression's indices.
            exprViews.sort((a, b) -> {
                boolean aSimple = a.plan() instanceof UnresolvedRelation;
                boolean bSimple = b.plan() instanceof UnresolvedRelation;
                return Boolean.compare(bSimple, aSimple);
            });
            result.addAll(exprViews);

            // Non-view indices or CPS index expression wildcards pass through as unresolved
            var localIndexExpression = RemoteClusterAware.splitIndexName(expr.original()).indexExpression();
            if (hasNonView || (crossProjectModeDecider.crossProjectEnabled() && Regex.isSimpleMatchPattern(localIndexExpression))) {
                if (unresolvedInsertPos < 0) {
                    unresolvedInsertPos = result.size();
                }
                patternsNeedingUnresolved.add(expr.original());
            }
        }

        // Second pass: build unresolvedPatterns from originalPatterns in original order, keeping
        // positive patterns flagged above and exclusions that don't target concrete views.
        List<String> unresolvedPatterns = new ArrayList<>();
        var viewNames = getMetadata().views();
        for (String pattern : originalPatterns) {
            if (patternIsExclusion(pattern)) {
                if (isConcreteViewExclusion(pattern, viewNames::containsKey) == false) {
                    unresolvedPatterns.add(pattern);
                }
            } else if (patternsNeedingUnresolved.contains(pattern)) {
                unresolvedPatterns.add(pattern);
            }
        }

        // Only emit the UnresolvedRelation plan if it would contribute at least one positive pattern.
        // An UnresolvedRelation with only exclusions has no positive basis to match against and
        // would be a semantically empty input — preserve prior behavior of omitting it in that case.
        if (patternsNeedingUnresolved.isEmpty() == false) {
            result.add(unresolvedInsertPos, createUnresolvedRelationPlan(unresolvedRelation, unresolvedPatterns));
        }

        return result;
    }

    private void validateViewReferenceAndMarkSeen(String viewName, LinkedHashSet<String> seenViews) {
        if (seenViews.add(viewName) == false) {
            throw new VerificationException("circular view reference '" + viewName + "': " + String.join(" -> ", seenViews));
        }
        if (seenViews.size() > this.maxViewDepth) {
            throw new VerificationException(
                "The maximum allowed view depth of " + this.maxViewDepth + " has been exceeded: " + String.join(" -> ", seenViews)
            );
        }
    }

    /**
     * Checks whether a pattern is a <em>local</em> exclusion targeting a concrete (non-wildcard)
     * view name — e.g. {@code -my_view}. Cluster-prefixed forms ({@code cluster:-my_view},
     * {@code *:-my_view}) and cluster-level exclusions ({@code -cluster:*}) do not target a
     * local concrete view name and are excluded here, even though they pass
     * {@link #patternIsExclusion}. View names cannot contain {@code :} (validated at create
     * time), so a colon in {@code pattern} indicates a cluster scope rather than a view target.
     */
    private static boolean isConcreteViewExclusion(String pattern, Predicate<String> viewExistsPredicate) {
        if (pattern.startsWith("-") == false || pattern.contains(":")) {
            return false;
        }
        String target = pattern.substring(1);
        return Regex.isSimpleMatchPattern(target) == false && viewExistsPredicate.test(target);
    }

    /**
     * True iff {@code pattern} is any field-caps exclusion form:
     * <ul>
     *   <li>{@code -name} — local exclusion;</li>
     *   <li>{@code cluster:-name} / {@code *:-name} — cluster-prefixed exclusion (the local
     *       part starts with {@code -});</li>
     *   <li>{@code -cluster:*} — cluster-level exclusion (whole cluster removed).</li>
     * </ul>
     * The cluster-prefixed forms in particular are essential for view-shadow exclusion
     * propagation: a query like {@code FROM my_view, cluster:-my_view} must attach
     * {@code cluster:-my_view} to {@code my_view}'s shadow so the lenient field-caps target
     * mirrors the local exclusion scope. Detecting only the {@code -name} form would silently
     * drop the cluster scope and the lenient lookup would erroneously include the excluded
     * remote.
     */
    private static boolean patternIsExclusion(String pattern) {
        if (pattern.startsWith("-")) {
            return true;
        }
        var split = RemoteClusterAware.splitIndexName(pattern);
        return split.clusterAlias() != null && split.indexExpression().startsWith("-");
    }

    /**
     * Returns a copy of the unresolved relation with concrete view exclusions removed from its pattern.
     * Used in the early return path when no views were resolved, to prevent valid view exclusions from
     * reaching field caps where they would fail.
     */
    private UnresolvedRelation stripValidConcreteViewExclusions(UnresolvedRelation ur, String[] patterns) {
        var viewNames = getMetadata().views();
        var filtered = Arrays.stream(patterns)
            .filter(p -> isConcreteViewExclusion(p, viewNames::containsKey) == false)
            .toArray(String[]::new);
        if (filtered.length == patterns.length) {
            return ur;
        }
        return new UnresolvedRelation(
            ur.source(),
            new IndexPattern(ur.indexPattern().source(), String.join(",", filtered)),
            ur.frozen(),
            ur.metadataFields(),
            ur.indexMode(),
            ur.unresolvedMessage()
        );
    }

    private ViewPlan createUnresolvedRelationPlan(UnresolvedRelation ur, List<String> unresolvedPatterns) {
        return new ViewPlan(
            null,
            new UnresolvedRelation(
                ur.source(),
                new IndexPattern(ur.indexPattern().source(), String.join(",", unresolvedPatterns)),
                ur.frozen(),
                ur.metadataFields(),
                ur.indexMode(),
                ur.unresolvedMessage()
            )
        );
    }

    // Visible for testing
    protected void doEsqlResolveViewsRequest(
        EsqlResolveViewAction.Request request,
        ActionListener<EsqlResolveViewAction.Response> listener
    ) {
        client.execute(EsqlResolveViewAction.TYPE, request, new ThreadedActionListener<>(executor, listener));
    }

    protected record OriginViewsResolution(boolean resolveLocalViews, @Nullable String originProjectAlias) {}

    record ViewPlan(String name, LogicalPlan plan) {}

    private LogicalPlan buildPlanFromBranches(UnresolvedRelation ur, List<ViewPlan> subqueries, int depth) {
        // Pass 1: Build all branches as named entries.
        LinkedHashMap<String, LogicalPlan> plans = new LinkedHashMap<>();
        for (ViewPlan vp : subqueries) {
            String key = makeUniqueKey(plans, vp.name);
            if (vp.plan instanceof NamedSubquery ns) {
                assertNamesMatch("Unexpected subquery name mismatch", ns.name(), vp.name);
                plans.put(key, ns);
            } else if (vp.plan instanceof UnresolvedRelation urp && urp.indexMode() == IndexMode.STANDARD) {
                plans.put(key, urp);
            } else if (vp.plan instanceof ViewShadowRelation) {
                // Leave ViewShadowRelation bare — Phase A's ViewCompaction strip recognises it by
                // type and removes it directly. Wrapping in NamedSubquery would hide it from the
                // strip and keep its name out of the dropped-entries' keyspace.
                plans.put(key, vp.plan);
            } else {
                plans.put(key, new NamedSubquery(ur.source(), vp.plan, key));
            }
        }

        // Pass 2: Try to merge bare UnresolvedRelations that don't share index patterns. Most of the
        // compaction work has moved to the {@link ViewCompaction} analyzer rule, but a per-level merge
        // here keeps the resolved plan compact: a wide branching level (e.g. {@code FROM v1, v2, ... v9}
        // of compactable views) folds into a single {@link UnresolvedRelation} entry rather than a
        // ViewUnionAll that would later trip {@link Fork#MAX_BRANCHES} at post-analysis verification.
        mergeCompatibleUnresolvedRelations(plans);

        if (plans.size() == 1) {
            return plans.values().iterator().next();
        }
        traceUnionAllBranches(depth, plans);
        return new ViewUnionAll(ur.source(), plans, List.of());
    }

    /**
     * Merges bare UnresolvedRelation entries that don't share index patterns into a single entry.
     * Those that cannot be merged are wrapped in NamedSubquery nodes to preserve data duplication
     * semantics. The full broader-scope compaction lives in {@link ViewCompaction}; this is the
     * per-level merge that keeps the resolved tree small enough to pass {@link Fork#MAX_BRANCHES}
     * at post-analysis verification.
     */
    private static void mergeCompatibleUnresolvedRelations(LinkedHashMap<String, LogicalPlan> plans) {
        List<String> urKeys = new ArrayList<>();
        for (Map.Entry<String, LogicalPlan> entry : plans.entrySet()) {
            if (entry.getValue() instanceof UnresolvedRelation ur && ur.indexMode() == IndexMode.STANDARD) {
                urKeys.add(entry.getKey());
            }
        }
        if (urKeys.size() <= 1) {
            return;
        }

        String firstKey = urKeys.getFirst();
        UnresolvedRelation merged = (UnresolvedRelation) plans.get(firstKey);

        for (int i = 1; i < urKeys.size(); i++) {
            String key = urKeys.get(i);
            UnresolvedRelation ur = (UnresolvedRelation) plans.get(key);
            UnresolvedRelation result = mergeIfPossible(merged, ur);
            if (result != null) {
                merged = result;
                plans.remove(key);
            } else {
                // Cannot merge — wrap in NamedSubquery to preserve independence
                plans.put(key, new NamedSubquery(ur.source(), ur, key));
            }
        }
        plans.put(firstKey, merged);
    }

    private static UnresolvedRelation mergeIfPossible(UnresolvedRelation main, UnresolvedRelation other) {
        return ViewCompaction.mergeIfPossible(main, other);
    }

    private static void assertNamesMatch(String message, String left, String right) {
        if (left.equals(right) == false) {
            throw new IllegalStateException(message + ": " + left + " != " + right);
        }
    }

    /**
     * Generate a unique key for the plans map, avoiding collisions with existing entries.
     */
    private static String makeUniqueKey(LinkedHashMap<String, LogicalPlan> plans, String key) {
        if (key == null) {
            key = "main";
        }
        String original = key;
        int counter = 2;
        while (plans.containsKey(key)) {
            key = original + "#" + counter++;
        }
        return key;
    }

    private void traceUnionAllBranches(int depth, Map<String, LogicalPlan> plans) {
        if (log.isTraceEnabled() == false) {
            return;
        }
        String tab = "    ".repeat(depth);
        log.trace("{}  creating UnionAll with {} branches:", tab, plans.size());
        String branchPrefix = "      " + tab;
        for (Map.Entry<String, LogicalPlan> entry : plans.entrySet()) {
            String name = entry.getKey();
            LogicalPlan p = entry.getValue();
            log.trace("{}    branch plan[{}]=\n{}{}", tab, branchPrefix, name, p.toString().replace("\n", "\n" + branchPrefix));
        }
    }

    private LogicalPlan resolve(View view, BiFunction<String, String, LogicalPlan> parser, Map<String, String> viewQueries) {
        log.debug("Resolving view '{}'", view.name());
        // Store the view query so it can be used during Source deserialization
        viewQueries.put(view.name(), view.query());

        // Parse the view query with the view name, which causes all Source objects
        // to be tagged with the view name during parsing
        LogicalPlan subquery = parser.apply(view.query(), view.name());
        if (subquery instanceof UnresolvedRelation ur && containsExclusion(ur) == false) {
            // Simple UnresolvedRelation subqueries are not kept as views, so we can compact them
            // together and avoid branched plans. But exclusion patterns must stay scoped to the
            // view body — a bare UnresolvedRelation with an exclusion that gets merged with sibling
            // or outer UnresolvedRelations would have its exclusion's scope widened across the
            // merged pattern list (see #146XXX), so those are wrapped in a NamedSubquery via the
            // else branch to prevent merging.
            return ur;
        } else {
            // More complex subqueries (or simple UnresolvedRelations containing exclusions) are
            // maintained with the view name for branch identification.
            return new NamedSubquery(subquery.source(), subquery, view.name());
        }
    }

    private static boolean containsExclusion(UnresolvedRelation ur) {
        for (String pattern : ur.indexPattern().indexPattern().split(",")) {
            if (patternIsExclusion(pattern)) {
                return true;
            }
        }
        return false;
    }

}
