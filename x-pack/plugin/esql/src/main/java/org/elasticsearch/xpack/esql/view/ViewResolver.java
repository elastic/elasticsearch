/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
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
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.View;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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
 *   <li>{@link UnresolvedRelation}: Resolves views and replaces them with their query plans (each kept view body wrapped in a
 *       {@link View} node), then recursively processes those plans</li>
 *   <li>{@link Fork}: Recursively processes each child branch</li>
 *   <li>{@link AbstractSubqueryJoin}: Recursively processes the left and right sides</li>
 *   <li>{@link Filter}: Calls {@link InSubqueryResolver} to expand any {@code InSubquery} into a {@code SemiJoin}/{@code AntiJoin}/
 *       {@code MarkJoin}, then recurses into the newly created subquery plans to resolve view references nested there</li>
 * </ul>
 * Each handler fully resolves the subtree it returns — including recursion into view bodies — so the walk treats a handler's
 * result as terminal and never re-descends into it. There is therefore no identity bookkeeping: a {@code UnionAll} emitted for a
 * multi-source {@code FROM} is built once and not re-processed.
 * <p>
 * Self-reference policing is separated out: circular references and the maximum view depth ({@link #MAX_VIEW_DEPTH_SETTING}) are a
 * property of the static view-definition graph, not of plan position, so they are checked once up front by {@link ViewGraph} over
 * the views reachable from the query's entry relations — before this substitution walk runs. With cycles and depth already proven,
 * the walk threads no {@code seenViews} / depth / re-entry state.
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
     * node types it intercepts. Produces a {@link ViewResolutionResult} containing the rewritten plan (each {@code FROM <view>}
     * wrapped in a first-class {@code View} node, folded later by {@code InlineView}), the map of
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
        // Self-reference policing (circular references + max view depth) is a property of the static
        // view-definition graph, not of where a view appears in the plan. Run it once up front over the
        // graph reachable from this query's entry views: the check throws the verbatim circular / max-depth
        // VerificationException the eager DFS used to throw, and proves the reachable sub-graph acyclic and
        // within the depth limit. With that guarantee established, the substitution walk below recurses into
        // view bodies without threading any seen-set / depth / re-entry guards. Any rejection is delivered
        // through the listener (onFailure), matching how the eager DFS surfaced it from inside the async walk.
        if (noViews == false) {
            try {
                new ViewGraph(viewNameToQuery(), parser, p -> expandViewNames(p, projectRouting), maxViewDepth).check(
                    collectEntryViews(plan, projectRouting)
                );
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
        }
        // Note: this returns the nested plan with each FROM <view> reference wrapped in a first-class
        // View node (whose body is the resolved view query). The View boundary is later folded into its
        // body by the InlineView optimizer rule, and nested UnionAlls produced by that folding are lifted
        // by FlattenUnionAll, so the post-optimization plan carries a single flat UnionAll. Keeping the
        // resolver's output as un-inlined View nodes (rather than substituting the body in place here) is
        // the foundation for the CPS lenient-field-caps work.
        replaceViews(
            plan,
            projectRouting,
            parser,
            viewQueries,
            hasInSubquery,
            listener.delegateFailureAndWrap(
                (l, rewritten) -> l.onResponse(new ViewResolutionResult(rewritten, viewQueries, hasInSubquery.get()))
            )
        );
    }

    /** Snapshot of the current project's view definitions as a name -&gt; query map, for {@link ViewGraph}. */
    private Map<String, String> viewNameToQuery() {
        Map<String, String> result = new HashMap<>();
        getMetadata().views().forEach((name, view) -> result.put(name, view.query()));
        return result;
    }

    /**
     * Collect the query's entry view names — the views directly reachable from a top-level position — in plan
     * pre-order, descending into {@link InSubquery} subplans (a view referenced from an IN subquery body is an
     * entry the same as one referenced from a top-level {@code FROM}). Each relation's patterns are expanded once
     * to matched view names via the same {@link EsqlResolveViewAction} expansion the substitution walk uses.
     */
    private List<String> collectEntryViews(LogicalPlan plan, String projectRouting) {
        List<String> entryViews = new ArrayList<>();
        forEachViewRelation(plan, ur -> {
            List<String> patterns = Arrays.asList(ur.indexPattern().indexPattern().split(","));
            for (String name : expandViewNames(patterns, projectRouting)) {
                if (entryViews.contains(name) == false) {
                    entryViews.add(name);
                }
            }
        });
        return entryViews;
    }

    /**
     * Pre-order walk over every view-reference {@link UnresolvedRelation} in {@code plan}, descending into the subplans
     * of {@link InSubquery} expressions that sit in a {@link Filter} condition — exactly the IN subqueries the
     * substitution walk lifts into joins and resolves (others are rejected later by {@code InSubqueryResolver#verify}, so
     * the eager DFS never policed views inside them either).
     * <p>
     * This is the single source of truth for "which relations the resolver's substitution walk reaches": both the
     * up-front {@link #collectEntryViews} expansion and {@link ViewGraph}'s edge computation drive their traversal
     * through here, so the graph's edge set is, by construction, exactly the relations the resolver substitutes — the two
     * can never drift apart and re-introduce the IN-subquery-edge gap they once had.
     */
    static void forEachViewRelation(LogicalPlan plan, Consumer<UnresolvedRelation> action) {
        plan.forEachDown(p -> {
            if (p instanceof UnresolvedRelation ur) {
                action.accept(ur);
            }
            if (p instanceof Filter filter) {
                filter.condition().forEachDown(InSubquery.class, in -> forEachViewRelation(in.subquery(), action));
            }
        });
    }

    /**
     * Expand the given index patterns to the matched <b>view names</b> (exclusions applied, in resolution order),
     * synchronously, through the same {@link EsqlResolveViewAction} the substitution walk drives per relation. The
     * action runs on {@code DIRECT_EXECUTOR_SERVICE}, so a non-threaded request completes inline on the calling thread
     * — there is no fork, and {@link PlainActionFuture#actionGet()} returns without blocking.
     */
    private List<String> expandViewNames(List<String> patterns, String projectRouting) {
        if (patterns.isEmpty()) {
            return List.of();
        }
        PlainActionFuture<EsqlResolveViewAction.Response> future = new PlainActionFuture<>();
        doEsqlResolveViewsRequestSync(buildResolveViewsRequest(patterns.toArray(new String[0]), projectRouting), future);
        return Arrays.stream(future.actionGet().views()).map(org.elasticsearch.cluster.metadata.View::name).toList();
    }

    /**
     * Build the {@link EsqlResolveViewAction.Request} for a set of {@code FROM} patterns. Single source of truth so the
     * up-front {@link #expandViewNames} graph expansion and the substitution walk issue an identical request: the CPS
     * flag ({@code crossProjectModeDecider.crossProjectEnabled()}) and {@code projectRouting} MUST be set the same way on
     * both, or cross-project pattern resolution ({@code _origin:}, linked-project exclusions) and authorization diverge
     * between the graph check and the actual resolution.
     */
    private EsqlResolveViewAction.Request buildResolveViewsRequest(String[] patterns, String projectRouting) {
        var req = new EsqlResolveViewAction.Request(REST_MASTER_TIMEOUT_DEFAULT, crossProjectModeDecider.crossProjectEnabled());
        req.setProjectRouting(projectRouting);
        req.indices(patterns);
        return req;
    }

    /**
     * Explicit pre-order substitution walk. Each intercepted node type ({@link Fork}, an {@link Filter} carrying an
     * {@code InSubquery}, {@link AbstractSubqueryJoin}, {@link UnresolvedRelation}) is fully resolved by its handler —
     * including recursion into its own children / view bodies — and the handler's result is <b>terminal</b> (the walk
     * does not re-descend into it). Every other node has its children resolved in place by recursing through this same
     * method, then is rebuilt.
     * <p>
     * The walk threads no self-reference state: circular references and max depth are policed up front by
     * {@link ViewGraph}, so a view body can be recursed into freely. Because each handler fully resolves the subtree it
     * returns (no {@link UnresolvedRelation} naming a view survives), there is no re-entrant wildcard re-expansion and
     * therefore none of the {@code seenWildcards} / {@code resolvedPlans} bookkeeping the old eager DFS needed.
     */
    private void replaceViews(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        Map<String, String> viewQueries,
        Holder<Boolean> hasInSubquery,
        ActionListener<LogicalPlan> listener
    ) {
        switch (plan) {
            case Fork fork -> replaceViewsFork(fork, projectRouting, parser, viewQueries, hasInSubquery, listener);
            case Filter filter -> {
                LogicalPlan resolved = InSubqueryResolver.resolveInSubqueryInFilter(filter);
                if (resolved == filter) {
                    // No InSubquery in this filter — resolve view references in its children.
                    replaceViewsChildren(filter, projectRouting, parser, viewQueries, hasInSubquery, listener);
                } else {
                    // InSubquery rewritten to SemiJoin/AntiJoin/MarkJoin — record it for telemetry, then resolve any view
                    // references introduced in the subquery plans.
                    hasInSubquery.set(true);
                    replaceViews(resolved, projectRouting, parser, viewQueries, hasInSubquery, listener);
                }
            }
            case AbstractSubqueryJoin subqueryJoin -> replaceViewsSubqueryJoin(
                subqueryJoin,
                projectRouting,
                parser,
                viewQueries,
                hasInSubquery,
                listener
            );
            case UnresolvedRelation ur -> replaceViewsUnresolvedRelation(ur, projectRouting, parser, viewQueries, hasInSubquery, listener);
            default -> replaceViewsChildren(plan, projectRouting, parser, viewQueries, hasInSubquery, listener);
        }
    }

    /**
     * Resolve view references in {@code plan}'s children (in order, one after another), rebuilding {@code plan} only if
     * a child changed. The children are not themselves intercept nodes the handlers already recursed through; this is
     * the pre-order descent for the pass-through node types.
     */
    private void replaceViewsChildren(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        Map<String, String> viewQueries,
        Holder<Boolean> hasInSubquery,
        ActionListener<LogicalPlan> listener
    ) {
        List<LogicalPlan> children = plan.children();
        if (children.isEmpty()) {
            listener.onResponse(plan);
            return;
        }
        Holder<List<LogicalPlan>> updated = new Holder<>();
        SubscribableListener<Void> chain = SubscribableListener.newForked(l -> l.onResponse(null));
        for (int i = 0; i < children.size(); i++) {
            var index = i;
            var child = children.get(index);
            chain = chain.andThen(
                (l, ignored) -> replaceViews(
                    child,
                    projectRouting,
                    parser,
                    viewQueries,
                    hasInSubquery,
                    l.delegateFailureAndWrap((sl, newChild) -> {
                        if (newChild.equals(child) == false) {
                            if (updated.get() == null) {
                                updated.set(new ArrayList<>(children));
                            }
                            updated.get().set(index, newChild);
                        }
                        sl.onResponse(null);
                    })
                )
            );
        }
        chain.andThenApply(ignored -> updated.get() == null ? plan : plan.replaceChildren(updated.get())).addListener(listener);
    }

    private void replaceViewsFork(
        Fork fork,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        Map<String, String> viewQueries,
        Holder<Boolean> hasInSubquery,
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
                    viewQueries,
                    hasInSubquery,
                    l.delegateFailureAndWrap((subListener, newPlan) -> {
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
        Map<String, String> viewQueries,
        Holder<Boolean> hasInSubquery,
        ActionListener<LogicalPlan> listener
    ) {
        LogicalPlan origLeft = subqueryJoin.left();
        LogicalPlan origRight = subqueryJoin.right();
        SubscribableListener<LogicalPlan> leftChain = SubscribableListener.newForked(
            l -> replaceViews(
                origLeft,
                projectRouting,
                parser,
                viewQueries,
                hasInSubquery,
                l.delegateFailureAndWrap((sl, newLeft) -> sl.onResponse(newLeft))
            )
        );
        leftChain.<LogicalPlan>andThen(
            (l, newLeft) -> replaceViews(
                origRight,
                projectRouting,
                parser,
                viewQueries,
                hasInSubquery,
                l.delegateFailureAndWrap((sl, newRight) -> {
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
        Map<String, String> viewQueries,
        Holder<Boolean> hasInSubquery,
        ActionListener<LogicalPlan> listener
    ) {
        String[] patterns = unresolvedRelation.indexPattern().indexPattern().split(",");

        // Linked relations (a local view name that may also be a remote index on a linked project)
        // are only emitted in CPS mode — they drive a per-level lenient field-caps lookup against
        // linked projects. In non-CPS mode there is no linked lookup, so we skip
        // the bookkeeping entirely; the rest of the resolver behaves as if they are not part of the tree.
        boolean cpsEnabled = crossProjectModeDecider.crossProjectEnabled();
        String[] urPatterns = patterns;

        var req = buildResolveViewsRequest(patterns, projectRouting);

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
            final HashMap<String, UnresolvedRelation> linkedRelations = new HashMap<>();
            SubscribableListener<Void> chain = SubscribableListener.newForked(l2 -> l2.onResponse(null));
            for (var view : response.views()) {
                chain = chain.andThen(l2 -> {
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
                                linkedRelations.putIfAbsent(
                                    view.name(),
                                    linkedRelation(unresolvedRelation, LinkedIndexPattern.Kind.OPTIONAL, String.join(",", pattern))
                                );
                            } else if (isRequiredOnEveryProject) {
                                var pattern = new ArrayList<String>();
                                pattern.add(urPatterns[patternPosition]);
                                pattern.add("-_origin:*");
                                pattern.addAll(collectExclusionsAfterPosition(patternPosition, urPatterns));
                                linkedRelations.putIfAbsent(
                                    view.name(),
                                    linkedRelation(unresolvedRelation, LinkedIndexPattern.Kind.REQUIRED, String.join(",", pattern))
                                );
                            }
                        }
                    }
                    replaceViews(
                        resolve(view, parser, viewQueries),
                        projectRouting,
                        parser,
                        viewQueries,
                        hasInSubquery,
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
                    // Append the per-resolved-view linked relations as additional siblings at this same
                    // level. They live under suffixed names so they don't collide with the strict
                    // ViewPlan keys when branches are assembled downstream.
                    for (var view : response.views()) {
                        UnresolvedRelation linked = linkedRelations.get(view.name());
                        if (linked != null) {
                            subqueries.add(new ViewPlan(view.name() + "#linked", linked));
                        }
                    }
                }
                if (subqueries.size() == 1) {
                    return subqueries.getFirst().plan();
                }
                return buildPlanFromBranches(unresolvedRelation, subqueries);
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

    /**
     * Synchronous companion to {@link #doEsqlResolveViewsRequest}, used only by the up-front {@link ViewGraph}
     * self-reference check. The action runs on {@code DIRECT_EXECUTOR_SERVICE}, so the listener is invoked inline on the
     * calling thread — deliberately <b>not</b> wrapped in a {@link ThreadedActionListener}, so the caller can drain it
     * with a {@link PlainActionFuture} without forking off (and so without the deadlock risk of blocking a search thread
     * on another search thread). Visible for testing.
     */
    protected void doEsqlResolveViewsRequestSync(
        EsqlResolveViewAction.Request request,
        ActionListener<EsqlResolveViewAction.Response> listener
    ) {
        client.execute(EsqlResolveViewAction.TYPE, request, listener);
    }

    protected record OriginViewsResolution(boolean resolveLocalViews, @Nullable String originProjectAlias) {}

    record ViewPlan(String name, LogicalPlan plan) {}

    private LogicalPlan buildPlanFromBranches(UnresolvedRelation ur, List<ViewPlan> subqueries) {
        // Each branch node carries its own identity: a View (for a kept view body), an EsRelation-to-be
        // (a bare UnresolvedRelation for an index or a compactable single-pattern view), or a linked
        // UnresolvedRelation (CPS shadow). The View boundary is what prevents a view body from being
        // merged with a sibling index relation, so the only compaction we do here is merging bare,
        // mergeable index UnresolvedRelations — which keeps a wide branching level (e.g.
        // {@code FROM v1, v2, ... v9} of compactable single-pattern views) under {@link Fork#MAX_BRANCHES}.
        List<LogicalPlan> branches = new ArrayList<>();
        for (ViewPlan vp : subqueries) {
            branches.add(vp.plan);
        }
        mergeCompatibleUnresolvedRelations(branches);

        if (branches.size() == 1) {
            return branches.getFirst();
        }
        traceUnionAllBranches(branches);
        return new UnionAll(ur.source(), branches, List.of());
    }

    /**
     * Merges bare standard-index {@link UnresolvedRelation} branches that don't share index patterns
     * into a single branch, in place. Linked relations and {@link View} nodes are left untouched —
     * the View boundary keeps a view body from being merged into a sibling index relation. This keeps
     * the resolved tree small enough to pass {@link Fork#MAX_BRANCHES} at post-analysis verification.
     */
    private static void mergeCompatibleUnresolvedRelations(List<LogicalPlan> branches) {
        List<Integer> urIndexes = new ArrayList<>();
        for (int i = 0; i < branches.size(); i++) {
            if (branches.get(i) instanceof UnresolvedRelation ur
                && ur.linkedIndexPattern() == null
                && ur.indexMode() == IndexMode.STANDARD) {
                urIndexes.add(i);
            }
        }
        if (urIndexes.size() <= 1) {
            return;
        }

        int firstIndex = urIndexes.getFirst();
        UnresolvedRelation merged = (UnresolvedRelation) branches.get(firstIndex);
        List<Integer> toRemove = new ArrayList<>();
        for (int i = 1; i < urIndexes.size(); i++) {
            int idx = urIndexes.get(i);
            UnresolvedRelation other = (UnresolvedRelation) branches.get(idx);
            UnresolvedRelation result = mergeIfPossible(merged, other);
            if (result != null) {
                merged = result;
                toRemove.add(idx);
            }
            // If it cannot be merged it stays as its own branch.
        }
        branches.set(firstIndex, merged);
        // Remove merged-away branches from the end so earlier indexes stay valid.
        toRemove.sort((a, b) -> Integer.compare(b, a));
        for (int idx : toRemove) {
            branches.remove(idx);
        }
    }

    /** Merge the unresolved relation unless the index patterns contain matching index names. */
    private static UnresolvedRelation mergeIfPossible(UnresolvedRelation main, UnresolvedRelation other) {
        for (String mainPattern : main.indexPattern().indexPattern().split(",")) {
            for (String otherPattern : other.indexPattern().indexPattern().split(",")) {
                if (mainPattern.equals(otherPattern)) {
                    // A duplicate index name was found, fail this attempt to merge.
                    return null;
                }
                // Prevent merging when a wildcard in one pattern matches a concrete name in the other.
                // Merging would produce a single UnresolvedRelation that deduplicates the overlapping index
                // during resolution, collapsing what should be two independent data copies into one.
                if (Regex.isSimpleMatchPattern(otherPattern) == false && Regex.simpleMatch(mainPattern, otherPattern)) {
                    return null;
                }
                if (Regex.isSimpleMatchPattern(otherPattern)
                    && Regex.isSimpleMatchPattern(mainPattern) == false
                    && Regex.simpleMatch(otherPattern, mainPattern)) {
                    return null;
                }
            }
        }
        return new UnresolvedRelation(
            main.source(),
            new IndexPattern(main.indexPattern().source(), main.indexPattern().indexPattern() + "," + other.indexPattern().indexPattern()),
            main.frozen(),
            main.metadataFields(),
            main.indexMode(),
            main.unresolvedMessage()
        );
    }

    private void traceUnionAllBranches(List<LogicalPlan> branches) {
        if (log.isTraceEnabled() == false) {
            return;
        }
        log.trace("creating UnionAll with {} branches:", branches.size());
        for (int i = 0; i < branches.size(); i++) {
            LogicalPlan p = branches.get(i);
            log.trace("  branch plan[{}]=\n{}", i, p.toString());
        }
    }

    private static UnresolvedRelation linkedRelation(UnresolvedRelation source, LinkedIndexPattern.Kind kind, String pattern) {
        return new UnresolvedRelation(
            source.source(),
            new IndexPattern(source.source(), pattern),
            source.frozen(),
            source.metadataFields(),
            IndexMode.STANDARD,
            source.unresolvedMessage(),
            source.telemetryLabel(),
            new LinkedIndexPattern(kind, new IndexPattern(source.source(), pattern))
        );
    }

    private LogicalPlan resolve(
        org.elasticsearch.cluster.metadata.View view,
        BiFunction<String, String, LogicalPlan> parser,
        Map<String, String> viewQueries
    ) {
        log.debug("Resolving view '{}'", view.name());
        // Store the view query so it can be used during Source deserialization
        viewQueries.put(view.name(), view.query());

        // Parse the view query with the view name, which causes all Source objects
        // to be tagged with the view name during parsing
        LogicalPlan subquery = parser.apply(view.query(), view.name());
        if (subquery instanceof UnresolvedRelation ur && containsExclusion(ur) == false) {
            // A view whose body is a single plain index pattern with no exclusion can be compacted
            // with sibling/outer index relations and avoid branched plans. But exclusion patterns
            // must stay scoped to the view body — a bare UnresolvedRelation with an exclusion merged
            // with sibling or outer UnresolvedRelations would have its exclusion's scope widened
            // across the merged pattern list, so those go through the View branch below
            // to keep the view boundary intact.
            return ur;
        }
        // Every other view body is wrapped in a first-class View node — the boundary that survives
        // analysis (folded only by the optimizer's InlineView) and that prevents cross-view sibling
        // merge. The body becomes the View's child after recursive resolution by the caller.
        return new View(subquery.source(), view.name(), subquery);
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
