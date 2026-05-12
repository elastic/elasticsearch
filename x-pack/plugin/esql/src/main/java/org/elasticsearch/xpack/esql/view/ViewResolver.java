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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

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
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

public class ViewResolver {

    protected Logger log = LogManager.getLogger(getClass());
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
        this.clusterService = null;
        this.projectResolver = null;
        this.crossProjectModeDecider = CrossProjectModeDecider.NOOP;
        this.maxViewDepth = 0;
        this.client = null;
    }

    public ViewResolver(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Client client,
        CrossProjectModeDecider crossProjectModeDecider
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.crossProjectModeDecider = crossProjectModeDecider;
        this.client = client;
        clusterService.getClusterSettings().initializeAndWatch(MAX_VIEW_DEPTH_SETTING, v -> this.maxViewDepth = v);
    }

    ViewMetadata getMetadata() {
        return clusterService.state().metadata().getProject(projectResolver.getProjectId()).custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
    }

    // TODO: Remove this function entirely if we no longer need to do micro-benchmarks on views enabled/disabled
    protected boolean viewsFeatureEnabled() {
        return true;
    }

    /**
     * Result of view resolution containing both the rewritten plan and the view queries.
     */
    public record ViewResolutionResult(LogicalPlan plan, Map<String, String> viewQueries) {}

    /**
     * Replaces views in the logical plan with their subqueries recursively.
     * <p>
     * This method performs a depth-first, top-down (pre-order) traversal of the plan tree.
     * During traversal, it intercepts specific node types:
     * <ul>
     *   <li>{@code UnresolvedRelation}: Resolves views and replaces them with their query plans,
     *       then recursively processes those plans</li>
     *   <li>{@code Fork}: Recursively processes each child branch</li>
     *   <li>{@code UnionAll}: Skipped (assumes rewriting is already complete)</li>
     * </ul>
     * <p>
     * View resolution may introduce new nodes that need further processing, so explicit
     * recursive calls are made on newly resolved view plans. The method tracks circular
     * references and enforces maximum view depth limits.
     *
     * @param plan the logical plan to process
     * @param parser function to parse view query strings into logical plans
     * @param listener callback that receives the rewritten plan and a map of view names to their queries
     */
    public void replaceViews(
        LogicalPlan plan,
        BiFunction<String, String, LogicalPlan> parser,
        ActionListener<ViewResolutionResult> listener
    ) {
        Map<String, String> viewQueries = new HashMap<>();
        if (viewsFeatureEnabled() == false || getMetadata().views().isEmpty()) {
            listener.onResponse(new ViewResolutionResult(plan, viewQueries));
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
            parser,
            new LinkedHashSet<>(),
            viewQueries,
            0,
            listener.delegateFailureAndWrap((l, rewritten) -> l.onResponse(new ViewResolutionResult(rewritten, viewQueries)))
        );
    }

    private void replaceViews(
        LogicalPlan plan,
        BiFunction<String, String, LogicalPlan> parser,
        LinkedHashSet<String> seenViews,
        Map<String, String> viewQueries,
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
                    parser,
                    seenInner,
                    viewQueries,
                    depth,
                    planListener.delegateFailureAndWrap((l, result) -> {
                        plan.forEachDown(resolvedPlans::add);
                        result.forEachDown(resolvedPlans::add);
                        l.onResponse(result);
                    })
                );
                case UnresolvedRelation ur -> replaceViewsUnresolvedRelation(
                    ur,
                    parser,
                    seenInner,
                    seenWildcards,
                    viewQueries,
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
        BiFunction<String, String, LogicalPlan> parser,
        LinkedHashSet<String> seenViews,
        Map<String, String> viewQueries,
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
                    parser,
                    seenViews,
                    viewQueries,
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

    private void replaceViewsUnresolvedRelation(
        UnresolvedRelation unresolvedRelation,
        BiFunction<String, String, LogicalPlan> parser,
        LinkedHashSet<String> seenViews,
        HashSet<String> seenWildcards,
        Map<String, String> viewQueries,
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

        // For each position in the parent UnresolvedRelation's pattern list, the exclusions that
        // appear strictly after it. Used to attach position-aware exclusions to each
        // {@link ViewShadowRelation} so the lenient field-caps target mirrors the local exclusion
        // scope exactly. Index resolution is left-to-right: an exclusion only narrows what's
        // already been accumulated, so a view referenced at position i is only affected by
        // exclusions at positions > i. See esql-planning #543.
        String[] urPatterns = unresolvedRelation.indexPattern().indexPattern().split(",");
        List<List<String>> exclusionsAfter = cpsEnabled ? computeExclusionsAfterByPosition(urPatterns) : List.of();

        var req = new EsqlResolveViewAction.Request(REST_MASTER_TIMEOUT_DEFAULT);
        req.indices(patterns);

        doEsqlResolveViewsRequest(req, listener.delegateFailureAndWrap((l1, response) -> {
            if (response.views().length == 0) {
                listener.onResponse(stripValidConcreteViewExclusions(unresolvedRelation, patterns));
                return;
            }

            // Map each resolved view name to the earliest position in urPatterns at which it was
            // matched (broadest applicable-exclusion set). Earliest-position wins so we don't drop
            // exclusions that the user wrote after a wildcard match of the same view. Only used
            // for shadow exclusion attribution, so we skip the work entirely outside CPS.
            Map<String, Integer> viewToEarliestPosition = cpsEnabled ? computeViewToEarliestPosition(urPatterns, response) : Map.of();

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
                        // Build the per-view {@link ViewShadowRelation} once, alongside the resolved
                        // body. Lives at the same plan-tree level as the strict resolution so the
                        // post-resolution rule can find the pair structurally.
                        Integer pos = viewToEarliestPosition.get(view.name());
                        List<String> applicableExclusions = (pos != null) ? exclusionsAfter.get(pos) : List.of();
                        viewShadows.putIfAbsent(
                            view.name(),
                            new ViewShadowRelation(unresolvedRelation.source(), view.name(), applicableExclusions)
                        );
                    }
                    replaceViews(
                        resolve(view, parser, viewQueries),
                        parser,
                        branchSeenViews,
                        viewQueries,
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
     * For each position {@code i} in {@code urPatterns}, computes the list of exclusion patterns at
     * positions {@code j > i}, preserving original order. Used to attach position-aware exclusions
     * to each {@link ViewShadowRelation}.
     */
    private static List<List<String>> computeExclusionsAfterByPosition(String[] urPatterns) {
        List<List<String>> exclusionsAfter = new ArrayList<>(urPatterns.length);
        List<String> later = new ArrayList<>();
        for (int i = urPatterns.length - 1; i >= 0; i--) {
            // Snapshot what's accumulated so far before potentially adding the current pattern.
            exclusionsAfter.add(0, List.copyOf(later));
            if (patternIsExclusion(urPatterns[i])) {
                later.add(0, urPatterns[i]);
            }
        }
        return exclusionsAfter;
    }

    /**
     * Maps each resolved view name to the earliest position in {@code urPatterns} at which it was
     * matched. When a view appears at multiple positions (e.g. matched both by a wildcard pattern
     * earlier in the list and by an explicit name later), earliest wins, giving the broadest set of
     * later exclusions — the most conservative reading for the lenient lookup.
     */
    private static Map<String, Integer> computeViewToEarliestPosition(String[] urPatterns, EsqlResolveViewAction.Response response) {
        Set<String> resolvedViewNames = new HashSet<>();
        for (var view : response.views()) {
            resolvedViewNames.add(view.name());
        }
        Map<String, Integer> viewToEarliestPosition = new HashMap<>();
        for (var expr : response.getResolvedIndexExpressions().expressions()) {
            int position = -1;
            for (int i = 0; i < urPatterns.length; i++) {
                if (urPatterns[i].equals(expr.original())) {
                    position = i;
                    break;
                }
            }
            if (position < 0) {
                continue;
            }
            for (String index : expr.localExpressions().indices()) {
                if (resolvedViewNames.contains(index)) {
                    viewToEarliestPosition.merge(index, position, Math::min);
                }
            }
        }
        return viewToEarliestPosition;
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

            // Non-view indices or CPS wildcards pass through as unresolved
            if (hasNonView || (crossProjectModeDecider.crossProjectEnabled() && Regex.isSimpleMatchPattern(expr.original()))) {
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
        String[] split = RemoteClusterAware.splitIndexName(pattern);
        return split[0] != null && split[1].startsWith("-");
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
        client.execute(EsqlResolveViewAction.TYPE, request, listener);
    }

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

    /** Merge the unresolved relation unless the index patterns contain matching index names. */
    private static UnresolvedRelation mergeIfPossible(UnresolvedRelation main, UnresolvedRelation other) {
        for (String mainPattern : main.indexPattern().indexPattern().split(",")) {
            for (String otherPattern : other.indexPattern().indexPattern().split(",")) {
                if (mainPattern.equals(otherPattern)) {
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
