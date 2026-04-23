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
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
        replaceViews(plan, parser, new LinkedHashSet<>(), viewQueries, 0, listener.delegateFailureAndWrap((l, rewritten) -> {
            LogicalPlan postProcessed = rewriteUnionAllsWithNamedSubqueries(rewritten);
            postProcessed = compactNestedViewUnionAlls(postProcessed);
            postProcessed = postProcessed.transformDown(NamedSubquery.class, UnaryPlan::child);
            listener.onResponse(new ViewResolutionResult(postProcessed, viewQueries));
        }));
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
        // Tracks plans already resolved by view handlers (Fork, UR) to prevent double-processing.
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
                case ViewUnionAll viewUnion -> {
                    // ViewUnionAll is the result of view resolution, so we skip it.
                    // Plain UnionAll (from user-written subqueries) matches the Fork case below
                    // and its children are recursed into with proper seen-set scoping.
                    planListener.onResponse(viewUnion);
                    return;
                }
                case Fork fork -> {
                    replaceViewsFork(fork, parser, seenInner, viewQueries, depth, planListener.delegateFailureAndWrap((l, result) -> {
                        plan.forEachDown(resolvedPlans::add);
                        result.forEachDown(resolvedPlans::add);
                        l.onResponse(result);
                    }));
                    return;
                }
                case UnresolvedRelation ur -> {
                    replaceViewsUnresolvedRelation(
                        ur,
                        parser,
                        seenInner,
                        seenWildcards,
                        viewQueries,
                        depth,
                        planListener.delegateFailureAndWrap((l, result) -> {
                            plan.forEachDown(resolvedPlans::add);
                            // Also mark the resolved result subtree so transformDown does not
                            // re-process view-body nodes the UR was replaced with.
                            result.forEachDown(resolvedPlans::add);
                            l.onResponse(result);
                        })
                    );
                    return;
                }
                default -> {
                }
            }
            planListener.onResponse(p);
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

        var req = new EsqlResolveViewAction.Request(REST_MASTER_TIMEOUT_DEFAULT);
        req.indices(patterns);

        doEsqlResolveViewsRequest(req, listener.delegateFailureAndWrap((l1, response) -> {
            if (response.views().length == 0) {
                listener.onResponse(stripValidConcreteViewExclusions(unresolvedRelation, patterns));
                return;
            }

            final HashMap<String, ViewPlan> resolvedViews = new HashMap<>();
            final LinkedHashSet<String> ancestorViews = new LinkedHashSet<>(seenViews);
            SubscribableListener<Void> chain = SubscribableListener.newForked(l2 -> l2.onResponse(null));
            for (var view : response.views()) {
                chain = chain.andThen(l2 -> {
                    // Make sure we don't block sibling branches from containing the same views
                    LinkedHashSet<String> branchSeenViews = new LinkedHashSet<>(ancestorViews);
                    validateViewReferenceAndMarkSeen(view.name(), branchSeenViews);
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
                if (subqueries.size() == 1) {
                    return subqueries.getFirst().plan();
                }
                return buildPlanFromBranches(unresolvedRelation, subqueries, depth);
            }).addListener(listener);
        }));
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
        // Positive patterns that must remain in the unresolved UR because they contribute non-view
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
            // Sort so simple UR plans come before complex plans — this ensures deterministic
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

        // Only emit the UR plan if it would contribute at least one positive pattern.
        // A UR with only exclusions has no positive basis to match against and would be a
        // semantically empty input — preserve prior behavior of omitting it in that case.
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
     * Checks whether a pattern is an exclusion targeting a concrete (non-wildcard) view name.
     */
    private static boolean isConcreteViewExclusion(String pattern, Predicate<String> viewExistsPredicate) {
        if (patternIsExclusion(pattern) == false) {
            return false;
        }
        String target = pattern.substring(1);
        return Regex.isSimpleMatchPattern(target) == false && viewExistsPredicate.test(target);
    }

    private static boolean patternIsExclusion(String pattern) {
        return pattern.startsWith("-");
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
        // Pass 1: Build all branches as named entries
        LinkedHashMap<String, LogicalPlan> plans = new LinkedHashMap<>();
        for (ViewPlan vp : subqueries) {
            String key = makeUniqueKey(plans, vp.name);
            if (vp.plan instanceof NamedSubquery ns) {
                assertNamesMatch("Unexpected subquery name mismatch", ns.name(), vp.name);
                plans.put(key, ns);
            } else if (vp.plan instanceof UnresolvedRelation urp && urp.indexMode() == IndexMode.STANDARD) {
                plans.put(key, urp);
            } else {
                plans.put(key, new NamedSubquery(ur.source(), vp.plan, key));
            }
        }

        // Pass 2: Try to merge bare UnresolvedRelations that don't share index patterns
        mergeCompatibleUnresolvedRelations(plans);

        if (plans.size() == 1) {
            return plans.values().iterator().next();
        }
        traceUnionAllBranches(depth, plans);
        return new ViewUnionAll(ur.source(), plans, List.of());
    }

    /**
     * Merges bare UnresolvedRelation entries that don't share index patterns into a single entry.
     * Those that cannot be merged are wrapped in NamedSubquery nodes to preserve data duplication semantics.
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

        // Try to merge all URs with the first one
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

    /** Merge the unresolved relation unless the index patterns contain matching index names */
    private static UnresolvedRelation mergeIfPossible(UnresolvedRelation main, UnresolvedRelation other) {
        for (String mainPattern : main.indexPattern().indexPattern().split(",")) {
            for (String otherPattern : other.indexPattern().indexPattern().split(",")) {
                if (mainPattern.equals(otherPattern)) {
                    // A duplicate index name was found, fail this attempt to merge
                    // This will cause the UnresolvedRelation to remain inside a subquery
                    return null;
                }
            }
        }
        // No duplicated index names found, let's merge into a single UnresolvedRelation, reducing the branching required to execute
        return new UnresolvedRelation(
            main.source(),
            new IndexPattern(main.indexPattern().source(), main.indexPattern().indexPattern() + "," + other.indexPattern().indexPattern()),
            main.frozen(),
            main.metadataFields(),
            main.indexMode(),
            main.unresolvedMessage()
        );
    }

    /**
     * Top-down rewrite that:
     * <ol>
     *   <li>Unwraps {@code Subquery[NamedSubquery[X]]} → {@code NamedSubquery[X]}</li>
     *   <li>Converts plain {@link UnionAll} nodes containing at least one {@link NamedSubquery}
     *       child into {@link ViewUnionAll} nodes</li>
     * </ol>
     * This handles user-written {@code UNION ALL (FROM my_view)} where the parser creates a
     * {@link Subquery} wrapper and view resolution replaces its child with a {@link NamedSubquery}.
     */
    static LogicalPlan rewriteUnionAllsWithNamedSubqueries(LogicalPlan plan) {
        // Replace Subquery/NamedSubquery with just NamedSubquery
        plan = plan.transformDown(Subquery.class, sq -> sq.child() instanceof NamedSubquery n ? n : sq);

        // Any UnionAll containing at least one NamedSubquery should be replaced by ViewUnionAll
        plan = plan.transformDown(UnionAll.class, unionAll -> {
            if (unionAll instanceof ViewUnionAll) {
                return unionAll;
            }
            // Only convert if this UnionAll contains at least one NamedSubquery child
            boolean hasNamedSubqueries = unionAll.children().stream().anyMatch(c -> c instanceof NamedSubquery);
            if (hasNamedSubqueries == false) {
                return unionAll;
            }
            LinkedHashMap<String, LogicalPlan> subPlans = new LinkedHashMap<>();
            for (LogicalPlan child : unionAll.children()) {
                if (child instanceof NamedSubquery named) {
                    assertSubqueryDoesNotExist(subPlans, named.name());
                    subPlans.put(named.name(), named.child());
                } else if (child instanceof Subquery unnamed) {
                    String name = "unnamed_view_" + Integer.toHexString(unnamed.toString().hashCode());
                    assertSubqueryDoesNotExist(subPlans, name);
                    subPlans.put(name, unnamed.child());
                } else {
                    assertSubqueryDoesNotExist(subPlans, null);
                    subPlans.put(null, child);
                }
            }
            return new ViewUnionAll(unionAll.source(), subPlans, unionAll.output());
        });
        return plan;
    }

    private static void assertNamesMatch(String message, String left, String right) {
        checkAssertion(message + ": " + left + " != " + right, left.equals(right));
    }

    private static void assertSubqueryDoesNotExist(Map<String, LogicalPlan> plans, String name) {
        String message = name == null ? "Un-named subquery already exists" : "Named subquery already exists: " + name;
        checkAssertion(message, plans.containsKey(name) == false);
    }

    private static void checkAssertion(String message, boolean condition) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * Bottom-up rewrite that flattens nested {@link ViewUnionAll} structures. When a
     * {@link NamedSubquery} entry in a {@link ViewUnionAll} wraps another {@link ViewUnionAll},
     * its entries are merged into the parent: index patterns are combined and named entries are
     * lifted. Flattening is skipped when it would create duplicate named entries (e.g. when
     * sibling views share a common subview).
     */
    static LogicalPlan compactNestedViewUnionAlls(LogicalPlan plan) {
        List<LogicalPlan> children = plan.children();
        List<LogicalPlan> newChildren = null;
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            LogicalPlan newChild = compactNestedViewUnionAlls(child);
            if (newChild != child) {
                if (newChildren == null) {
                    newChildren = new ArrayList<>(children);
                }
                newChildren.set(i, newChild);
            }
        }
        LogicalPlan current = (newChildren != null) ? plan.replaceChildren(newChildren) : plan;

        if (current instanceof ViewUnionAll vua) {
            return tryFlattenViewUnionAll(vua);
        }
        return current;
    }

    private static LogicalPlan tryFlattenViewUnionAll(ViewUnionAll vua) {
        // Trial pass: collect all entries from full flattening and check for conflicts.
        // Inner ViewUnionAlls that only contain UnresolvedRelations are lifted into the parent,
        // eliminating nesting that the runtime doesn't yet support.
        // Inner Forks/UnionAlls (from user-written subqueries inside views) are also lifted,
        // with each child becoming a separate named entry suffixed from the parent view name.
        LinkedHashMap<String, LogicalPlan> flat = new LinkedHashMap<>();
        boolean hasInnerFork = false;

        // Process non-fork entries first so that all outer keys are in `flat` before we attempt
        // to flatten inner forks. This makes the conflict check order-independent —
        // without it, an inner fork processed before a later outer entry with the same key would
        // miss the conflict, producing extra branches that can exceed the Fork limit.
        List<Map.Entry<String, LogicalPlan>> forkEntries = new ArrayList<>();
        for (Map.Entry<String, LogicalPlan> entry : vua.namedSubqueries().entrySet()) {
            String key = entry.getKey();
            LogicalPlan value = entry.getValue();
            LogicalPlan inner = (value instanceof NamedSubquery ns) ? ns.child() : value;
            if (inner instanceof Fork) {
                forkEntries.add(entry);
            } else if (value instanceof UnresolvedRelation) {
                flat.put(makeUniqueKey(flat, key), value);
            } else {
                if (flat.containsKey(key)) {
                    return vua; // conflict
                }
                flat.put(key, value);
            }
        }

        for (Map.Entry<String, LogicalPlan> entry : forkEntries) {
            String parentKey = entry.getKey();
            LogicalPlan value = entry.getValue();
            LogicalPlan inner = (value instanceof NamedSubquery ns) ? ns.child() : value;
            hasInnerFork = true;
            if (inner instanceof ViewUnionAll innerVua) {
                // Named branches from inner ViewUnionAll: lift with their own names
                for (Map.Entry<String, LogicalPlan> innerEntry : innerVua.namedSubqueries().entrySet()) {
                    flat.put(makeUniqueKey(flat, innerEntry.getKey()), innerEntry.getValue());
                }
            } else {
                // Plain Fork/UnionAll from user-written subqueries: lift children with suffixed parent name
                Fork fork = (Fork) inner;
                int childIndex = 1;
                for (LogicalPlan child : fork.children()) {
                    LogicalPlan unwrapped = (child instanceof Subquery sq) ? sq.child() : child;
                    String childKey = parentKey + "#" + childIndex++;
                    flat.put(makeUniqueKey(flat, childKey), unwrapped);
                }
            }
        }

        if (hasInnerFork == false) {
            return vua;
        }

        // Try to merge all UR entries into a single one, unless there are duplicates
        mergeUnresolvedRelationEntries(flat);

        if (flat.size() > Fork.MAX_BRANCHES) {
            return vua; // flattening would exceed the branch limit, keep the nested structure
        }
        if (flat.size() == 1) {
            return flat.values().iterator().next();
        }
        return new ViewUnionAll(vua.source(), flat, vua.output());
    }

    /**
     * Generate a unique key for the flat map, avoiding collisions with existing entries.
     */
    private static String makeUniqueKey(LinkedHashMap<String, LogicalPlan> flat, String key) {
        if (key == null) {
            key = "main";
        }
        String original = key;
        int counter = 2;
        while (flat.containsKey(key)) {
            key = original + "#" + counter++;
        }
        return key;
    }

    /**
     * Merges bare UnresolvedRelation entries in the map into a single entry where possible.
     * URs that share individual index names with the merged result are kept as separate entries
     * to prevent IndexResolution from deduplicating them and losing data.
     * Uses the same per-index-name overlap check as {@link #mergeIfPossible}.
     */
    private static void mergeUnresolvedRelationEntries(LinkedHashMap<String, LogicalPlan> flat) {
        List<String> urKeys = new ArrayList<>();
        for (Map.Entry<String, LogicalPlan> entry : flat.entrySet()) {
            if (entry.getValue() instanceof UnresolvedRelation) {
                urKeys.add(entry.getKey());
            }
        }
        if (urKeys.size() <= 1) {
            return;
        }

        String firstKey = urKeys.getFirst();
        UnresolvedRelation merged = (UnresolvedRelation) flat.get(firstKey);

        for (int i = 1; i < urKeys.size(); i++) {
            String key = urKeys.get(i);
            UnresolvedRelation ur = (UnresolvedRelation) flat.get(key);
            UnresolvedRelation result = mergeIfPossible(merged, ur);
            if (result != null) {
                merged = result;
                flat.remove(key);
            }
        }
        flat.put(firstKey, merged);
    }

    private static UnresolvedRelation mergeUnresolvedRelations(Collection<UnresolvedRelation> unresolvedRelations) {
        UnresolvedRelation template = unresolvedRelations.iterator().next();
        if (unresolvedRelations.size() == 1) {
            return template;
        }
        List<String> patterns = new ArrayList<>();
        for (UnresolvedRelation ur : unresolvedRelations) {
            patterns.add(ur.indexPattern().indexPattern());
        }
        return new UnresolvedRelation(
            template.source(),
            new IndexPattern(template.indexPattern().source(), String.join(",", patterns)),
            template.frozen(),
            template.metadataFields(),
            template.indexMode(),
            template.unresolvedMessage()
        );
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
            // Simple UnresolvedRelation subqueries are not kept as views, so we can compact them together and avoid branched plans.
            // But exclusion patterns must stay scoped to the view body — a bare UR with an exclusion that gets merged with sibling
            // or outer URs would have its exclusion's scope widened across the merged pattern list (see #146XXX), so those are
            // wrapped in a NamedSubquery via the else branch to prevent merging.
            return ur;
        } else {
            // More complex subqueries (or simple URs containing exclusions) are maintained with the view name for branch identification
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
