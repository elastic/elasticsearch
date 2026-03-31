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
import org.elasticsearch.xpack.core.esql.EsqlFeatureFlags;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
    public static final Setting<Integer> MAX_VIEW_DEPTH_SETTING = Setting.intSetting(
        "esql.views.max_view_depth",
        10,
        0,
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
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

    protected boolean viewsFeatureEnabled() {
        return EsqlFeatureFlags.ESQL_VIEWS_FEATURE_FLAG.isEnabled();
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

        plan.transformDown((p, planListener) -> {
            switch (p) {
                case ViewUnionAll viewUnion -> {
                    // ViewUnionAll is the result of view resolution, so we skip it.
                    // Plain UnionAll (from user-written subqueries) matches the Fork case below
                    // and its children are recursed into with proper seen-set scoping.
                    planListener.onResponse(viewUnion);
                    return;
                }
                case Fork fork -> {
                    replaceViewsFork(fork, parser, seenInner, viewQueries, depth, planListener);
                    return;
                }
                case UnresolvedRelation ur -> {
                    replaceViewsUnresolvedRelation(ur, parser, seenInner, seenWildcards, viewQueries, depth, planListener);
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

            final List<ViewPlan> subqueries = new ArrayList<>();
            final LinkedHashSet<String> ancestorViews = new LinkedHashSet<>(seenViews);
            SubscribableListener<Void> chain = SubscribableListener.newForked(l2 -> l2.onResponse(null));
            for (var view : response.views()) {
                chain = chain.andThen(l2 -> {
                    seenViews.add(view.name());
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
                            subqueries.add(new ViewPlan(view.name(), fullyResolved));
                            l3.onResponse(null);
                        })
                    );
                });
            }
            chain.andThenApply(ignored -> {
                var unresolvedPatterns = buildUnresolvedPatterns(response, seenViews, patterns);
                if (unresolvedPatterns.isEmpty() && subqueries.size() == 1) {
                    // Only one view resolved with no remaining index patterns - return its plan directly.
                    return subqueries.getFirst().plan();
                }
                if (unresolvedPatterns.isEmpty() == false) {
                    // We have non-view indexes, so we need an UnresolvedRelation for them too
                    subqueries.add(createUnresolvedRelationPlan(unresolvedRelation, unresolvedPatterns));
                }
                return buildPlanFromBranches(unresolvedRelation, subqueries, depth);
            }).addListener(listener);
        }));
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
     * Builds the list of unresolved (non-view) patterns from the view resolution response.
     * <p>
     * Expressions marked as {@code CONCRETE_RESOURCE_NOT_VISIBLE}, {@code CONCRETE_RESOURCE_UNAUTHORIZED} or isn't a view flows through to
     * field caps. There they either fail via the same security checks that handle non-view queries (a search) or are resolved to a non-view
     * resource. Exclusion patterns from the original query that target non-view resources are also preserved. This ensures that
     * index-level exclusions are re-applied during the later index resolution step.
     */
    private List<String> buildUnresolvedPatterns(
        EsqlResolveViewAction.Response response,
        LinkedHashSet<String> seenViews,
        String[] originalPatterns
    ) {
        List<String> unresolvedPatterns = new ArrayList<>();
        for (var resolvedIndexExpression : response.getResolvedIndexExpressions().expressions()) {
            var result = resolvedIndexExpression.localExpressions().localIndexResolutionResult();
            // If any concrete resource (view, alias, datastream or index) was unauthorized, pass it along as an unresolved relation
            if (result == ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                || result == ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED) {
                unresolvedPatterns.add(resolvedIndexExpression.original());
                continue;
            }
            // If any of the concrete resources were not views, pass them along as an unresolved relation.
            // When CPS is enabled, also keep wildcard patterns because they may match remote indexes
            // in other projects (unlike CCS, CPS does not require explicit remote references).
            if (resolvedIndexExpression.localExpressions().indices().stream().anyMatch(index -> seenViews.contains(index) == false)
                || (crossProjectModeDecider.crossProjectEnabled() && Regex.isSimpleMatchPattern(resolvedIndexExpression.original()))) {
                unresolvedPatterns.add(resolvedIndexExpression.original());
            }
        }
        if (unresolvedPatterns.isEmpty() == false) {
            var viewNames = getMetadata().views();
            for (String pattern : originalPatterns) {
                if (patternIsExclusion(pattern) && isConcreteViewExclusion(pattern, viewNames::containsKey) == false) {
                    unresolvedPatterns.add(pattern);
                }
            }
        }
        return unresolvedPatterns;
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
        List<UnresolvedRelation> unresolvedRelations = new ArrayList<>();
        LinkedHashMap<String, LogicalPlan> otherPlans = new LinkedHashMap<>();
        for (ViewPlan lp : subqueries) {
            if (lp.plan instanceof UnresolvedRelation urp && urp.indexMode() == IndexMode.STANDARD) {
                unresolvedRelations.add(urp);
            } else if (lp.plan instanceof NamedSubquery namedSubquery) {
                assert namedSubquery.name().equals(lp.name);
                assert otherPlans.containsKey(lp.name) == false;
                otherPlans.put(lp.name, lp.plan);
            } else {
                assert otherPlans.containsKey(lp.name) == false;
                otherPlans.put(lp.name, new NamedSubquery(ur.source(), lp.plan, lp.name));
            }
        }
        if (unresolvedRelations.isEmpty() == false) {
            List<String> mergedIndexes = new ArrayList<>();
            for (UnresolvedRelation r : unresolvedRelations) {
                mergedIndexes.add(r.indexPattern().indexPattern());
            }
            UnresolvedRelation mergedUnresolved = new UnresolvedRelation(
                ur.source(),
                new IndexPattern(ur.indexPattern().source(), String.join(",", mergedIndexes)),
                ur.frozen(),
                ur.metadataFields(),
                ur.indexMode(),
                ur.unresolvedMessage()
            );
            assert otherPlans.containsKey(null) == false;
            otherPlans.putFirst(null, mergedUnresolved);
        }
        if (otherPlans.size() == 1) {
            return otherPlans.values().stream().findFirst().get();
        }
        traceUnionAllBranches(depth, otherPlans);
        return new ViewUnionAll(ur.source(), otherPlans, List.of());
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
            LinkedHashMap<String, LogicalPlan> subPlans = new LinkedHashMap<>();
            boolean hasNamedSubqueries = false;
            for (LogicalPlan child : unionAll.children()) {
                if (child instanceof NamedSubquery named) {
                    assert subPlans.containsKey(named.name()) == false;
                    subPlans.put(named.name(), named.child());
                    hasNamedSubqueries = true;
                } else if (child instanceof Subquery unnamed) {
                    // This named subquery is only maintained if it exists together with a named subquery
                    String name = "unnamed_view_" + Integer.toHexString(unnamed.toString().hashCode());
                    assert subPlans.containsKey(name) == false;
                    subPlans.put(name, unnamed.child());
                } else {
                    assert subPlans.containsKey(null) == false;
                    subPlans.put(null, child);
                }
            }
            if (hasNamedSubqueries) {
                unionAll = new ViewUnionAll(unionAll.source(), subPlans, unionAll.output());
            }
            return unionAll;
        });
        return plan;
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
        // Trial pass: collect all entries from full flattening and check for conflicts
        LinkedHashMap<String, LogicalPlan> flat = new LinkedHashMap<>();
        List<UnresolvedRelation> indexPatternURs = new ArrayList<>();
        boolean hasInnerVua = false;

        for (Map.Entry<String, LogicalPlan> entry : vua.namedSubqueries().entrySet()) {
            String key = entry.getKey();
            LogicalPlan value = entry.getValue();

            LogicalPlan inner = (value instanceof NamedSubquery ns) ? ns.child() : value;
            if (key != null && inner instanceof ViewUnionAll innerVua) {
                hasInnerVua = true;
                for (Map.Entry<String, LogicalPlan> innerEntry : innerVua.namedSubqueries().entrySet()) {
                    if (innerEntry.getKey() == null && innerEntry.getValue() instanceof UnresolvedRelation innerUr) {
                        indexPatternURs.add(innerUr);
                    } else if (innerEntry.getKey() != null && flat.containsKey(innerEntry.getKey())) {
                        return vua; // conflict: duplicate named entry across siblings, abort
                    } else {
                        flat.put(innerEntry.getKey(), innerEntry.getValue());
                    }
                }
            } else if (key == null && value instanceof UnresolvedRelation ur) {
                indexPatternURs.add(ur);
            } else {
                if (key != null && flat.containsKey(key)) {
                    return vua; // conflict
                }
                flat.put(key, value);
            }
        }

        if (hasInnerVua == false) {
            return vua;
        }

        if (indexPatternURs.isEmpty() == false) {
            flat.putFirst(null, mergeUnresolvedRelations(indexPatternURs));
        }

        if (flat.size() == 1) {
            return flat.values().iterator().next();
        }
        return new ViewUnionAll(vua.source(), flat, vua.output());
    }

    private static UnresolvedRelation mergeUnresolvedRelations(List<UnresolvedRelation> unresolvedRelations) {
        UnresolvedRelation template = unresolvedRelations.getFirst();
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
        if (subquery instanceof UnresolvedRelation ur) {
            // Simple UnresolvedRelation subqueries are not kept as views, so we can compact them together and avoid branched plans
            return ur;
        } else {
            // More complex subqueries are maintained with the view name for branch identification
            return new NamedSubquery(subquery.source(), subquery, view.name());
        }
    }
}
