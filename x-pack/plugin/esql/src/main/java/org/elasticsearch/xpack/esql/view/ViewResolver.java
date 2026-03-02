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
import org.elasticsearch.xpack.core.esql.EsqlFeatureFlags;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

public class ViewResolver {

    protected Logger log = LogManager.getLogger(getClass());
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
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
        this.maxViewDepth = 0;
        this.client = null;
    }

    public ViewResolver(ClusterService clusterService, ProjectResolver projectResolver, Client client) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
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
        replaceViews(
            plan,
            parser,
            new LinkedHashSet<>(),
            viewQueries,
            0,
            listener.delegateFailureAndWrap((l, rewritten) -> listener.onResponse(new ViewResolutionResult(rewritten, viewQueries)))
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

        plan.transformDown((p, planListener) -> {
            switch (p) {
                case UnionAll union -> {
                    // UnionAll is the result of this re-writing, so we assume rewriting is completed
                    // TODO: This could conflicts with subquery feature, perhaps we need a new plan node type?
                    planListener.onResponse(union);
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
                return new Fork(fork.source(), updatedSubplans, fork.output());
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
        // Avoid re-resolving wildcards preserved for non-view matches, and deduplicate duplicate wildcard patterns to match how wildcard
        // resolving for indices handle duplicates.
        var patterns = Arrays.stream(unresolvedRelation.indexPattern().indexPattern().split(","))
            .filter(pattern -> Regex.isSimpleMatchPattern(pattern) == false || seenWildcards.add(pattern))
            .toArray(String[]::new);

        var req = new EsqlResolveViewAction.Request(REST_MASTER_TIMEOUT_DEFAULT);
        req.indices(patterns);

        doEsqlResolveViewsRequest(req, listener.delegateFailureAndWrap((l1, response) -> {
            if (response.views().length == 0) {
                listener.onResponse(unresolvedRelation);
                return;
            }

            final List<ViewPlan> subqueries = new ArrayList<>();
            SubscribableListener<Void> chain = SubscribableListener.newForked(l2 -> l2.onResponse(null));
            for (var view : response.views()) {
                chain = chain.andThen(l2 -> {
                    validateViewReference(view.name(), seenViews);
                    replaceViews(
                        resolve(view, parser, viewQueries),
                        parser,
                        seenViews,
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
                    // only one view, no need for UnionAll, return view plan directly
                    return subqueries.getFirst().plan();
                }
                if (unresolvedPatterns.isEmpty() == false) {
                    // We have non-view indexes, so we need an UnresolvedRelation for them too
                    subqueries.addFirst(createUnresolvedRelationPlan(unresolvedRelation, unresolvedPatterns));
                }
                return buildPlanFromBranches(unresolvedRelation, subqueries, depth);
            }).addListener(listener);
        }));
    }

    private void validateViewReference(String viewName, LinkedHashSet<String> seenViews) {
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
            // If any of the concrete resources were not views, pass them along as an unresolved relation
            if (resolvedIndexExpression.localExpressions().indices().stream().anyMatch(index -> seenViews.contains(index) == false)) {
                unresolvedPatterns.add(resolvedIndexExpression.original());
            }
        }
        if (unresolvedPatterns.isEmpty() == false) {
            var viewNames = getMetadata().views();
            for (String pattern : originalPatterns) {
                // If there is an exclusion, check if it references a view or is a wildcard
                if (pattern.startsWith("-")) {
                    String target = pattern.substring(1);
                    if (Regex.isSimpleMatchPattern(target) || viewNames.containsKey(target) == false) {
                        unresolvedPatterns.add(pattern);
                    }
                }
            }
        }
        return unresolvedPatterns;
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
        List<LogicalPlan> otherPlans = new ArrayList<>();
        for (ViewPlan lp : subqueries) {
            if (lp.plan instanceof UnresolvedRelation urp && urp.indexMode() == IndexMode.STANDARD) {
                unresolvedRelations.add(urp);
            } else {
                otherPlans.add(new Subquery(ur.source(), lp.plan, lp.name));
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
            otherPlans.addFirst(mergedUnresolved);
        }
        if (otherPlans.size() == 1) {
            return otherPlans.getFirst();
        }
        traceUnionAllBranches(depth, otherPlans);
        return new UnionAll(ur.source(), otherPlans, List.of());
    }

    private void traceUnionAllBranches(int depth, List<LogicalPlan> plans) {
        if (log.isTraceEnabled() == false) {
            return;
        }
        String tab = "    ".repeat(depth);
        log.trace("{}  creating UnionAll with {} branches:", tab, plans.size());
        String branchPrefix = "      " + tab;
        for (LogicalPlan p : plans) {
            log.trace("{}    branch plan=\n{}{}", tab, branchPrefix, p.toString().replace("\n", "\n" + branchPrefix));
        }
    }

    private LogicalPlan resolve(View view, BiFunction<String, String, LogicalPlan> parser, Map<String, String> viewQueries) {
        log.debug("Resolving view '{}'", view.name());
        // Store the view query so it can be used during Source deserialization
        viewQueries.put(view.name(), view.query());

        // Parse the view query with the view name, which causes all Source objects
        // to be tagged with the view name during parsing
        return parser.apply(view.query(), view.name());
    }
}
