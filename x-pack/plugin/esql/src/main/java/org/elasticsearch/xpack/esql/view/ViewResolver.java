/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

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

    protected Map<String, IndexAbstraction> getIndicesLookup() {
        return clusterService.state().metadata().getProject(projectResolver.getProjectId()).getIndicesLookup();
    }

    protected boolean viewsFeatureEnabled() {
        return EsqlFeatures.ESQL_VIEWS_FEATURE_FLAG.isEnabled();
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
            new HashSet<>(),
            viewQueries,
            0,
            ActionListener.wrap(rewritten -> listener.onResponse(new ViewResolutionResult(rewritten, viewQueries)), listener::onFailure)
        );
    }

    private void replaceViews(
        LogicalPlan plan,
        BiFunction<String, String, LogicalPlan> parser,
        LinkedHashSet<String> seenViews,
        HashSet<String> seenWildcards,
        Map<String, String> viewQueries,
        int depth,
        ActionListener<LogicalPlan> listener
    ) {
        LinkedHashSet<String> seenInner = new LinkedHashSet<>(seenViews);
        HashSet<String> seenWildcardsInner = new HashSet<>(seenWildcards);

        plan.transformDown((p, planListener) -> {
            switch (p) {
                case UnionAll union -> {
                    // UnionAll is the result of this re-writing, so we assume rewriting is completed
                    // TODO: This could conflicts with subquery feature, perhaps we need a new plan node type?
                    planListener.onResponse(union);
                    return;
                }
                case Fork fork -> {
                    replaceViewsFork(fork, parser, seenInner, seenWildcardsInner, viewQueries, depth, planListener);
                    return;
                }
                case UnresolvedRelation ur -> {
                    replaceViewsUnresolvedRelation(ur, parser, seenInner, seenWildcardsInner, viewQueries, depth, planListener);
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
        HashSet<String> seenWildcards,
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
                    seenWildcards,
                    viewQueries,
                    depth + 1,
                    ActionListener.wrap(newPlan -> {
                        if (newPlan.equals(subplan) == false) {
                            var updatedSubplansInner = updatedSubplans;
                            if (updatedSubplansInner == null) {
                                updatedSubplansInner = new ArrayList<>(currentSubplans);
                            }
                            updatedSubplansInner.set(index, newPlan);
                            l.onResponse(updatedSubplansInner);
                        } else {
                            l.onResponse(updatedSubplans);
                        }
                    }, l::onFailure)
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
        UnresolvedRelation ur,
        BiFunction<String, String, LogicalPlan> parser,
        LinkedHashSet<String> seenViews,
        HashSet<String> seenWildcards,
        Map<String, String> viewQueries,
        int depth,
        ActionListener<LogicalPlan> listener
    ) {
        var patterns = Arrays.stream(ur.indexPattern().indexPattern().split(","))
            .filter(pattern -> Regex.isSimpleMatchPattern(pattern) == false || seenWildcards.add(pattern))
            .toArray(String[]::new);

        var req = new EsqlResolveViewAction.Request(REST_MASTER_TIMEOUT_DEFAULT);
        req.indices(patterns);

        doEsqlResolveViewsRequest(req, ActionListener.wrap(response -> {
            if (response.views().length == 0) {
                // TODO: if there are exclusions that result in no views and there are not other index abstraction matches, the current
                // behaviour for indices is to throw a "not found", the same should probably be true for views for consistency reasons.
                // So we might want to check if we have any unresolvedPatterns here.
                listener.onResponse(ur);
                return;
            }

            final List<ViewPlan> subqueries = new ArrayList<>();
            SubscribableListener<Void> chain = SubscribableListener.newForked(l -> l.onResponse(null));
            for (var view : response.views()) {
                chain = chain.andThen(l -> {
                    validateViewReference(view.name(), seenViews);
                    replaceViews(
                        resolve(view, parser, viewQueries),
                        parser,
                        seenViews,
                        seenWildcards,
                        viewQueries,
                        depth + 1,
                        ActionListener.wrap(fullyResolved -> {
                            subqueries.add(new ViewPlan(view.name(), fullyResolved));
                            l.onResponse(null);
                        }, l::onFailure)
                    );
                });
            }
            chain.andThenApply(ignored -> {
                var unresolvedPatterns = buildUnresolvedPatterns(response, seenViews);
                if (unresolvedPatterns.isEmpty() && subqueries.size() == 1) {
                    return subqueries.getFirst().plan();
                }
                if (unresolvedPatterns.isEmpty() == false) {
                    subqueries.addFirst(createUnresolvedRelationPlan(ur, unresolvedPatterns));
                }
                return createTopPlan(ur, subqueries, depth);
            }).addListener(listener);
        }, listener::onFailure));
    }

    private void validateViewReference(String viewName, LinkedHashSet<String> seenViews) {
        if (seenViews.add(viewName) == false) {
            throw viewError("circular view reference '" + viewName + "': ", new ArrayList<>(seenViews));
        }
        if (seenViews.size() > this.maxViewDepth) {
            throw viewError("The maximum allowed view depth of " + this.maxViewDepth + " has been exceeded: ", seenViews);
        }
    }

    private List<String> buildUnresolvedPatterns(EsqlResolveViewAction.Response response, LinkedHashSet<String> seenViews) {
        final var unresolvedPatterns = new ArrayList<String>();
        response.getResolvedIndexExpressions().expressions().forEach(resolvedIndexExpression -> {
            var indices = resolvedIndexExpression.localExpressions().indices();
            if (indices.stream().anyMatch(index -> seenViews.contains(index) == false)) {
                unresolvedPatterns.add(resolvedIndexExpression.original());
            }
            unresolvedPatterns.addAll(resolvedIndexExpression.remoteExpressions());
        });
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

    private LogicalPlan createTopPlan(UnresolvedRelation ur, List<ViewPlan> subqueries, int depth) {
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
        String tab = "    ".repeat(depth);
        log.trace(tab + "  creating UnionAll with " + otherPlans.size() + " branches:");
        String pt = "      " + tab;
        for (LogicalPlan p : otherPlans) {
            log.trace(tab + "    branch plan=\n" + pt + p.toString().replace("\n", "\n" + pt));
        }
        return new UnionAll(ur.source(), otherPlans, List.of());
    }

    private LogicalPlan resolve(View view, BiFunction<String, String, LogicalPlan> parser, Map<String, String> viewQueries) {
        log.debug("Resolving view '{}'", view.name());
        // Store the view query so it can be used during Source deserialization
        viewQueries.put(view.name(), view.query());

        // Parse the view query with the view name, which causes all Source objects
        // to be tagged with the view name during parsing
        return parser.apply(view.query(), view.name());
    }

    private VerificationException viewError(String type, Collection<String> seen) {
        StringBuilder b = new StringBuilder();
        for (String s : seen) {
            if (b.isEmpty()) {
                b.append(type);
            } else {
                b.append(" -> ");
            }
            b.append(s);
        }
        throw new VerificationException(b.toString());
    }
}
