/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

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
import org.elasticsearch.xpack.core.esql.EsqlFeatureFlags;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class ViewResolver {

    protected Logger log = LogManager.getLogger(getClass());
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private volatile int maxViewDepth;
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
    }

    public ViewResolver(ClusterService clusterService, ProjectResolver projectResolver) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        clusterService.getClusterSettings().initializeAndWatch(MAX_VIEW_DEPTH_SETTING, v -> this.maxViewDepth = v);
    }

    ViewMetadata getMetadata() {
        return clusterService.state().metadata().getProject(projectResolver.getProjectId()).custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
    }

    protected Map<String, IndexAbstraction> getIndicesLookup() {
        return clusterService.state().metadata().getProject(projectResolver.getProjectId()).getIndicesLookup();
    }

    protected boolean viewsFeatureEnabled() {
        return EsqlFeatureFlags.ESQL_VIEWS_FEATURE_FLAG.isEnabled();
    }

    /**
     * Result of view resolution containing both the rewritten plan and the view queries.
     */
    public record ViewResolutionResult(LogicalPlan plan, Map<String, String> viewQueries) {}

    /**
     * Replaces views in the plan with their resolved definitions.
     * @param plan the plan to resolve views in
     * @param parser a function that parses a view query with a given view name
     *               The BiFunction takes (query, viewName) and returns the parsed LogicalPlan.
     *               The viewName is used to tag Source objects so they can be correctly deserialized.
     * @return the resolution result containing the rewritten plan and collected view queries
     */
    public ViewResolutionResult replaceViews(LogicalPlan plan, BiFunction<String, String, LogicalPlan> parser) {
        if (viewsFeatureEnabled() == false) {
            return new ViewResolutionResult(plan, Map.of());
        }
        ViewMetadata views = getMetadata();
        if (views.views().isEmpty()) {
            // Don't bother to traverse the plan if there are no views defined
            return new ViewResolutionResult(plan, Map.of());
        }
        // Get all non-view names for this project, so we know if wildcards match any non-view indexes
        Set<String> nonViewNames = getIndicesLookup().entrySet()
            .stream()
            .filter(e -> e.getValue() == null || e.getValue().getType() != IndexAbstraction.Type.VIEW)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Map<String, String> viewQueries = new HashMap<>();
        LogicalPlan rewritten = replaceViewsInSubplan(
            plan,
            parser,
            views,
            nonViewNames,
            new LinkedHashSet<>(),
            new HashSet<>(),
            0,
            viewQueries
        );
        if (rewritten.equals(plan)) {
            log.debug("No views resolved");
            return new ViewResolutionResult(plan, Map.of());
        }
        log.debug("Views resolved:\n" + rewritten);
        return new ViewResolutionResult(rewritten, viewQueries);
    }

    /**
     * This method uses recursion to handle branched plans (Fork, Union, Subqueries, Views), while also using transformDown to handle
     * linear plans. TransformDown is also a recursive method, so this results in a depth-first traversal of the plan tree.
     * We maintain the same "seen" set for each branch so that multiple branches can refer to the same view without causing
     * false circular reference errors.
     */
    private LogicalPlan replaceViewsInSubplan(
        LogicalPlan plan,
        BiFunction<String, String, LogicalPlan> parser,
        ViewMetadata views,
        Set<String> nonViewNames,
        LinkedHashSet<String> outerSeen,
        HashSet<String> outerSeenWildcards,
        int depth,
        Map<String, String> viewQueries
    ) {
        // Do not modify the outer seen set, copy it for this subplan, allowing multiple subplans to refer to the same view
        LinkedHashSet<String> seen = new LinkedHashSet<>(outerSeen);
        HashSet<String> seenWildcards = new HashSet<>(outerSeenWildcards);
        String tab = "    ".repeat(depth);
        String pt = "    " + tab;
        log.trace(
            tab + "replaceViewsInSubplan depth=" + depth + " seen=" + seen + " plan=\n" + pt + plan.toString().replace("\n", "\n" + pt)
        );
        LogicalPlan rewritten = plan.transformDown(LogicalPlan.class, p -> {
            switch (p) {
                case UnionAll union -> {
                    // UnionAll is the result of this re-writing, so we assume rewriting is completed
                    // TODO: This could conflicts with subquery feature, perhaps we need a new plan node type?
                    return union;
                }
                case Fork fork -> {
                    List<LogicalPlan> subplans = new ArrayList<>(fork.children());
                    boolean changed = false;
                    for (int i = 0; i < subplans.size(); i++) {
                        LogicalPlan subplan = replaceViewsInSubplan(
                            subplans.get(i),
                            parser,
                            views,
                            nonViewNames,
                            seen,
                            seenWildcards,
                            depth + 1,
                            viewQueries
                        );
                        if (subplan.equals(subplans.get(i)) == false) {
                            changed = true;
                            subplans.set(i, subplan);
                        }
                    }
                    if (changed) {
                        return new Fork(fork.source(), subplans, fork.output());
                    }
                    return fork;
                }
                case UnresolvedRelation ur -> {
                    List<ViewPlan> subqueries = new ArrayList<>();
                    List<String> indexes = new ArrayList<>();
                    IndexPatterns patterns = extractViewAndIndexNames(views, ur, nonViewNames, seenWildcards);
                    if (patterns.views().isEmpty()) {
                        // No views found, return the original plan node
                        return ur;
                    }
                    log.trace(tab + "  found UnresolvedRelation with views: " + patterns.views().stream().map(View::name).toList());
                    for (View view : patterns.views()) {
                        if (seen.add(view.name()) == false) {
                            throw viewError("circular view reference '" + view.name() + "': ", new ArrayList<>(seen));
                        }
                        if (seen.size() > this.maxViewDepth) {
                            throw viewError("The maximum allowed view depth of " + this.maxViewDepth + " has been exceeded: ", seen);
                        }
                        LogicalPlan resolvedView = resolve(view, parser, viewQueries);
                        subqueries.add(
                            new ViewPlan(
                                view.name(),
                                replaceViewsInSubplan(
                                    resolvedView,
                                    parser,
                                    views,
                                    nonViewNames,
                                    seen,
                                    seenWildcards,
                                    depth + 1,
                                    viewQueries
                                )
                            )
                        );
                    }
                    indexes.addAll(patterns.indexNames());
                    indexes.addAll(patterns.wildCards());
                    if (indexes.isEmpty()) {
                        if (subqueries.size() == 1) {
                            // only one view, no need for union, return view plan directly
                            return subqueries.getFirst().plan;
                        }
                    } else {
                        // We have non-view indexes, so we need an UnresolvedRelation for them too
                        subqueries.addFirst(
                            new ViewPlan(
                                null,
                                new UnresolvedRelation(
                                    ur.source(),
                                    new IndexPattern(ur.indexPattern().source(), String.join(",", indexes)),
                                    ur.frozen(),
                                    ur.metadataFields(),
                                    ur.indexMode(),
                                    ur.unresolvedMessage()
                                )
                            )
                        );
                    }
                    // We replace the UnresolvedRelation with a UnionAll of all the view subqueries (and possibly an UnresolvedRelation)
                    return createTopPlan(ur, subqueries, depth);
                }
                default -> {
                }
            }
            // All other plan types are returned unchanged
            // TODO: determine if we need to modify source fields to resolve deserialization issues
            return p;
        });
        log.trace(
            tab + "rewritten plan at depth=" + depth + " seen=" + seen + " is\n" + pt + rewritten.toString().replace("\n", "\n" + pt)
        );
        return rewritten;
    }

    private record IndexPatterns(List<View> views, List<String> indexNames, List<String> wildCards) {}

    /**
     * Extract view names from an UnresolvedRelation, expanding any wildcards.
     * This method also returns the original names (including wildcards) so that indexes can be resolved later.
     */
    private IndexPatterns extractViewAndIndexNames(
        ViewMetadata viewMetadata,
        UnresolvedRelation unresolvedRelation,
        Set<String> nonViewNames,
        HashSet<String> seenWildcards
    ) {
        List<View> views = new ArrayList<>();
        List<String> indexNames = new ArrayList<>();
        List<String> wildCards = new ArrayList<>();
        for (String name : unresolvedRelation.indexPattern().indexPattern().split(",")) {
            name = name.trim();
            // We do not allow remote cluster ':' specifications for views
            if (Regex.isSimpleMatchPattern(name) && name.contains(":") == false) {
                if (seenWildcards.contains(name)) {
                    continue; // already processed this wildcard
                }
                seenWildcards.add(name);
                // If the name includes a wildcard, expand it to all matching views,
                // leaving the original wildcard name in place for index resolution
                for (String viewName : viewMetadata.views().keySet()) {
                    if (Regex.simpleMatch(name, viewName)) {
                        views.add(viewMetadata.getView(viewName));
                    }
                }
                // If there exist local indices matching the wildcard, keep the wildcard for later index resolution
                // TODO: See how to generalize this for CCS and CPS, probably need additional field-caps calls
                for (String indexName : nonViewNames) {
                    if (Regex.simpleMatch(name, indexName)) {
                        // The existence of any non-view index matching the wildcard means we need to keep the wildcard
                        wildCards.add(name);
                        break;
                    }
                }
            } else if (viewMetadata.getView(name) != null) {
                views.add(viewMetadata.getView(name));
            } else {
                indexNames.add(name);
            }
        }
        return new IndexPatterns(views, indexNames, wildCards);
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
