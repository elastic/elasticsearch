/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Static directed graph of view-to-view references, used to police view self-reference independently of plan-shaped
 * resolution. Nodes are view names; an edge {@code a -> b} means view {@code a}'s body references view {@code b}.
 * <p>
 * This is the standalone, behaviour-tested substrate for the two self-reference rejections that today are threaded
 * through {@code ViewResolver}'s eager async DFS (see {@code ViewResolver#validateViewReferenceAndMarkSeen}):
 * <ul>
 *   <li><b>Circular reference</b> — a cycle in the graph reachable from the query's entry views;</li>
 *   <li><b>Max depth</b> — a path from the entry views longer than the configured maximum.</li>
 * </ul>
 * <p>
 * The decisive difference from the eager DFS is that a view body's wildcard/pattern references are expanded into
 * concrete view names <b>once</b> (via the supplied {@code wildcardExpander}, which mirrors the resolver's
 * {@code EsqlResolveViewAction} expansion — same matched names, same exclusion handling, same ordering). Because the
 * graph never re-walks and re-expands wildcards in-plan, the false-circular re-entry the eager DFS suffers (and patches
 * with {@code seenWildcards} / {@code resolvedPlans} / per-branch {@code seenViews} copies) never arises here: shared
 * wildcards across sibling views, a wildcard matching a sibling view's name, and exclusion-removed circular targets all
 * resolve correctly without any in-walk guard.
 * <p>
 * The DFS in {@link #check} reproduces the eager resolver's {@code seenViews} {@link LinkedHashSet} visitation order, so
 * the error-chain strings it produces are byte-identical to those the resolver produces today.
 */
public final class ViewGraph {

    private final Map<String, String> viewQueries;
    private final BiFunction<String, String, LogicalPlan> parser;
    private final Function<List<String>, List<String>> wildcardExpander;
    private final int maxViewDepth;

    /** Cache of each view's referenced view names (body order, wildcards expanded once). */
    private final Map<String, List<String>> edgeCache = new HashMap<>();

    /**
     * @param viewQueries      name -&gt; view query body, taken from cluster-state {@code ViewMetadata#views()}
     * @param parser           the same view parser the resolver uses to turn a view body into a {@link LogicalPlan}, so
     *                         the {@link UnresolvedRelation}s discovered here are exactly the ones the resolver intercepts
     * @param wildcardExpander given a view body's comma-split index patterns, returns the ordered list of matched
     *                         <b>view names</b> with exclusions applied — semantics identical to the resolver's
     *                         {@code EsqlResolveViewAction} expansion
     * @param maxViewDepth     the maximum allowed view depth ({@code esql.views.max_view_depth})
     */
    public ViewGraph(
        Map<String, String> viewQueries,
        BiFunction<String, String, LogicalPlan> parser,
        Function<List<String>, List<String>> wildcardExpander,
        int maxViewDepth
    ) {
        this.viewQueries = viewQueries;
        this.parser = parser;
        this.wildcardExpander = wildcardExpander;
        this.maxViewDepth = maxViewDepth;
    }

    /**
     * Police the self-reference contract for a query whose top-level {@code FROM} patterns expand to {@code entryViews}.
     * Throws the verbatim {@link VerificationException} the eager resolver would, or returns normally if the reachable
     * sub-graph is acyclic and within the depth limit.
     *
     * @param entryViews the view names the query's entry patterns resolve to, in the order the resolver visits them
     */
    public void check(Collection<String> entryViews) {
        for (String entry : entryViews) {
            // Each entry view starts a fresh ancestor path, mirroring the resolver's per-branch seenViews copy taken
            // from the (empty at top level) ancestor set.
            visit(entry, new LinkedHashSet<>());
        }
    }

    /**
     * Expand the query's top-level {@code FROM} patterns to entry view names and run {@link #check}. Convenience for the
     * common case where the caller has the raw patterns rather than pre-expanded entry views.
     */
    public void checkPatterns(List<String> entryPatterns) {
        check(wildcardExpander.apply(entryPatterns));
    }

    /**
     * Depth-first visit of {@code viewName}, with {@code ancestors} carrying the path from an entry view down to (but not
     * including) {@code viewName}. This reproduces {@code ViewResolver#validateViewReferenceAndMarkSeen}: adding the view
     * to the path detects a cycle, the post-add path size enforces the depth limit, and a per-edge copy of the path keeps
     * sibling branches independent (so a view appearing on two sibling branches is not a false cycle).
     */
    private void visit(String viewName, LinkedHashSet<String> ancestors) {
        // Per-branch copy: a sibling branch must be free to contain the same view (resolver's branchSeenViews).
        LinkedHashSet<String> path = new LinkedHashSet<>(ancestors);
        if (path.add(viewName) == false) {
            throw new VerificationException("circular view reference '" + viewName + "': " + String.join(" -> ", path));
        }
        if (path.size() > maxViewDepth) {
            throw new VerificationException(
                "The maximum allowed view depth of " + maxViewDepth + " has been exceeded: " + String.join(" -> ", path)
            );
        }
        for (String referenced : edges(viewName)) {
            visit(referenced, path);
        }
    }

    /**
     * The view names {@code viewName}'s body references, in body order with wildcards expanded once. Cached, since the
     * static graph is path-independent.
     */
    private List<String> edges(String viewName) {
        List<String> cached = edgeCache.get(viewName);
        if (cached != null) {
            return cached;
        }
        List<String> referenced = computeEdges(viewName);
        edgeCache.put(viewName, referenced);
        return referenced;
    }

    private List<String> computeEdges(String viewName) {
        String query = viewQueries.get(viewName);
        if (query == null) {
            return List.of();
        }
        LogicalPlan body = parser.apply(query, viewName);
        List<String> referencedViews = new ArrayList<>();
        // Collect every relation's patterns in body order, then expand each relation's patterns to matched view names.
        // Expanding per-relation (rather than concatenating all patterns) keeps each relation's exclusion scope intact,
        // matching how the resolver issues one EsqlResolveViewAction request per UnresolvedRelation. The walk descends
        // into IN-subquery bodies (in WHERE conditions) too, since those are resolved — and so policed — by the resolver,
        // so a view reachable only through an IN subquery is still a graph edge.
        // Descend through the same relation walk the resolver's substitution pass uses (including IN-subquery bodies in
        // WHERE conditions), so the graph's edges cover exactly the view references the resolver reaches — sharing the
        // single ViewResolver#forEachViewRelation walk keeps the two from ever drifting.
        ViewResolver.forEachViewRelation(body, ur -> {
            List<String> patterns = Arrays.asList(ur.indexPattern().indexPattern().split(","));
            for (String matched : wildcardExpander.apply(patterns)) {
                if (referencedViews.contains(matched) == false) {
                    referencedViews.add(matched);
                }
            }
        });
        return referencedViews;
    }
}
