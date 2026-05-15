/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Compacts the nested plan produced by {@link ViewResolver} into the form expected by the rest
 * of the query pipeline. The work is split into two phases so that {@link ViewShadowRelation}
 * siblings (CPS lenient lookups) survive long enough to be paired with their strict
 * {@link UnresolvedRelation} sibling at field-caps time:
 * <ol>
 *   <li>{@link #preIndexResolution(LogicalPlan)} — runs from {@code EsqlSession} before {@code PreAnalyzer}.
 *       Reshapes user-written {@link Subquery}/{@link UnionAll} structures into {@link ViewUnionAll}
 *       so the analyzer sees a uniform tree shape. Leaves shadows in place; leaves nested
 *       {@link ViewUnionAll}s nested. The {@link UnresolvedRelation} index patterns it leaves in
 *       the tree are exactly what {@code PreAnalyzer} hands to field-caps and what
 *       {@code ResolveTable} later looks up.</li>
 *   <li>{@link #postIndexResolution(LogicalPlan)} — runs as an analyzer rule after {@code ResolveTable}.
 *       Strips any {@link ViewShadowRelation} that lenient field-caps did not fold into a sibling
 *       {@code EsRelation} (in Phase A this is all of them, since lenient field-caps is not yet
 *       wired up — see esql-planning#543), then flattens nested {@link ViewUnionAll}s and unwraps
 *       remaining {@link NamedSubquery} wrappers.</li>
 * </ol>
 * <p>
 * The split is what lets a colleague implement lenient field-caps purely as a Phase B analyzer
 * rule that lives between {@code ResolveTable} and {@link #postIndexResolution}: shadows that match
 * remote indices get rewritten to {@link UnresolvedRelation}/{@code EsRelation}; shadows that
 * fail to match are simply left unresolved and {@link #postIndexResolution} sweeps them away. Ordering
 * details: see <a href="https://github.com/elastic/esql-planning/issues/472">esql-planning#472</a>.
 * <p>
 * Note: a small amount of compaction stays inside {@link ViewResolver#buildPlanFromBranches} —
 * specifically the per-level sibling {@link UnresolvedRelation} merge — to keep the resolved tree
 * compact at the per-level boundary, so wide branching levels of compactable views (e.g.
 * {@code FROM v1, v2, ... v9}) collapse to a single {@link UnresolvedRelation} rather than
 * tripping {@link Fork#MAX_BRANCHES} at post-analysis verification.
 */
public class ViewCompaction extends Rule<LogicalPlan, LogicalPlan> {

    /**
     * Backward-compatible helper: runs {@link #preIndexResolution(LogicalPlan)} followed by
     * {@link #postIndexResolution(LogicalPlan)}. Production code calls the two phases separately;
     * tests that exercise the compaction logic without going through the full analyzer call
     * this to get the same end state as the live pipeline produces.
     */
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return postIndexResolution(preIndexResolution(plan));
    }

    /**
     * Phase 1, runs before {@code PreAnalyzer}. Reshapes user-written {@link Subquery}/
     * {@link UnionAll} structures into {@link ViewUnionAll} for uniform downstream handling.
     * Deliberately does NOT strip {@link ViewShadowRelation} siblings or flatten nested
     * {@link ViewUnionAll}s — those are deferred to {@link #postIndexResolution} so lenient field-caps
     * (Phase B) can pair each shadow with its strict resolution at field-caps time.
     */
    public static LogicalPlan preIndexResolution(LogicalPlan plan) {
        return rewriteUnionAllsWithNamedSubqueries(plan);
    }

    /**
     * Phase 2, runs as an analyzer rule after {@code ResolveTable}. Strips
     * {@link ViewShadowRelation} siblings that lenient field-caps did not resolve, then flattens
     * nested {@link ViewUnionAll} structures and unwraps remaining {@link NamedSubquery}
     * wrappers. By the time this runs, all reachable {@link UnresolvedRelation}s have been
     * replaced by {@code EsRelation}s, so the {@link UnresolvedRelation}-merge step inside
     * {@link #compactNestedViewUnionAlls} is effectively a no-op — sibling {@code EsRelation}s
     * stay separate (Strategy A from esql-planning#543).
     */
    public static LogicalPlan postIndexResolution(LogicalPlan plan) {
        plan = stripViewShadowRelations(plan);
        // Strip can collapse a {@code ViewUnionAll[NamedSubquery, ViewShadowRelation]} to its sole
        // {@link NamedSubquery} when the shadow is removed. That exposes a {@code Subquery[NamedSubquery]}
        // pattern (and a parent {@link UnionAll} containing a {@link NamedSubquery} child) that
        // {@link #rewriteUnionAllsWithNamedSubqueries} needs to see in order to unwrap and convert
        // to {@link ViewUnionAll}, so we re-run the rewrite after the strip.
        plan = rewriteUnionAllsWithNamedSubqueries(plan);
        plan = compactNestedViewUnionAlls(plan);
        plan = plan.transformDown(NamedSubquery.class, UnaryPlan::child);
        return plan;
    }

    /**
     * Drop any still-unresolved {@link ViewShadowRelation} siblings from {@link ViewUnionAll}s.
     * Delegates to {@link ViewUnionAll#pruneEmptyBranches(java.util.function.Predicate)} so the
     * named-subqueries map stays in sync with the surviving children. Shares the same primitive
     * as {@code Analyzer.PruneEmptyUnionAllBranch} and {@code PruneEmptyForkBranches} —
     * different predicates, same shape — which keeps these rules order-independent: running
     * them in any order yields the same end state for the branches each predicate identifies.
     * <p>
     * Strip-specific extra: collapses a single-survivor {@link ViewUnionAll} to that lone
     * child. A view-resolved union with one branch left is no longer a branching choice — it's
     * just that single resolved subtree. (The other prune rules don't do this — they preserve
     * the wrapper. The collapse is a {@link ViewCompaction} semantic, not a {@link UnionAll} one.)
     */
    private static LogicalPlan stripViewShadowRelations(LogicalPlan plan) {
        return plan.transformDown(ViewUnionAll.class, vua -> {
            LogicalPlan pruned = vua.pruneEmptyBranches(child -> child instanceof ViewShadowRelation);
            if (pruned instanceof ViewUnionAll prunedVua && prunedVua.children().size() == 1) {
                return prunedVua.children().getFirst();
            }
            return pruned;
        });
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
        plan = plan.transformDown(Subquery.class, sq -> sq.child() instanceof NamedSubquery n ? n : sq);

        plan = plan.transformDown(UnionAll.class, unionAll -> {
            if (unionAll instanceof ViewUnionAll) {
                return unionAll;
            }
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

    /**
     * Bottom-up rewrite that flattens nested {@link ViewUnionAll} structures and merges sibling
     * bare {@link UnresolvedRelation}s at each level. See {@link #tryFlattenViewUnionAll}.
     * <p>
     * Also unwraps {@link NamedSubquery} entries whose child has been reduced to a bare
     * {@link UnresolvedRelation} without exclusions. The wrap was added by
     * {@code ViewResolver.buildPlanFromBranches} purely because the original child wasn't a bare
     * {@link UnresolvedRelation}; once nested compaction reduces it to one, the wrapper has no
     * purpose and would block the outer level's sibling {@link UnresolvedRelation} merge step from
     * seeing it. Exclusion-bearing {@link UnresolvedRelation}s stay wrapped to preserve their
     * narrow scope (see exclusion-leak tests).
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

        if (current instanceof NamedSubquery ns && ns.child() instanceof UnresolvedRelation ur && containsExclusion(ur) == false) {
            return ur;
        }
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
            if (inner instanceof ViewUnionAll innerVua) {
                // Named branches from inner ViewUnionAll: lift with their own names. A bare
                // UnresolvedRelation with an exclusion must be wrapped in a NamedSubquery before
                // lifting — otherwise the subsequent merge step would concatenate its pattern list
                // with a sibling outer UnresolvedRelation, widening the exclusion's scope beyond
                // the inner view body it came from.
                for (Map.Entry<String, LogicalPlan> innerEntry : innerVua.namedSubqueries().entrySet()) {
                    String innerKey = innerEntry.getKey();
                    LogicalPlan innerValue = innerEntry.getValue();
                    if (innerValue instanceof UnresolvedRelation innerUr && containsExclusion(innerUr)) {
                        innerValue = new NamedSubquery(innerUr.source(), innerUr, innerKey);
                    }
                    flat.put(makeUniqueKey(flat, innerKey), innerValue);
                }
            } else {
                // Plain Fork/UnionAll from user-written subqueries: lift children with suffixed
                // parent name. As in the ViewUnionAll branch above, a bare UnresolvedRelation child
                // with an exclusion must be wrapped in a NamedSubquery before lifting so the
                // subsequent merge step does not widen its scope.
                Fork fork = (Fork) inner;
                int childIndex = 1;
                for (LogicalPlan child : fork.children()) {
                    LogicalPlan unwrapped = (child instanceof Subquery sq) ? sq.child() : child;
                    String childKey = parentKey + "#" + childIndex++;
                    if (unwrapped instanceof UnresolvedRelation childUr && containsExclusion(childUr)) {
                        unwrapped = new NamedSubquery(childUr.source(), childUr, childKey);
                    }
                    flat.put(makeUniqueKey(flat, childKey), unwrapped);
                }
            }
        }

        // Always attempt to merge bare UnresolvedRelation siblings — the strip step earlier in this
        // rule may have just exposed entries that were previously hidden inside a ViewUnionAll
        // wrapping shadows + strict; without an unconditional merge here those exposed siblings
        // would stay as separate branches even when their patterns are mergeable.
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
     * Merges bare {@link UnresolvedRelation} entries in the map into a single entry where possible.
     * {@link UnresolvedRelation}s that share individual index names with the merged result are kept
     * as separate entries to prevent IndexResolution from deduplicating them and losing data.
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

    /** Merge the unresolved relation unless the index patterns contain matching index names. */
    private static UnresolvedRelation mergeIfPossible(UnresolvedRelation main, UnresolvedRelation other) {
        for (String mainPattern : main.indexPattern().indexPattern().split(",")) {
            for (String otherPattern : other.indexPattern().indexPattern().split(",")) {
                if (mainPattern.equals(otherPattern)) {
                    // A duplicate index name was found, fail this attempt to merge.
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

    /**
     * True iff any of {@code ur}'s comma-separated patterns is an exclusion in any of the
     * forms field-caps recognises ({@code -name}, {@code cluster:-name}, {@code *:-name},
     * {@code -cluster:*}). Mirrors {@code ViewResolver.patternIsExclusion} — see that
     * method's Javadoc for why the cluster-prefixed forms must be detected here too: order
     * matters when patterns are concatenated by the merge step, and silently dropping a
     * cluster-prefixed exclusion would let it merge into a sibling and lose its scope.
     */
    private static boolean containsExclusion(UnresolvedRelation ur) {
        for (String pattern : ur.indexPattern().indexPattern().split(",")) {
            if (pattern.startsWith("-")) {
                return true;
            }
            String[] split = RemoteClusterAware.splitIndexName(pattern);
            if (split[0] != null && split[1].startsWith("-")) {
                return true;
            }
        }
        return false;
    }

    private static void assertSubqueryDoesNotExist(Map<String, LogicalPlan> plans, String name) {
        if (plans.containsKey(name)) {
            String message = name == null ? "Un-named subquery already exists" : "Named subquery already exists: " + name;
            throw new IllegalStateException(message);
        }
    }
}
