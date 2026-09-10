/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.common.util.set.Sets.haveNonEmptyIntersection;

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
 *   <li>{@link #postIndexResolution(LogicalPlan, boolean)} — runs as an analyzer rule after {@code ResolveTable}.
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
 * tripping {@link MergePlan#MAX_BRANCHES} at post-analysis verification.
 */
public class ViewCompaction extends Rule<LogicalPlan, LogicalPlan> {

    /**
     * Backward-compatible helper: runs {@link #preIndexResolution(LogicalPlan)} followed by
     * {@link #postIndexResolution(LogicalPlan, boolean)}. Production code calls the two phases separately;
     * tests that exercise the compaction logic without going through the full analyzer call
     * this to get the same end state as the live pipeline produces.
     * <p>
     * TODO: this skips {@code ResolveTable} between the two phases, so {@link #postIndexResolution}
     * sees {@link UnresolvedRelation}s rather than {@code EsRelation}s. This causes
     * {@link #mergeUnresolvedRelationEntries} in {@link #compactNestedViewUnionAlls} to perform
     * cross-level UR merging that never happens in the production pipeline (where all URs are
     * already resolved by the time {@link #postIndexResolution} runs). Tests using this method
     * therefore diverge slightly from production; fix in a follow-up by having test helpers
     * replicate the full pipeline (view resolution → {@link #preIndexResolution} → ResolveTable →
     * {@link #postIndexResolution}) instead of using this shortcut.
     */
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return postIndexResolution(preIndexResolution(plan), false);
    }

    /**
     * Phase 1, runs before {@code PreAnalyzer}. Reshapes user-written {@link Subquery}/
     * {@link UnionAll} structures into {@link ViewUnionAll} for uniform downstream handling.
     * Deliberately does NOT strip {@link ViewShadowRelation} siblings or flatten nested
     * {@link ViewUnionAll}s — those are deferred to {@link #postIndexResolution} so lenient field-caps
     * (Phase B) can pair each shadow with its strict resolution at field-caps time.
     *
     * Records view-branch membership in {@link ViewUnionAll#viewBranchKeys()} but never collapses a
     * boundary, so it needs no knowledge of the request filter.
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
     *
     * @param preserveViewBoundaries {@code true} when the request carries a DSL filter that must be
     *                               applied at view-output boundaries. This is the <em>only</em> reason
     *                               to keep a wrapper that would otherwise be compacted away: when
     *                               {@code false} every collapse that was possible before the
     *                               request-filter feature is still performed. When {@code true}, a
     *                               single-survivor {@link ViewUnionAll} whose surviving branch is a view
     *                               branch is kept intact so that
     *                               {@link org.elasticsearch.xpack.esql.dsltranslate.ViewRequestFilterRewriter}
     *                               can find the boundary; every other branch kind still collapses.
     */
    public static LogicalPlan postIndexResolution(LogicalPlan plan, boolean preserveViewBoundaries) {
        plan = stripViewShadowRelations(plan, preserveViewBoundaries);
        // Strip can collapse a {@code ViewUnionAll[NamedSubquery, ViewShadowRelation]} to its sole
        // {@link NamedSubquery} when the shadow is removed. That exposes a {@code Subquery[NamedSubquery]}
        // pattern (and a parent {@link UnionAll} containing a {@link NamedSubquery} child) that
        // {@link #rewriteUnionAllsWithNamedSubqueries} needs to see in order to unwrap and convert
        // to {@link ViewUnionAll}, so we re-run the rewrite after the strip.
        plan = rewriteUnionAllsWithNamedSubqueries(plan);
        plan = compactNestedViewUnionAlls(plan, preserveViewBoundaries);
        plan = plan.transformDown(NamedSubquery.class, UnaryPlan::child);
        return plan;
    }

    /**
     * Drop any still-unresolved {@link ViewShadowRelation} siblings from {@link ViewUnionAll}s.
     * Delegates to {@link ViewUnionAll#pruneEmptyBranches(java.util.function.Predicate)} so the
     * named-subqueries map stays in sync with the surviving children. Shares the same primitive
     * as {@code Analyzer.PruneEmptyUnionAllBranch} and {@code PruneEmptyMergeBranches} —
     * different predicates, same shape — which keeps these rules order-independent: running
     * them in any order yields the same end state for the branches each predicate identifies.
     * <p>
     * Strip-specific extra: collapses a single-survivor {@link ViewUnionAll} to that lone
     * child. A view-resolved merge with one branch left is no longer a branching choice — it's
     * just that single resolved subtree. (The other prune rules don't do this — they preserve
     * the wrapper. The collapse is a {@link ViewCompaction} semantic, not a {@link UnionAll} one.)
     */
    private static LogicalPlan stripViewShadowRelations(LogicalPlan plan, boolean preserveViewBoundaries) {
        return plan.transformDown(ViewUnionAll.class, vua -> {
            LogicalPlan pruned = vua.pruneEmptyBranches(child -> child instanceof ViewShadowRelation);
            if (pruned instanceof ViewUnionAll prunedVua && prunedVua.children().size() == 1) {
                // Collapse the single-survivor wrapper unless a request filter still needs this
                // boundary. Both conditions matter and neither implies the other:
                // preserveViewBoundaries asks whether there is a filter to apply at a view boundary
                // at all — with no filter we collapse exactly as before the feature existed.
                // isViewBranch asks whether this branch is a view, whose output the filter must be
                // applied above; a bare index or a user-written subquery takes the ordinary Lucene
                // pushdown path instead and so collapses freely even when a filter is present.
                String survivingKey = prunedVua.namedSubqueries().keySet().iterator().next();
                if (preserveViewBoundaries == false || prunedVua.isViewBranch(survivingKey) == false) {
                    return prunedVua.children().getFirst();
                }
            }
            return pruned;
        });
    }

    /**
     * Top-down rewrite that:
     * <ol>
     *   <li>Unwraps {@code Subquery[NamedSubquery[X]]} → {@code NamedSubquery[X]}</li>
     *   <li>Unwraps {@code Subquery[ViewUnionAll[...]]} → {@code ViewUnionAll[...]} so that the
     *       parent {@link UnionAll} can inline the view-branch entries in step 3.</li>
     *   <li>Converts plain {@link UnionAll} nodes containing at least one {@link NamedSubquery}
     *       or {@link ViewUnionAll} child into a single {@link ViewUnionAll} node, inlining all
     *       view branches from nested {@code ViewUnionAll} children</li>
     * </ol>
     * This handles user-written {@code UNION ALL (FROM my_view)} where the parser creates a
     * {@link Subquery} wrapper and view resolution replaces its child with a {@link NamedSubquery}
     * or a {@link ViewUnionAll} (in the single-view case, view resolution always wraps in
     * {@link ViewUnionAll} so that {@link org.elasticsearch.xpack.esql.dsltranslate.ViewRequestFilterRewriter}
     * can identify and filter view boundaries after analysis).
     */
    static LogicalPlan rewriteUnionAllsWithNamedSubqueries(LogicalPlan plan) {
        // Unwrap Subquery[NamedSubquery[X]] → NamedSubquery[X]
        // Unwrap Subquery[ViewUnionAll[...]] → ViewUnionAll[...] so the parent UnionAll can inline it.
        plan = plan.transformDown(Subquery.class, sq -> switch (sq.child()) {
            case NamedSubquery n -> n;
            case ViewUnionAll vua -> vua;
            default -> sq;
        });

        plan = plan.transformDown(UnionAll.class, unionAll -> {
            if (unionAll instanceof ViewUnionAll) {
                return unionAll;
            }
            boolean hasViewChildren = unionAll.children().stream().anyMatch(c -> c instanceof NamedSubquery || c instanceof ViewUnionAll);
            if (hasViewChildren == false) {
                return unionAll;
            }
            LinkedHashMap<String, LogicalPlan> subPlans = new LinkedHashMap<>();
            // Structural truth only: which branches came from views. Recorded unconditionally —
            // whether a boundary must survive compaction is decided separately, from
            // preserveViewBoundaries, at the points that would collapse it.
            Set<String> viewBranchKeys = new HashSet<>();
            for (LogicalPlan child : unionAll.children()) {
                if (child instanceof NamedSubquery named) {
                    assertSubqueryDoesNotExist(subPlans, named.name());
                    subPlans.put(named.name(), named.child());
                    // A NamedSubquery is by construction a resolved view branch.
                    viewBranchKeys.add(named.name());
                } else if (child instanceof ViewUnionAll vua) {
                    // Inline the ViewUnionAll's named entries directly into this level, preserving
                    // their view-branch status. This handles the case where view resolution wraps
                    // a single view in a ViewUnionAll (e.g. FROM emp2, (FROM my_view) where the
                    // user-written Subquery wrapper was unwrapped above and the inner ViewUnionAll
                    // now appears as a direct child of this UnionAll).
                    for (Map.Entry<String, LogicalPlan> entry : vua.namedSubqueries().entrySet()) {
                        assertSubqueryDoesNotExist(subPlans, entry.getKey());
                        subPlans.put(entry.getKey(), entry.getValue());
                        if (vua.isViewBranch(entry.getKey())) {
                            viewBranchKeys.add(entry.getKey());
                        }
                    }
                } else if (child instanceof Subquery unnamed) {
                    String name = "unnamed_view_" + Integer.toHexString(unnamed.toString().hashCode());
                    assertSubqueryDoesNotExist(subPlans, name);
                    subPlans.put(name, unnamed.child());
                    // Literal user-written subquery: NOT a view branch.
                } else {
                    assertSubqueryDoesNotExist(subPlans, null);
                    subPlans.put(null, child);
                    // Bare plan: NOT a view branch.
                }
            }
            return new ViewUnionAll(unionAll.source(), subPlans, viewBranchKeys, unionAll.output());
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
    static LogicalPlan compactNestedViewUnionAlls(LogicalPlan plan, boolean preserveViewBoundaries) {
        List<LogicalPlan> children = plan.children();
        List<LogicalPlan> newChildren = null;
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            LogicalPlan newChild = compactNestedViewUnionAlls(child, preserveViewBoundaries);
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
            return tryFlattenViewUnionAll(vua, preserveViewBoundaries);
        }
        return current;
    }

    private static LogicalPlan tryFlattenViewUnionAll(ViewUnionAll vua, boolean preserveViewBoundaries) {
        // Trial pass: collect all entries from full flattening and check for conflicts.
        // Inner ViewUnionAlls that only contain UnresolvedRelations are lifted into the parent,
        // eliminating nesting that the runtime doesn't yet support.
        // Inner MergePlans (from user-written subqueries inside views) are also lifted,
        // with each child becoming a separate named entry suffixed from the parent view name.
        LinkedHashMap<String, LogicalPlan> flat = new LinkedHashMap<>();
        // Tracks which keys in `flat` correspond to actual resolved view branches.
        Set<String> flatViewBranchKeys = new HashSet<>();

        // Process non-merge entries first so that all outer keys are in `flat` before we attempt
        // to flatten inner merges. This makes the conflict check order-independent —
        // without it, an inner merge processed before a later outer entry with the same key would
        // miss the conflict, producing extra branches that can exceed the MergePlan branch limit.
        List<Map.Entry<String, LogicalPlan>> mergeEntries = new ArrayList<>();
        for (Map.Entry<String, LogicalPlan> entry : vua.namedSubqueries().entrySet()) {
            String key = entry.getKey();
            LogicalPlan value = entry.getValue();
            LogicalPlan inner = (value instanceof NamedSubquery ns) ? ns.child() : value;
            if (inner instanceof MergePlan) {
                mergeEntries.add(entry);
            } else if (value instanceof UnresolvedRelation) {
                String assignedKey = makeUniqueKey(flat, key);
                flat.put(assignedKey, value);
                // Propagate view-branch status: a view whose body is a bare UnresolvedRelation
                // is still a view branch (the view is a pass-through to an index/alias).
                if (vua.isViewBranch(key)) {
                    flatViewBranchKeys.add(assignedKey);
                }
            } else {
                if (flat.containsKey(key)) {
                    return vua; // conflict
                }
                flat.put(key, value);
                // Propagate view-branch status from the outer ViewUnionAll.
                if (vua.isViewBranch(key)) {
                    flatViewBranchKeys.add(key);
                }
            }
        }

        for (Map.Entry<String, LogicalPlan> entry : mergeEntries) {
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
                    String assignedKey = makeUniqueKey(flat, innerKey);
                    flat.put(assignedKey, innerValue);
                    // Propagate view-branch status from the inner ViewUnionAll.
                    if (innerVua.isViewBranch(innerKey)) {
                        flatViewBranchKeys.add(assignedKey);
                    }
                }
            } else {
                // Plain MergePlan from user-written subqueries: lift children with suffixed
                // parent name. As in the ViewUnionAll branch above, a bare UnresolvedRelation child
                // with an exclusion must be wrapped in a NamedSubquery before lifting so the
                // subsequent merge step does not widen its scope.
                MergePlan mergePlan = (MergePlan) inner;
                int childIndex = 1;
                for (LogicalPlan child : mergePlan.children()) {
                    LogicalPlan unwrapped = (child instanceof Subquery sq) ? sq.child() : child;
                    String childKey = parentKey + "#" + childIndex++;
                    if (unwrapped instanceof UnresolvedRelation childUr && containsExclusion(childUr)) {
                        unwrapped = new NamedSubquery(childUr.source(), childUr, childKey);
                    }
                    flat.put(makeUniqueKey(flat, childKey), unwrapped);
                }
            }
        }

        // Merge bare UnresolvedRelation siblings that ended up at the same level after
        // flattening. ViewResolver.buildPlanFromBranches only merges URs within a single
        // nesting level; after nested ViewUnionAlls are hoisted into this flat map, URs from
        // different levels may become adjacent and mergeable.
        //
        // No alias resolver is available here, so alias-vs-backing-index overlap is not
        // detected. This is safe in the production pipeline: by the time postIndexResolution
        // runs, ResolveTable has already converted every UnresolvedRelation to an EsRelation,
        // making the merge loop a no-op. The gap only exists in the apply() test convenience
        // (which runs both phases without ResolveTable in between), and only for the
        // multi-level nesting variant of the alias scenario — not a production concern.
        mergeUnresolvedRelationEntries(flat, flatViewBranchKeys);
        // Remove any view-branch keys that the merge step may have removed.
        flatViewBranchKeys.retainAll(flat.keySet());

        if (flat.size() > MergePlan.MAX_BRANCHES) {
            return vua; // flattening would exceed the branch limit, keep the nested structure
        }
        if (flat.size() == 1) {
            String survivingKey = flat.keySet().iterator().next();
            LogicalPlan survivingPlan = flat.values().iterator().next();
            // Same two-part decision as in stripViewShadowRelations: collapse the lone entry unless a
            // request filter still needs this view boundary. With no filter this always collapses, so
            // the pre-feature compaction is preserved in full.
            if (preserveViewBoundaries == false || flatViewBranchKeys.contains(survivingKey) == false) {
                return survivingPlan;
            }
        }
        return new ViewUnionAll(vua.source(), flat, flatViewBranchKeys, vua.output());
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
     * Merges bare {@link UnresolvedRelation} entries in the map into a single entry where possible,
     * using string-equality and wildcard overlap as guards. Called after nested {@link ViewUnionAll}s
     * are flattened so that URs lifted from inner levels can be merged with sibling URs at the outer
     * level. Alias-vs-backing-index overlap is not checked here (no alias resolver is available at
     * this call site); see the comment at the call site for why that is safe in production.
     * <p>
     * When the flat map contains a mix of view-branch and non-view-branch URs, the first
     * view-branch UR is chosen as {@code firstKey} (the merge accumulator). This ensures that
     * view-content patterns appear before wildcard index patterns in the merged UR pattern string
     * (e.g. {@code "emp1,emp3,view_1_*"} rather than {@code "view_1_*,emp1,emp3"}) because the
     * non-view-branch wildcard UR ("main") is merged as {@code other} (appended) rather than as
     * the base.
     */
    private static void mergeUnresolvedRelationEntries(LinkedHashMap<String, LogicalPlan> flat, Set<String> viewBranchKeys) {
        List<String> urKeys = new ArrayList<>();
        for (Map.Entry<String, LogicalPlan> entry : flat.entrySet()) {
            if (entry.getValue() instanceof UnresolvedRelation) {
                urKeys.add(entry.getKey());
            }
        }
        if (urKeys.size() <= 1) {
            return;
        }

        // Prefer the first view-branch UR as firstKey so that view content precedes wildcard
        // patterns in the merged result. Fall back to the first UR if there are no view branches.
        String firstKey = urKeys.getFirst();
        for (String key : urKeys) {
            if (viewBranchKeys.contains(key)) {
                firstKey = key;
                break;
            }
        }
        UnresolvedRelation merged = (UnresolvedRelation) flat.get(firstKey);

        for (String key : urKeys) {
            if (key.equals(firstKey)) {
                continue;
            }
            UnresolvedRelation ur = (UnresolvedRelation) flat.get(key);
            UnresolvedRelation result = mergeIfPossible(merged, ur);
            if (result != null) {
                merged = result;
                flat.remove(key);
            }
        }
        flat.put(firstKey, merged);
    }

    /**
     * Merge the unresolved relation unless the index patterns contain matching index names, or
     * unless alias resolution via {@code aliasResolver} reveals that patterns in {@code main} and
     * {@code other} map to overlapping concrete indices (e.g. one pattern is an alias that points
     * to the same index as a pattern in the other relation). Pass {@code null} to skip alias
     * checking (the existing string-equality and wildcard checks still apply).
     * <p>
     * {@code aliasResolver} should map a local, non-wildcard index/alias name to the set of
     * concrete index names it backs. For a concrete index {@code x} it should return {@code {x}};
     * for an alias {@code a → x} it should return {@code {x}}.
     */
    static UnresolvedRelation mergeIfPossible(
        UnresolvedRelation main,
        UnresolvedRelation other,
        @Nullable Function<String, Set<String>> aliasResolver
    ) {
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
                // Check alias resolution: two non-wildcard, non-remote patterns may point to the same
                // concrete index via alias. Without this check, merging "source-index" and "source-alias"
                // (where source-alias → source-index) produces UnresolvedRelation("source-index,source-alias")
                // which field-caps deduplicates to a single copy, silently dropping one data branch.
                if (aliasResolver != null
                    && Regex.isSimpleMatchPattern(mainPattern) == false
                    && Regex.isSimpleMatchPattern(otherPattern) == false
                    && RemoteClusterAware.isRemoteIndexName(mainPattern) == false
                    && RemoteClusterAware.isRemoteIndexName(otherPattern) == false
                    && haveNonEmptyIntersection(aliasResolver.apply(mainPattern), aliasResolver.apply(otherPattern))) {
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
     * Merge the unresolved relation unless the index patterns overlap by string equality or wildcard.
     * Alias resolution is not performed; this overload is used by {@link #mergeUnresolvedRelationEntries}
     * after nested {@link ViewUnionAll} flattening, where no alias resolver is available. In the
     * production pipeline this is always a no-op because {@code ResolveTable} has already converted
     * every {@link UnresolvedRelation} to an {@code EsRelation} before {@link #postIndexResolution} runs.
     */
    static UnresolvedRelation mergeIfPossible(UnresolvedRelation main, UnresolvedRelation other) {
        return mergeIfPossible(main, other, null);
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
            var split = RemoteClusterAware.splitIndexName(pattern);
            if (split.clusterAlias() != null && split.indexExpression().startsWith("-")) {
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
