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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedMetadataAttributeExpression;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.BranchIdentity;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
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
 *       Reshapes user-written {@link Subquery}/{@link UnionAll} structures into {@link BranchPlan}
 *       so the analyzer sees a uniform tree shape. Leaves shadows in place; leaves nested
 *       {@link BranchPlan}s nested. The {@link UnresolvedRelation} index patterns it leaves in
 *       the tree are exactly what {@code PreAnalyzer} hands to field-caps and what
 *       {@code ResolveTable} later looks up.</li>
 *   <li>{@link #postIndexResolution(LogicalPlan, boolean)} — runs as an analyzer rule after {@code ResolveTable}.
 *       Strips any {@link ViewShadowRelation} that lenient field-caps did not fold into a sibling
 *       {@code EsRelation} (today this is all of them, since lenient field-caps is not yet
 *       wired up), then flattens nested {@link BranchPlan}s.</li>
 * </ol>
 * <p>
 * The split is what lets a colleague implement lenient field-caps purely as an analyzer
 * rule that lives between {@code ResolveTable} and {@link #postIndexResolution}: shadows that match
 * remote indices get rewritten to {@link UnresolvedRelation}/{@code EsRelation}; shadows that
 * fail to match are simply left unresolved and {@link #postIndexResolution} sweeps them away.
 * <p>
 * Note: a small amount of compaction stays inside {@link ViewResolver#buildPlanFromBranches} —
 * specifically the per-level sibling {@link UnresolvedRelation} merge — to keep the resolved tree
 * compact at the per-level boundary, so wide branching levels of compactable views (e.g.
 * {@code FROM v1, v2, ... v9}) collapse to a single {@link UnresolvedRelation} rather than
 * one branch per view, which would count against {@code max_branch_count}.
 */
public class ViewCompaction extends Rule<LogicalPlan, LogicalPlan> {

    /**
     * Backward-compatible helper: runs {@link #preIndexResolution(LogicalPlan)} followed by
     * {@link #postIndexResolution(LogicalPlan, boolean)} and the {@link NamedSubquery} unwrap the analyzer
     * performs right after ({@code Analyzer.UnwrapNamedSubqueries}). Production code calls the phases separately;
     * tests that exercise the compaction logic without going through the full analyzer call
     * this to get the same end state as the live pipeline produces.
     * <p>
     * TODO: this skips {@code ResolveTable} between the two phases, so {@link #postIndexResolution}
     * sees {@link UnresolvedRelation}s rather than {@code EsRelation}s. This causes
     * {@link #mergeUnresolvedRelationEntries} in {@link #compactNestedBranchPlans} to perform
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
     * Phase 1, runs before {@code PreAnalyzer}. Converts user-written {@link UnionAll}s — view-related or not —
     * into {@link BranchPlan}, recording a {@link BranchIdentity} per branch, and nothing else.
     * Every branch value is kept <em>verbatim</em>: no {@link Subquery}/{@link NamedSubquery} wrapper is
     * unwrapped or rewrapped here, because the analyzer's {@code ResolveSubqueryRelationColumns} rule reads
     * those wrappers to freeze {@code _class}/{@code _name}, and it must see them exactly as the parser and
     * {@link ViewResolver} left them. All unwrapping happens post-freeze, in {@link #postIndexResolution}.
     * <p>
     * Deliberately does NOT strip {@link ViewShadowRelation} siblings or flatten nested
     * {@link BranchPlan}s — those are deferred to {@link #postIndexResolution} so lenient field-caps
     * can pair each shadow with its strict resolution at field-caps time.
     */
    public static LogicalPlan preIndexResolution(LogicalPlan plan) {
        return plan.transformDown(UnionAll.class, unionAll -> toBranchPlan(unionAll, true));
    }

    /**
     * Phase 2, runs as an analyzer rule after {@code ResolveTable}. Strips
     * {@link ViewShadowRelation} siblings that lenient field-caps did not resolve, then flattens
     * nested {@link BranchPlan} structures. By the time this runs, all reachable {@link UnresolvedRelation}s have been
     * replaced by {@code EsRelation}s, so the {@link UnresolvedRelation}-merge step inside
     * {@link #compactNestedBranchPlans} is effectively a no-op — sibling {@code EsRelation}s
     * stay separate rather than merging via a combined field-caps call.
     *
     * @param preserveViewBoundaries {@code true} when the request carries a DSL filter that must be
     *                               applied at view-output boundaries. This is the <em>only</em> reason
     *                               to keep a wrapper that would otherwise be compacted away: when
     *                               {@code false} every collapse that was possible before the
     *                               request-filter feature is still performed. When {@code true}, a
     *                               single-survivor {@link BranchPlan} whose surviving branch is a view
     *                               branch is kept intact so that
     *                               {@link org.elasticsearch.xpack.esql.dsltranslate.ViewRequestFilterRewriter}
     *                               can find the boundary; every other branch kind still collapses.
     */
    public static LogicalPlan postIndexResolution(LogicalPlan plan, boolean preserveViewBoundaries) {
        return postIndexResolution(plan, preserveViewBoundaries, EsqlFlags.DEFAULTS.maxBranchCount());
    }

    /**
     * As {@link #postIndexResolution(LogicalPlan, boolean)}, flattening a nested view union only when the flat width
     * is within {@code maxBranchCount}.
     */
    public static LogicalPlan postIndexResolution(LogicalPlan plan, boolean preserveViewBoundaries, int maxBranchCount) {
        plan = stripViewShadowRelations(plan, preserveViewBoundaries);
        // Strip can collapse a {@code BranchPlan[NamedSubquery, ViewShadowRelation]} to its sole
        // {@link NamedSubquery} when the shadow is removed. That exposes a {@code Subquery[NamedSubquery]}
        // pattern (and a parent {@link UnionAll} containing a {@link NamedSubquery} child) that
        // {@link #rewriteUnionAllsWithNamedSubqueries} needs to see in order to unwrap and convert
        // to {@link BranchPlan}, so we re-run the rewrite after the strip.
        //
        // No wrapper needs protecting here: the analyzer freezes _class/_name into literal Evals
        // (Analyzer.ResolveSubqueryRelationColumns) BEFORE this phase runs, so a branch whose identity
        // matters carries a frozen Eval that blocks the merges below by structure alone.
        plan = rewriteUnionAllsWithNamedSubqueries(plan);
        plan = compactNestedBranchPlans(plan, preserveViewBoundaries, maxBranchCount);
        return plan;
    }

    /**
     * Drop any still-unresolved {@link ViewShadowRelation} siblings from {@link BranchPlan}s.
     * Delegates to {@link BranchPlan#pruneEmptyBranches(java.util.function.Predicate)} so the
     * named-subqueries map stays in sync with the surviving children. Shares the same primitive
     * as {@code Analyzer.PruneEmptyUnionAllBranch} and {@code PruneEmptyMergeBranches} —
     * different predicates, same shape — which keeps these rules order-independent: running
     * them in any order yields the same end state for the branches each predicate identifies.
     * <p>
     * Strip-specific extra: collapses a single-survivor {@link BranchPlan} to that lone
     * child. A view-resolved merge with one branch left is no longer a branching choice — it's
     * just that single resolved subtree. (The other prune rules don't do this — they preserve
     * the wrapper. The collapse is a {@link ViewCompaction} semantic, not a {@link UnionAll} one.)
     */
    private static LogicalPlan stripViewShadowRelations(LogicalPlan plan, boolean preserveViewBoundaries) {
        return plan.transformDown(BranchPlan.class, vua -> {
            LogicalPlan pruned = vua.pruneEmptyBranches(child -> child instanceof ViewShadowRelation);
            if (pruned instanceof BranchPlan prunedVua && prunedVua.children().size() == 1) {
                // Collapse the single-survivor wrapper unless a request filter still needs this
                // boundary. Both conditions matter and neither implies the other:
                // preserveViewBoundaries asks whether there is a filter to apply at a view boundary
                // at all — with no filter we collapse exactly as before the feature existed.
                // isViewBranch asks whether this branch is a view, whose output the filter must be
                // applied above; a bare index or a user-written subquery takes the ordinary Lucene
                // pushdown path instead and so collapses freely even when a filter is present.
                String survivingKey = prunedVua.namedSubqueries().keySet().iterator().next();
                if (preserveViewBoundaries == false || prunedVua.isViewBranch(survivingKey) == false) {
                    // Recurse into the exposed child rather than just returning it. Collapsing can expose another
                    // BranchPlan — nested pass-through views under CPS produce exactly that — and transformDown
                    // descends into the *children* of whatever the rule returns, so the rule would never be applied
                    // to the newly exposed node itself, leaving its shadow branch unstripped and failing verification
                    // with "view-shadow lookup [...] not yet resolved". Terminates because each step removes a node.
                    return stripViewShadowRelations(prunedVua.children().getFirst(), preserveViewBoundaries);
                }
            }
            return pruned;
        });
    }

    /**
     * Post-freeze top-down rewrite that:
     * <ol>
     *   <li>Unwraps {@code Subquery[NamedSubquery[X]]} → {@code NamedSubquery[X]}</li>
     *   <li>Unwraps {@code Subquery[BranchPlan[...]]} → {@code BranchPlan[...]} so that the
     *       parent {@link UnionAll} can inline the view-branch entries in step 3.</li>
     *   <li>Converts plain {@link UnionAll} nodes containing at least one {@link NamedSubquery}
     *       or {@link BranchPlan} child into a single {@link BranchPlan} node, inlining all
     *       view branches from nested {@code BranchPlan} children</li>
     *   <li>Drops the branch wrappers {@link #preIndexResolution} preserved ({@link #unwrapBranchWrappers}).</li>
     * </ol>
     * This handles user-written {@code UNION ALL (FROM my_view)} where the parser creates a
     * {@link Subquery} wrapper and view resolution replaces its child with a {@link NamedSubquery}
     * or a {@link BranchPlan} (in the single-view case, view resolution always wraps in
     * {@link BranchPlan} so that {@link org.elasticsearch.xpack.esql.dsltranslate.ViewRequestFilterRewriter}
     * can identify and filter view boundaries after analysis).
     */
    static LogicalPlan rewriteUnionAllsWithNamedSubqueries(LogicalPlan plan) {
        // Unwrap Subquery[NamedSubquery[X]] → NamedSubquery[X]
        // Unwrap Subquery[BranchPlan[...]] → BranchPlan[...] so the parent UnionAll can inline it.
        plan = plan.transformDown(Subquery.class, sq -> switch (sq.child()) {
            case NamedSubquery n -> n;
            case BranchPlan vua -> vua;
            default -> sq;
        });

        plan = plan.transformDown(UnionAll.class, unionAll -> toBranchPlan(unionAll, false));
        plan = plan.transformDown(BranchPlan.class, ViewCompaction::unwrapBranchWrappers);
        return plan;
    }

    /**
     * Drops the {@link Subquery}/{@link NamedSubquery} wrappers of every branch. Post-freeze they are dead weight:
     * what they said about each branch is already frozen in {@code _class}/{@code _name} Evals (which sit <em>above</em>
     * any wrapper, so a frozen branch — {@code Eval[Subquery[...]]} — is left alone here and loses its wrapper to
     * {@code Analyzer.UnwrapNamedSubqueries} or the physical planner instead), and the branch's remaining meaning lives
     * in its {@link BranchIdentity}. The one exception is a {@link NamedSubquery} directly around an exclusion-bearing
     * {@link UnresolvedRelation}, which stays wrapped so the sibling merge step cannot widen the exclusion's scope
     * beyond the view body it came from (see {@link #containsExclusion}).
     */
    private static BranchPlan unwrapBranchWrappers(BranchPlan vua) {
        LinkedHashMap<String, LogicalPlan> entries = new LinkedHashMap<>();
        boolean changed = false;
        for (Map.Entry<String, LogicalPlan> entry : vua.namedSubqueries().entrySet()) {
            LogicalPlan value = unwrapBranchValue(entry.getValue());
            changed |= value != entry.getValue();
            entries.put(entry.getKey(), value);
        }
        return changed ? new BranchPlan(vua.source(), entries, vua.branchIdentities(), vua.output()) : vua;
    }

    /**
     * Strips a branch value's {@link Subquery}/{@link NamedSubquery} wrappers, except a {@link NamedSubquery} directly
     * around an exclusion-bearing {@link UnresolvedRelation} (see {@link #unwrapBranchWrappers} for why).
     */
    private static LogicalPlan unwrapBranchValue(LogicalPlan value) {
        while (value instanceof Subquery sq) {
            if (sq instanceof NamedSubquery && sq.child() instanceof UnresolvedRelation ur && containsExclusion(ur)) {
                break;
            }
            value = sq.child();
        }
        return value;
    }

    /**
     * Converts a plain {@link UnionAll} into a {@link BranchPlan} with a {@link BranchIdentity} per branch. Every
     * union is converted, whether or not views are involved — a literal subquery union gets SUBQUERY/INDEX identities.
     * (Dataset unions never pass through here: {@code DatasetRewriter} emits identity-carrying {@link BranchPlan}s
     * directly, and it is the only producer of unions with dataset branches.)
     *
     * @param preserveWrappers {@code true} in the pre-freeze phase ({@link #preIndexResolution}): branch values are
     *                         stored verbatim so the analyzer's metadata freeze still sees every wrapper. {@code false}
     *                         post-freeze: wrappers are compacted away as the branches are recorded.
     */
    private static LogicalPlan toBranchPlan(UnionAll unionAll, boolean preserveWrappers) {
        if (unionAll instanceof BranchPlan) {
            return unionAll;
        }
        LinkedHashMap<String, LogicalPlan> subPlans = new LinkedHashMap<>();
        // Structural truth only: what each branch is. Recorded unconditionally —
        // whether a boundary must survive compaction is decided separately, from
        // preserveViewBoundaries, at the points that would collapse it.
        LinkedHashMap<String, BranchIdentity> identities = new LinkedHashMap<>();
        int unnamedCount = 0;
        for (LogicalPlan child : unionAll.children()) {
            if (child instanceof NamedSubquery named) {
                assertSubqueryDoesNotExist(subPlans, named.name());
                subPlans.put(named.name(), preserveWrappers ? named : named.child());
                // A NamedSubquery is by construction a resolved view branch.
                identities.put(named.name(), BranchIdentity.view(named.name()));
            } else if (child instanceof BranchPlan vua) {
                // Inline the BranchPlan's named entries directly into this level, preserving
                // their view-branch status. This handles the case where view resolution wraps
                // a single view in a BranchPlan (e.g. FROM emp2, (FROM my_view) where the
                // user-written Subquery wrapper was unwrapped above and the inner BranchPlan
                // now appears as a direct child of this UnionAll).
                for (Map.Entry<String, LogicalPlan> entry : vua.namedSubqueries().entrySet()) {
                    assertSubqueryDoesNotExist(subPlans, entry.getKey());
                    subPlans.put(entry.getKey(), entry.getValue());
                    identities.put(entry.getKey(), vua.branchIdentity(entry.getKey()));
                }
            } else if (isSubqueryOfAView(child)) {
                // Only reachable pre-freeze: the Subquery wrapper answers _class at the freeze, its view still takes the filter.
                String name = ((NamedSubquery) ((Subquery) child).child()).name();
                assertSubqueryDoesNotExist(subPlans, name);
                subPlans.put(name, child);
                identities.put(name, BranchIdentity.view(name));
            } else if (child instanceof Subquery unnamed) {
                // A literal user-written subquery: NOT a view branch, even when its body resolved to a view union
                // ((FROM my_view) under CPS, or (FROM v1, v2)) — one identity per nesting level. The views inside keep
                // their own identities on the inner BranchPlan, a level deeper; post-freeze compaction lifts them
                // (liftedThrough is a no-op through a non-view branch, so they stay view branches for the filter).
                // Positional rather than content-derived: a toString() hash embeds NameIds, which depend on global
                // counter state and would make golden/EXPLAIN output nondeterministic across runs. Uniquified rather
                // than asserted: compaction lifts can merge unions whose branches used the same ordinals.
                String name = makeUniqueKey(subPlans, "unnamed_view_" + ++unnamedCount);
                subPlans.put(name, preserveWrappers ? unnamed : unnamed.child());
                identities.put(name, BranchIdentity.SUBQUERY);
            } else {
                // Bare plan: NOT a view branch. The first bare branch keeps the null key ("always a bare-plan branch"
                // to readers like the request-filter rewriter); further bare branches get uniquified "main" labels.
                String key = subPlans.containsKey(null) ? makeUniqueKey(subPlans, null) : null;
                subPlans.put(key, child);
                identities.put(key, BranchIdentity.INDEX);
            }
        }
        return new BranchPlan(unionAll.source(), subPlans, identities, unionAll.output());
    }

    /**
     * A literal subquery directly wrapping a resolved view's {@link NamedSubquery}: the only shape where a single
     * branch level carries both the subquery wrapper (the request point, consumed by the metadata freeze) and the
     * view (the boundary, recorded as the branch identity) without an intervening {@link BranchPlan} level to hold
     * the view's identity. Retires when {@code ViewResolver} stops emitting {@link NamedSubquery} wrappers in favor
     * of identity-carrying single-branch unions.
     */
    private static boolean isSubqueryOfAView(LogicalPlan plan) {
        return plan instanceof Subquery sq && sq instanceof NamedSubquery == false && sq.child() instanceof NamedSubquery;
    }

    /**
     * Bottom-up rewrite that flattens nested {@link BranchPlan} structures and merges sibling
     * bare {@link UnresolvedRelation}s at each level. See {@link #tryFlattenBranchPlan}.
     * <p>
     * Also unwraps {@link NamedSubquery} entries whose child has been reduced to a bare
     * {@link UnresolvedRelation} without exclusions. The wrap was added by
     * {@code ViewResolver.buildPlanFromBranches} purely because the original child wasn't a bare
     * {@link UnresolvedRelation}; once nested compaction reduces it to one, the wrapper has no
     * purpose and would block the outer level's sibling {@link UnresolvedRelation} merge step from
     * seeing it. Exclusion-bearing {@link UnresolvedRelation}s stay wrapped to preserve their
     * narrow scope (see exclusion-leak tests).
     */
    static LogicalPlan compactNestedBranchPlans(LogicalPlan plan, boolean preserveViewBoundaries, int maxBranchCount) {
        List<LogicalPlan> children = plan.children();
        List<LogicalPlan> newChildren = null;
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            LogicalPlan newChild = compactNestedBranchPlans(child, preserveViewBoundaries, maxBranchCount);
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
        if (current instanceof BranchPlan vua) {
            return tryFlattenBranchPlan(vua, preserveViewBoundaries, maxBranchCount);
        }
        return current;
    }

    private static LogicalPlan tryFlattenBranchPlan(BranchPlan vua, boolean preserveViewBoundaries, int maxBranchCount) {
        // Trial pass: collect all entries from full flattening and check for conflicts.
        // Inner BranchPlans that only contain UnresolvedRelations are lifted into the parent,
        // eliminating nesting that the runtime doesn't yet support.
        // Inner MergePlans (from user-written subqueries inside views) are also lifted,
        // with each child becoming a separate named entry suffixed from the parent view name.
        LinkedHashMap<String, LogicalPlan> flat = new LinkedHashMap<>();
        // Tracks what each key in `flat` is, propagated from the identities the lifted entries already carry.
        LinkedHashMap<String, BranchIdentity> flatIdentities = new LinkedHashMap<>();

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
                // Propagate identity: a view whose body is a bare UnresolvedRelation
                // is still a view branch (the view is a pass-through to an index/alias).
                flatIdentities.put(assignedKey, vua.branchIdentity(key));
            } else {
                if (flat.containsKey(key)) {
                    return vua; // conflict
                }
                flat.put(key, value);
                // Propagate identity from the outer BranchPlan.
                flatIdentities.put(key, vua.branchIdentity(key));
            }
        }

        for (Map.Entry<String, LogicalPlan> entry : mergeEntries) {
            String parentKey = entry.getKey();
            LogicalPlan value = entry.getValue();
            LogicalPlan inner = (value instanceof NamedSubquery ns) ? ns.child() : value;
            if (inner instanceof BranchPlan innerVua) {
                // Named branches from inner BranchPlan: lift with their own names. A bare
                // UnresolvedRelation with an exclusion must be wrapped in a NamedSubquery before
                // lifting — otherwise the subsequent merge step would concatenate its pattern list
                // with a sibling outer UnresolvedRelation, widening the exclusion's scope beyond
                // the inner view body it came from.
                for (Map.Entry<String, LogicalPlan> innerEntry : innerVua.namedSubqueries().entrySet()) {
                    String innerKey = innerEntry.getKey();
                    LogicalPlan innerValue = innerEntry.getValue();
                    boolean bareRelationPiece = innerValue instanceof UnresolvedRelation;
                    if (bareRelationPiece && containsExclusion((UnresolvedRelation) innerValue)) {
                        innerValue = new NamedSubquery(innerValue.source(), (UnresolvedRelation) innerValue, innerKey);
                    }
                    String assignedKey = makeUniqueKey(flat, innerKey);
                    flat.put(assignedKey, innerValue);
                    // Lift the inner identity through the outer branch: inside a view branch, the outer view's name
                    // wins (METADATA _name answers the outermost view) and the lineage chain is extended, so that the
                    // request filter still binds at the lifted piece's output — a piece that computes a column (e.g.
                    // Eval-topped) cannot take the raw Lucene filter path. The one exception is a bare non-view
                    // relation piece: with no computation above its source the Lucene path is equivalent, and keeping
                    // its own identity lets it merge with sibling bare patterns into the plain "main" branch.
                    BranchIdentity innerIdentity = innerVua.branchIdentity(innerKey);
                    flatIdentities.put(
                        assignedKey,
                        bareRelationPiece && innerIdentity.isView() == false
                            ? innerIdentity
                            : innerIdentity.liftedThrough(vua.branchIdentity(parentKey))
                    );
                }
            } else {
                // Plain MergePlan from user-written subqueries: lift children with suffixed
                // parent name. As in the BranchPlan branch above, a bare UnresolvedRelation child
                // with an exclusion must be wrapped in a NamedSubquery before lifting so the
                // subsequent merge step does not widen its scope.
                // This also lifts a body-topping Fork, dissolving the Fork node: results are
                // unchanged because the parser already attached the per-branch _fork Evals, and
                // the pieces keep the view identity below. What the dissolved node no longer gets
                // is Fork's own post-analysis verification (Fork.checkFork); its pieces are
                // checked by the enclosing union's instead — to be unified when FORK folds into
                // the MergePlan/BranchIdentity model.
                MergePlan mergePlan = (MergePlan) inner;
                int childIndex = 1;
                // A view whose body is a bare union lifts into one branch per body piece; each piece is still part of
                // the view, so the branch keeps the view's identity. The request filter distributes over a union, so
                // filtering each lifted piece's output equals filtering the un-lifted view's output — while losing the
                // mark would silently send the raw DSL into the piece's source scan instead.
                BranchIdentity parentIdentity = vua.branchIdentity(parentKey);
                for (LogicalPlan child : mergePlan.children()) {
                    LogicalPlan unwrapped = (child instanceof Subquery sq) ? sq.child() : child;
                    String childKey = parentKey + "~" + childIndex++;
                    if (unwrapped instanceof UnresolvedRelation childUr && containsExclusion(childUr)) {
                        unwrapped = new NamedSubquery(childUr.source(), childUr, childKey);
                    }
                    String assignedKey = makeUniqueKey(flat, childKey);
                    flat.put(assignedKey, unwrapped);
                    flatIdentities.put(assignedKey, parentIdentity);
                }
            }
        }

        // Merge bare UnresolvedRelation siblings that ended up at the same level after
        // flattening. ViewResolver.buildPlanFromBranches only merges URs within a single
        // nesting level; after nested BranchPlans are hoisted into this flat map, URs from
        // different levels may become adjacent and mergeable.
        //
        // No alias resolver is available here, so alias-vs-backing-index overlap is not
        // detected. This is safe in the production pipeline: by the time postIndexResolution
        // runs, ResolveTable has already converted every UnresolvedRelation to an EsRelation,
        // making the merge loop a no-op. The gap only exists in the apply() test convenience
        // (which runs both phases without ResolveTable in between), and only for the
        // multi-level nesting variant of the alias scenario — not a production concern.
        mergeUnresolvedRelationEntries(flat, flatIdentities);
        // Drop identities of entries the merge step removed.
        flatIdentities.keySet().retainAll(flat.keySet());

        if (flat.size() > maxBranchCount) {
            return vua; // flattening would exceed max_branch_count, keep the nested structure
        }
        if (flat.size() == 1) {
            String survivingKey = flat.keySet().iterator().next();
            LogicalPlan survivingPlan = flat.values().iterator().next();
            // Same two-part decision as in stripViewShadowRelations: collapse the lone entry unless a
            // request filter still needs this view boundary. With no filter this always collapses, so
            // the pre-feature compaction is preserved in full.
            if (preserveViewBoundaries == false || flatIdentities.get(survivingKey).isView() == false) {
                return survivingPlan;
            }
        }
        return new BranchPlan(vua.source(), flat, flatIdentities, vua.output());
    }

    /**
     * Generate a unique key for the flat map, avoiding collisions with existing entries.
     */
    // The separator must not be `#` — see ViewResolver#makeUniqueKey.
    private static String makeUniqueKey(LinkedHashMap<String, LogicalPlan> flat, String key) {
        if (key == null) {
            key = "main";
        }
        String original = key;
        int counter = 2;
        while (flat.containsKey(key)) {
            key = original + "~" + counter++;
        }
        return key;
    }

    /**
     * Merges bare {@link UnresolvedRelation} entries in the map into a single entry where possible,
     * using string-equality and wildcard overlap as guards. Called after nested {@link BranchPlan}s
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
    private static void mergeUnresolvedRelationEntries(LinkedHashMap<String, LogicalPlan> flat, Map<String, BranchIdentity> identities) {
        List<String> urKeys = new ArrayList<>();
        for (Map.Entry<String, LogicalPlan> entry : flat.entrySet()) {
            // An exclusion's scope is its branch's own pattern list: "inner-*,-*b" excludes only from inner-*.
            // Concatenating a sibling into it (or it into a sibling) widens that scope, so exclusion-bearing
            // branches never participate in this cross-branch merge. ViewResolver's own per-level merge is
            // different: there the resolver concatenates patterns it split from one user-written list, where
            // the exclusion legitimately spans the merged result.
            if (entry.getValue() instanceof UnresolvedRelation ur && containsExclusion(ur) == false) {
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
            if (identities.getOrDefault(key, BranchIdentity.INDEX).isView()) {
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
     * Merge the unresolved relation unless either relation contains an exclusion (merging would widen
     * its scope), unless the index patterns contain matching index names, or
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
        if (metadataNames(main).equals(metadataNames(other)) == false) {
            return null;
        }
        String[] otherPatterns = other.indexPattern().indexPattern().split(",");
        for (String mainPattern : main.indexPattern().indexPattern().split(",")) {
            for (String otherPattern : otherPatterns) {
                // An exclusion applies to the patterns before it in the merged list, so merging would widen its scope.
                if (isExclusion(mainPattern) || isExclusion(otherPattern)) {
                    return null;
                }
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
     * Merge the unresolved relation unless either relation contains an exclusion or the index patterns
     * overlap by string equality or wildcard.
     * Alias resolution is not performed; this overload is used by {@link #mergeUnresolvedRelationEntries}
     * after nested {@link BranchPlan} flattening, where no alias resolver is available. In the
     * production pipeline this is always a no-op because {@code ResolveTable} has already converted
     * every {@link UnresolvedRelation} to an {@code EsRelation} before {@link #postIndexResolution} runs.
     */
    static UnresolvedRelation mergeIfPossible(UnresolvedRelation main, UnresolvedRelation other) {
        return mergeIfPossible(main, other, null);
    }

    private static Set<String> metadataNames(UnresolvedRelation relation) {
        Set<String> names = new HashSet<>();
        for (NamedExpression field : relation.metadataFields()) {
            names.add(field instanceof UnresolvedMetadataAttributeExpression unresolved ? unresolved.pattern() : field.name());
        }
        return names;
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
            if (isExclusion(pattern)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isExclusion(String pattern) {
        if (pattern.startsWith("-")) {
            return true;
        }
        var split = RemoteClusterAware.splitIndexName(pattern);
        return split.clusterAlias() != null && split.indexExpression().startsWith("-");
    }

    private static void assertSubqueryDoesNotExist(Map<String, LogicalPlan> plans, String name) {
        if (plans.containsKey(name)) {
            String message = name == null ? "Un-named subquery already exists" : "Named subquery already exists: " + name;
            throw new IllegalStateException(message);
        }
    }
}
