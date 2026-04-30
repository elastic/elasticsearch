/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rule that compacts the nested plan produced by {@link ViewResolver} into the form expected by the
 * rest of the query pipeline. This work used to live as a post-pass at the bottom of
 * {@link ViewResolver#replaceViews}, but was moved to the analyzer so the resolver can return an
 * uncompacted plan — needed for CPS to attach lenient field-caps calls to specific resolution levels
 * (see <a href="https://github.com/elastic/esql-planning/issues/543">esql-planning#543</a> and
 * <a href="https://github.com/elastic/esql-planning/issues/472">#472</a>).
 * <p>
 * Behaviour is intended to be identical to the previous in-resolver compaction:
 * <ol>
 *   <li>{@link #rewriteUnionAllsWithNamedSubqueries} — convert plain {@link UnionAll} containing at
 *       least one {@link NamedSubquery} child into {@link ViewUnionAll}.</li>
 *   <li>{@link #compactNestedViewUnionAlls} — bottom-up flatten nested {@link ViewUnionAll} (and
 *       merge sibling bare {@link UnresolvedRelation}s when an inner Fork is lifted). Also unwraps
 *       {@link NamedSubquery} wrappers whose child has been reduced to a bare
 *       {@link UnresolvedRelation} without exclusions, so the outer level's compaction can see them
 *       as bare {@link UnresolvedRelation}s.</li>
 *   <li>Unwrap remaining {@link NamedSubquery} wrappers — they only existed to defeat the merge
 *       step in scope-sensitive cases.</li>
 * </ol>
 * <p>
 * Note: a small amount of compaction stays inside {@link ViewResolver#buildPlanFromBranches} —
 * specifically the per-level sibling {@link UnresolvedRelation} merge — because
 * {@link ViewUnionAll} (a {@link Fork} subclass) enforces {@link Fork#MAX_BRANCHES} at construction
 * time, and a wide branching level of compactable views would be unconstructible without first
 * reducing the entry count.
 */
public class ViewCompaction extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        plan = rewriteUnionAllsWithNamedSubqueries(plan);
        plan = compactNestedViewUnionAlls(plan);
        plan = plan.transformDown(NamedSubquery.class, UnaryPlan::child);
        return plan;
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

        if (hasInnerFork == false) {
            return vua;
        }

        // Try to merge all UnresolvedRelation entries into a single one, unless there are duplicates
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

    private static boolean containsExclusion(UnresolvedRelation ur) {
        for (String pattern : ur.indexPattern().indexPattern().split(",")) {
            if (pattern.startsWith("-")) {
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
