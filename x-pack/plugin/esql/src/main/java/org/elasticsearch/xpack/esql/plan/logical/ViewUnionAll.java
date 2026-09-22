/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A {@link UnionAll} produced by view resolution, as opposed to user-written subqueries.
 * This type marker allows {@link org.elasticsearch.xpack.esql.view.ViewResolver} to distinguish
 * between unions it has already processed (view-produced) and unions from the parser (subqueries)
 * that may still contain unresolved view references.
 *
 * <p>Not every entry in {@link #namedSubqueries()} is an actual resolved view. The map also
 * carries bare-index branches (assigned keys like {@code "main"} by {@link
 * org.elasticsearch.xpack.esql.view.ViewResolver}) and user-written literal subquery branches
 * (assigned synthetic {@code "unnamed_view_<hash>"} keys by {@link
 * org.elasticsearch.xpack.esql.view.ViewCompaction}). Only the keys in {@link #viewBranchKeys()}
 * are actual resolved views; callers that need to apply view-specific logic (e.g. the request
 * filter rewriter) must check {@link #isViewBranch(String)} rather than {@code key != null}.
 */
public class ViewUnionAll extends UnionAll {
    private final LinkedHashMap<String, LogicalPlan> namedSubqueries = new LinkedHashMap<>();
    /** Keys that are actual resolved view subplans, as opposed to bare-index or literal-subquery branches. */
    private final Set<String> viewBranchKeys;

    /**
     * Creates a {@link ViewUnionAll} with explicit view-branch tracking.
     *
     * @param viewBranchKeys the keys in {@code children} that are actual resolved view subplans;
     *                       bare-index and user-written subquery keys must not be included.
     */
    public ViewUnionAll(Source source, LinkedHashMap<String, LogicalPlan> children, Set<String> viewBranchKeys, List<Attribute> output) {
        super(source, children.values().stream().toList(), output);
        namedSubqueries.putAll(children);
        this.viewBranchKeys = Set.copyOf(viewBranchKeys);
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new ViewUnionAll(source(), asSubqueryMap(newChildren), viewBranchKeys, output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, ViewUnionAll::new, namedSubqueries, viewBranchKeys, output());
    }

    @Override
    public ViewUnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new ViewUnionAll(source(), asSubqueryMap(subPlans), viewBranchKeys, output());
    }

    @Override
    public ViewUnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new ViewUnionAll(source(), asSubqueryMap(subPlans), viewBranchKeys, output);
    }

    // Currently for testing only, could also be useful for EXPLAIN and PROFILE
    public Map<String, LogicalPlan> namedSubqueries() {
        return namedSubqueries;
    }

    /**
     * Returns the set of keys in {@link #namedSubqueries()} that correspond to actual resolved view subplans.
     * Keys for bare-index branches (e.g. {@code "main"}) and literal user-written subquery branches
     * (e.g. {@code "unnamed_view_<hash>"}) are excluded.
     */
    public Set<String> viewBranchKeys() {
        return viewBranchKeys;
    }

    /**
     * Returns {@code true} if the given key corresponds to an actual resolved view branch.
     * Use this instead of {@code key != null} checks; bare-index and literal-subquery branches also have non-null keys.
     * Returns {@code false} for {@code null} keys, which are always bare-plan branches.
     */
    public boolean isViewBranch(String key) {
        return key != null && viewBranchKeys.contains(key);
    }

    private LinkedHashMap<String, LogicalPlan> asSubqueryMap(List<LogicalPlan> children) {
        if (children.size() != namedSubqueries.size()) {
            throw new IllegalArgumentException(
                "ViewUnionAll.replaceChildren expects a 1:1 positional replacement; use pruneEmptyBranches"
                    + " to drop branches and preserve the named-subqueries invariant."
            );
        }
        // Read-only iterator: unlike sequencedKeySet(), calling next() here does not remove
        // entries from (and therefore does not corrupt) this instance's own namedSubqueries.
        Iterator<String> names = namedSubqueries.keySet().iterator();
        LinkedHashMap<String, LogicalPlan> newSubqueries = new LinkedHashMap<>();
        for (LogicalPlan child : children) {
            newSubqueries.put(names.next(), child);
        }
        return newSubqueries;
    }

    /**
     * Name-aware override of {@link UnionAll#pruneEmptyBranches(Predicate)}: filters the
     * named-subqueries map directly so the surviving children keep their original names. Like
     * the base, single-survivor wrappers are preserved — callers that want to collapse to the
     * lone child do so explicitly.
     */
    @Override
    public LogicalPlan pruneEmptyBranches(Predicate<LogicalPlan> isEmpty) {
        LinkedHashMap<String, LogicalPlan> kept = new LinkedHashMap<>();
        for (Map.Entry<String, LogicalPlan> entry : namedSubqueries.entrySet()) {
            if (isEmpty.test(entry.getValue()) == false) {
                kept.put(entry.getKey(), entry.getValue());
            }
        }
        if (kept.size() == namedSubqueries.size()) {
            return this;
        }
        // Retain only view-branch keys whose branches survived pruning.
        Set<String> keptViewBranchKeys = viewBranchKeys.stream().filter(kept::containsKey).collect(Collectors.toSet());
        return new ViewUnionAll(source(), kept, keptViewBranchKeys, output());
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName()).append("[[");
        boolean first = true;
        for (String key : namedSubqueries.keySet()) {
            if (first == false) {
                sb.append(", ");
            }
            first = false;
            sb.append(mapper.index(key));
        }
        sb.append("]]");
    }

    @Override
    public int hashCode() {
        // Standard Map.hashCode() uses sum of (key ^ value) per entry, which is separable:
        // swapping values between keys can produce the same sum. Instead, we use multiplication
        // (non-separable) so that each key is bound to its value in the hash.
        int h = 0;
        for (Map.Entry<String, LogicalPlan> entry : namedSubqueries.entrySet()) {
            int k = Objects.hashCode(entry.getKey());
            int v = Objects.hashCode(entry.getValue());
            h += k * (v + 1);
        }
        return Objects.hash(ViewUnionAll.class, h, viewBranchKeys);
    }

    @Override
    public boolean equals(Object o) {
        // Map equality is order independent, but does require the same keys map to the same sub-plans
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ViewUnionAll other = (ViewUnionAll) o;

        return Objects.equals(namedSubqueries, other.namedSubqueries()) && Objects.equals(viewBranchKeys, other.viewBranchKeys);
    }
}
