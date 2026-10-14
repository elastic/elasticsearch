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

import java.util.Collections;
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
 * (assigned synthetic {@code "unnamed_view_<n>"} keys by {@link
 * org.elasticsearch.xpack.esql.view.ViewCompaction}). Only the keys in {@link #viewBranchKeys()}
 * are actual resolved views; callers that need to apply view-specific logic (e.g. the request
 * filter rewriter) must check {@link #isViewBranch(String)} rather than {@code key != null}.
 */
public class ViewUnionAll extends UnionAll {
    private final LinkedHashMap<String, LogicalPlan> namedSubqueries = new LinkedHashMap<>();
    /** What each branch is — view (with its real name), subquery, or bare index — keyed like {@link #namedSubqueries}. */
    private final Map<String, BranchIdentity> branchIdentities;

    /**
     * Creates a {@link ViewUnionAll} with an explicit {@link BranchIdentity} per branch.
     *
     * @param branchIdentities identity per key in {@code children}; a missing entry means a bare index-pattern branch.
     */
    public ViewUnionAll(
        Source source,
        LinkedHashMap<String, LogicalPlan> children,
        Map<String, BranchIdentity> branchIdentities,
        List<Attribute> output
    ) {
        super(source, children.values().stream().toList(), output);
        namedSubqueries.putAll(children);
        // Never probe the caller's map by key: bare branches use the null key, which immutable maps (Map.of) reject.
        // Iterate the caller's entries instead, keeping only identities for keys that are actually branches — a caller
        // like pruneEmptyBranches passes its full pre-prune identity map and relies on the dropped keys being ignored.
        LinkedHashMap<String, BranchIdentity> normalized = new LinkedHashMap<>();
        for (String key : children.keySet()) {
            normalized.put(key, BranchIdentity.INDEX);
        }
        for (Map.Entry<String, BranchIdentity> entry : branchIdentities.entrySet()) {
            if (normalized.containsKey(entry.getKey())) {
                normalized.put(entry.getKey(), entry.getValue());
            }
        }
        this.branchIdentities = Collections.unmodifiableMap(normalized);
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new ViewUnionAll(source(), asSubqueryMap(newChildren), branchIdentities, output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            (source, children, identities, output) -> new ViewUnionAll(source, children, identities, output),
            namedSubqueries,
            branchIdentities,
            output()
        );
    }

    @Override
    public ViewUnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new ViewUnionAll(source(), asSubqueryMap(subPlans), branchIdentities, output());
    }

    @Override
    public ViewUnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new ViewUnionAll(source(), asSubqueryMap(subPlans), branchIdentities, output);
    }

    // Currently for testing only, could also be useful for EXPLAIN and PROFILE
    public Map<String, LogicalPlan> namedSubqueries() {
        return namedSubqueries;
    }

    /** What each branch is, keyed like {@link #namedSubqueries()}; every key has an entry. */
    public Map<String, BranchIdentity> branchIdentities() {
        return branchIdentities;
    }

    /** The identity of the branch stored under {@code key}; {@link BranchIdentity#INDEX} for unknown keys. */
    public BranchIdentity branchIdentity(String key) {
        return branchIdentities.getOrDefault(key, BranchIdentity.INDEX);
    }

    /**
     * Returns the set of keys in {@link #namedSubqueries()} that correspond to actual resolved view subplans.
     * Keys for bare-index branches (e.g. {@code "main"}) and literal user-written subquery branches
     * (e.g. {@code "unnamed_view_<n>"}) are excluded. Note these are branch <em>labels</em>; the view's real
     * name is {@link #branchIdentity(String)}'s {@link BranchIdentity#name()}, which uniquification never mangles.
     */
    public Set<String> viewBranchKeys() {
        return branchIdentities.entrySet()
            .stream()
            .filter(e -> e.getValue().isView())
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Returns {@code true} if the given key corresponds to an actual resolved view branch.
     * Use this instead of {@code key != null} checks; bare-index and literal-subquery branches also have non-null keys.
     * Returns {@code false} for {@code null} keys, which are always bare-plan branches.
     */
    public boolean isViewBranch(String key) {
        return key != null && branchIdentity(key).isView();
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
        // The identity constructor retains only the surviving keys' identities.
        return new ViewUnionAll(source(), kept, branchIdentities, output());
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
        return Objects.hash(ViewUnionAll.class, h, branchIdentities, output().size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // Delegate the children/output part to UnionAll: output arity is resolution state (compaction builds this node
        // with an empty output for resolveMergePlan to fill in) and LOAD_ALL alignment rewrites the output without
        // changing children; ignoring the output here would make transformUp discard both rewrites. super.equals also
        // rejects nulls and other classes via getClass().
        if (super.equals(o) == false) {
            return false;
        }
        ViewUnionAll other = (ViewUnionAll) o;

        // The map comparison requires the same keys to map to the same sub-plans; branch order is already pinned
        // by the positional children comparison in super.equals.
        return Objects.equals(namedSubqueries, other.namedSubqueries()) && Objects.equals(branchIdentities, other.branchIdentities);
    }
}
