/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SequencedSet;
import java.util.function.Predicate;

/**
 * A {@link UnionAll} produced by view resolution, as opposed to user-written subqueries.
 * This type marker allows {@link org.elasticsearch.xpack.esql.view.ViewResolver} to distinguish
 * between unions it has already processed (view-produced) and unions from the parser (subqueries)
 * that may still contain unresolved view references.
 */
public class ViewUnionAll extends UnionAll {
    private final LinkedHashMap<String, LogicalPlan> namedSubqueries = new LinkedHashMap<>();

    public ViewUnionAll(Source source, LinkedHashMap<String, LogicalPlan> children, List<Attribute> output) {
        super(source, children.values().stream().toList(), output);
        namedSubqueries.putAll(children);
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new ViewUnionAll(source(), asSubqueryMap(newChildren), output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, ViewUnionAll::new, namedSubqueries, output());
    }

    @Override
    public ViewUnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new ViewUnionAll(source(), asSubqueryMap(subPlans), output());
    }

    @Override
    public ViewUnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new ViewUnionAll(source(), asSubqueryMap(subPlans), output);
    }

    // Currently for testing only, could also be useful for EXPLAIN and PROFILE
    public Map<String, LogicalPlan> namedSubqueries() {
        return namedSubqueries;
    }

    private LinkedHashMap<String, LogicalPlan> asSubqueryMap(List<LogicalPlan> children) {
        SequencedSet<String> names = namedSubqueries.sequencedKeySet();
        assert children.size() == names.size()
            : "ViewUnionAll.replaceChildren expects a 1:1 positional replacement; use pruneEmptyBranches"
                + " to drop branches and preserve the named-subqueries invariant.";
        LinkedHashMap<String, LogicalPlan> newSubqueries = new LinkedHashMap<>();
        for (LogicalPlan child : children) {
            newSubqueries.put(names.removeFirst(), child);
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
        return new ViewUnionAll(source(), kept, output());
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format) {
        sb.append(nodeName()).append("[").append(namedSubqueries.keySet()).append("]");
    }

    @Override
    public int hashCode() {
        // Standard Map.hashCode() uses sum of (key ^ value) per entry, which is separable:
        // swapping values between keys can produce the same sum. Instead, we use multiplication
        // (non-separable) so that each key is bound to its value in the hash.
        int h = 0;
        for (Map.Entry<String, LogicalPlan> entry : namedSubqueries.entrySet()) {
            int k = entry.getKey().hashCode();
            int v = Objects.hashCode(entry.getValue());
            h += k * (v + 1);
        }
        return Objects.hash(ViewUnionAll.class, h);
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

        return Objects.equals(namedSubqueries, other.namedSubqueries());
    }
}
