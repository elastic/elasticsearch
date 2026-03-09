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
import java.util.SequencedSet;

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
    public UnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new ViewUnionAll(source(), asSubqueryMap(subPlans), output());
    }

    @Override
    public Fork replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new ViewUnionAll(source(), asSubqueryMap(subPlans), output);
    }

    private LinkedHashMap<String, LogicalPlan> asSubqueryMap(List<LogicalPlan> children) {
        SequencedSet<String> names = namedSubqueries.sequencedKeySet();
        assert children.size() == names.size();
        LinkedHashMap<String, LogicalPlan> newSubqueries = new LinkedHashMap<>();
        for (LogicalPlan child : children) {
            newSubqueries.put(names.removeFirst(), child);
        }
        return newSubqueries;
    }
}
