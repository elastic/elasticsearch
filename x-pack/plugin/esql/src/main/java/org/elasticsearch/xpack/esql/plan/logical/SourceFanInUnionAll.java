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
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A {@link UnionAll} produced by dataset source expansion, as opposed to user-written subqueries
 * or {@link ViewUnionAll}. Children are independently distributable source plans for one
 * resolved {@code FROM}. Nested source fan-ins are flattened into this node.
 */
public class SourceFanInUnionAll extends UnionAll {

    /**
     * Builds a source-fan-in union whose children are independently distributable producers.
     */
    public SourceFanInUnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, flattenSourceFanInChildren(children), output);
    }

    /**
     * Lifts nested {@link SourceFanInUnionAll} children into this node so a view whose body is
     * already a multi-source {@code FROM} can be composed with another source.
     */
    static List<LogicalPlan> flattenSourceFanInChildren(List<LogicalPlan> children) {
        boolean needsFlatten = false;
        for (LogicalPlan child : children) {
            if (child instanceof SourceFanInUnionAll) {
                needsFlatten = true;
                break;
            }
        }
        if (needsFlatten == false) {
            return children;
        }
        List<LogicalPlan> flattened = new ArrayList<>(children.size());
        for (LogicalPlan child : children) {
            if (child instanceof SourceFanInUnionAll nested) {
                flattened.addAll(nested.children());
            } else {
                flattened.add(child);
            }
        }
        return flattened;
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new SourceFanInUnionAll(source(), newChildren, output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, SourceFanInUnionAll::new, children(), output());
    }

    @Override
    public SourceFanInUnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new SourceFanInUnionAll(source(), subPlans, output());
    }

    @Override
    public SourceFanInUnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new SourceFanInUnionAll(source(), subPlans, output);
    }

    @Override
    public SourceFanInUnionAll refreshOutput() {
        return new SourceFanInUnionAll(source(), children(), refreshedOutput());
    }

    @Override
    public ExecuteLocation executesOn() {
        return ExecuteLocation.ANY;
    }

    @Override
    public LogicalPlan pruneEmptyBranches(Predicate<LogicalPlan> isEmpty) {
        List<LogicalPlan> kept = new ArrayList<>(children().size());
        for (LogicalPlan child : children()) {
            if (isEmpty.test(child) == false) {
                kept.add(child);
            }
        }
        if (kept.size() == children().size()) {
            return this;
        }
        return new SourceFanInUnionAll(source(), kept, output());
    }

    @Override
    public int hashCode() {
        return Objects.hash(SourceFanInUnionAll.class, children());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceFanInUnionAll other = (SourceFanInUnionAll) o;
        return Objects.equals(children(), other.children());
    }
}
