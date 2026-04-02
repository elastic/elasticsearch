/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * A {@link Subquery} that carries the view name it was resolved from.
 * <p>
 * Unlike plain {@link Subquery}, the name participates in {@link #equals} and {@link #hashCode},
 * which allows {@code Node.transformDown} to distinguish a newly-tagged subquery from its
 * untagged predecessor. After view resolution, a post-processing pass converts
 * {@link UnionAll} nodes containing {@code NamedSubquery} children into {@link ViewUnionAll}.
 * <p>
 * This class should only be used during query re-writing and not survive in the final query plan.
 * If we decide to keep named subqueries as a feature later, we should add serialization support.
 */
public class NamedSubquery extends Subquery {
    private final String name;

    public NamedSubquery(Source source, LogicalPlan subqueryPlan, String name) {
        super(source, subqueryPlan);
        this.name = Objects.requireNonNull(name);
    }

    public String name() {
        return name;
    }

    @Override
    protected NodeInfo<NamedSubquery> info() {
        return NodeInfo.create(this, NamedSubquery::new, child(), name);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new NamedSubquery(source(), newChild, name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NamedSubquery other = (NamedSubquery) obj;
        return Objects.equals(name, other.name) && Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString(NodeStringFormat format) {
        return nodeName() + "[" + name + "]";
    }
}
