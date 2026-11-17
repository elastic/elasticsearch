/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.RelationClass;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * A {@link Subquery} that wraps a {@code LET} binding substituted by {@code LetResolver}, carrying the binding name so that
 * {@code METADATA _name} can answer it ({@code _class} answers {@link RelationClass#SUBQUERY}). The analyzer's metadata
 * freeze consumes the name. A plain {@code NamedSubquery} is not a view: {@link ViewSubquery} is the view subclass, and code
 * that means "a view" must test for that.
 * <p>
 * Unlike plain {@link Subquery}, the name participates in {@link #equals} and {@link #hashCode}, which allows
 * {@code Node.transformDown} to distinguish a newly-tagged subquery from its untagged predecessor.
 * <p>
 * This class should only be used during query re-writing and not survive analysis, so it needs no serialization of its
 * own.
 */
public class NamedSubquery extends Subquery {
    private final String name;

    public NamedSubquery(Source source, LogicalPlan subqueryPlan, String name) {
        super(source, subqueryPlan);
        this.name = Objects.requireNonNull(name);
    }

    /** The name {@code METADATA _name} answers. */
    public String name() {
        return name;
    }

    /** The kind {@code METADATA _class} answers. */
    public RelationClass relationClass() {
        return RelationClass.SUBQUERY;
    }

    @Override
    protected NodeInfo<? extends NamedSubquery> info() {
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
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName()).append('[').append(mapper.index(name)).append(']');
    }
}
