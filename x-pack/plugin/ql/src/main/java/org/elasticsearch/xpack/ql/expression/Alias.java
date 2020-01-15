/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * An {@code Alias} is a {@code NamedExpression} that gets renamed to something else through the Alias.
 *
 * For example, in the statement {@code 5 + 2 AS x}, {@code x} is an alias which is points to {@code ADD(5, 2)}.
 *
 * And in {@code SELECT col AS x} "col" is a named expression that gets renamed to "x" through an alias.
 *
 */
public class Alias extends NamedExpression {

    private final Expression child;
    private final String qualifier;

    /**
     * Postpone attribute creation until it is actually created.
     * Being immutable, create only one instance.
     */
    private Attribute lazyAttribute;

    public Alias(Source source, String name, Expression child) {
        this(source, name, null, child, null);
    }

    public Alias(Source source, String name, String qualifier, Expression child) {
        this(source, name, qualifier, child, null);
    }

    public Alias(Source source, String name, String qualifier, Expression child, NameId id) {
        this(source, name, qualifier, child, id, false);
    }

    public Alias(Source source, String name, String qualifier, Expression child, NameId id, boolean synthetic) {
        super(source, name, singletonList(child), id, synthetic);
        this.child = child;
        this.qualifier = qualifier;
    }

    @Override
    protected NodeInfo<Alias> info() {
        return NodeInfo.create(this, Alias::new, name(), qualifier, child, id(), synthetic());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new Alias(source(), name(), qualifier, newChildren.get(0), id(), synthetic());
    }

    public Expression child() {
        return child;
    }

    public String qualifier() {
        return qualifier;
    }

    public String qualifiedName() {
        return qualifier == null ? name() : qualifier + "." + name();
    }

    @Override
    public Nullability nullable() {
        return child.nullable();
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public Attribute toAttribute() {
        if (lazyAttribute == null) {
            lazyAttribute = resolved() == true ?
                new ReferenceAttribute(source(), name(), dataType(), qualifier, nullable(), id(), synthetic()) :
                new UnresolvedAttribute(source(), name(), qualifier);
        }
        return lazyAttribute;
    }

    @Override
    public String toString() {
        return child + " AS " + name() + "#" + id();
    }
}
