/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

/**
 * An {@code Alias} is a {@code NamedExpression} that gets renamed to something else through the Alias.
 *
 * For example, in the statement {@code 5 + 2 AS x}, {@code x} is an alias which is points to {@code ADD(5, 2)}.
 *
 * And in {@code SELECT col AS x} "col" is a named expression that gets renamed to "x" through an alias.
 *
 */
public final class Alias extends NamedExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(NamedExpression.class, "Alias", Alias::new);

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

    public Alias(StreamInput in) throws IOException {
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readString(),
            in.readOptionalString(),
            in.readNamedWriteable(Expression.class),
            NameId.readFrom((StreamInput & PlanStreamInput) in),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(name());
        out.writeOptionalString(qualifier());
        out.writeNamedWriteable(child());
        id().writeTo(out);
        out.writeBoolean(synthetic());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Alias> info() {
        return NodeInfo.create(this, Alias::new, name(), qualifier, child, id(), synthetic());
    }

    public Alias replaceChild(Expression child) {
        return new Alias(source(), name(), qualifier, child, id(), synthetic());
    }

    @Override
    public Alias replaceChildren(List<Expression> newChildren) {
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
            lazyAttribute = resolved()
                ? new ReferenceAttribute(source(), name(), dataType(), qualifier, nullable(), id(), synthetic())
                : new UnresolvedAttribute(source(), name(), qualifier);
        }
        return lazyAttribute;
    }

    @Override
    public String toString() {
        return child + " AS " + name() + "#" + id();
    }

    @Override
    public String nodeString() {
        return child.nodeString() + " AS " + name();
    }

    /**
     * If the given expression is an alias, return its child - otherwise return as is.
     */
    public static Expression unwrap(Expression e) {
        return e instanceof Alias as ? as.child() : e;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        Alias other = (Alias) obj;
        return Objects.equals(qualifier, other.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), qualifier);
    }
}
